from relation_extraction.core.parser import RelParser
from relation_extraction.core import entity_extraction
from relation_extraction.core import keras_models
from datetime import datetime
import json
from logging import Logger
import os
from pathlib import Path
from pipeline import AbstractStep, file_iter, json_output_handler as oh
from pycorenlp import StanfordCoreNLP
from typing import Any, AnyStr, Callable, Dict, Iterator, IO, List, Tuple
from utils import convert_name_to_underscore

ROOT_DIR = Path(__file__).parent.parent
GLOVE_PATH = ROOT_DIR / 'resources' / 'glove' / 'glove.6B.50d.txt'
MODELS_PATH = ROOT_DIR / 'models' / 'relation_extraction'


class ExtractRelationsStep(AbstractStep):
    """
    Extract relations from collected text.
    """

    def __init__(self,
                 name: str,
                 source_key: str = None,
                 overwrite: bool = False,
                 source_iter: Callable[[List[str]], Iterator[IO[AnyStr]]] = file_iter,
                 output_handler: Callable[[str, Dict[str, Any]], None] = oh):
        super().__init__(name, source_key, overwrite)
        self.__source_iter = source_iter
        self.__output_handler = output_handler
        self.__corenlp = StanfordCoreNLP('http://localhost:9000')
        self.__corenlp_properties = {
            'annotators': 'tokenize, pos, ner',
            'outputFormat': 'json'
        }

    def get_tagged_from_server(self, input_text: str) -> List[Tuple]:
        output = self.__corenlp.annotate(input_text, properties=self.__corenlp_properties).get('sentences', [])[0]
        tagged = [(t['originalText'], t['ner'], t['pos']) for t in output['tokens']]
        return tagged

    def process_file(self,
                     file: IO[AnyStr],
                     path: str,
                     control_data: Dict[str, Any],
                     logger: Logger,
                     accumulator: Dict[str, Any]
                     ) -> None:
        logger.debug('process file: {}'.format(file.name))
        input_doc = json.load(file)
        metadata = input_doc['metadata']
        record_id = metadata['record_id']
        text = input_doc['data']['text']
        graphs = []
        for t in text:
            tagged = self.get_tagged_from_server(t)
            entity_fragments = entity_extraction.extract_entities(tagged)
            edges = entity_extraction.generate_edges(entity_fragments)
            tokens = [t for t, _, _ in tagged]
            non_parsed_graph = {'tokens': tokens, 'edgeSet': edges}
            keras_models.model_params['wordembeddings'] = str(GLOVE_PATH)
            rel_parser = RelParser('model_ContextWeighted', models_foldes=str(MODELS_PATH))
            parsed_graph = rel_parser.classify_graph_relations(non_parsed_graph)
            # e.g.:
            # {'tokens': ['Germany', 'is', 'a', 'country', 'in', 'Europe'], 'edgeSet': [{'left': [0],
            # 'right': [5], 'kbID': 'P30', 'lexicalInput': 'continent'}, {'left': [0], 'right': [3],
            # 'kbID': 'P0', 'lexicalInput': 'ALL_ZERO'}, {'left': [5], 'right': [3], 'kbID': 'P31',
            # 'lexicalInput': 'instance of'}]}

            relations = []
            if parsed_graph:
                graphs.append(parsed_graph)
                for edge in parsed_graph['edgeSet']:
                    if edge['kbID'] != 'P0':
                        left = ' '.join([tokens[t] for t in edge['left']])
                        right = ' '.join([tokens[t] for t in edge['right']])
                        relations.append([left, edge['lexicalInput'], right])

                parsed_graph['relations'] = relations

        now = datetime.utcnow().isoformat()
        write_root_dir = control_data['job']['write_root_dir']
        step_name = convert_name_to_underscore(self.name)
        output_filename = '{}_{}.json'.format(step_name, record_id)
        output_path = os.path.join(write_root_dir, step_name, output_filename)
        content = {'metadata': metadata, 'data': {'graphs': graphs}}
        accumulator['files_output'].append({
            'filename': output_filename,
            'input': path,
            'path': output_path,
            'status': 'processed',
            'time': now
        })
        self.__output_handler(output_path, content)

    def run(self, control_data: Dict[str, Any], logger: Logger, accumulator: Dict[str, Any]) -> None:
        file_paths = [x['path'] for x in control_data[self.source_key]]
        step_name = convert_name_to_underscore(self.name)
        processed_file_paths = {}
        if step_name in control_data:
            for x in control_data[step_name]:
                if x['status'] == 'processed':
                    processed_file_paths[x['input']] = x

        for file, path in self.__source_iter(file_paths):
            if not self._overwrite and path in processed_file_paths.keys():
                accumulator['files_output'].append(processed_file_paths[path])
                continue

            self.process_file(file, path, control_data, logger, accumulator)
