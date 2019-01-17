from datetime import datetime
import json
from logging import Logger
import os
from pipeline import AbstractStep, file_iter, json_output_handler as oh
import spacy
from spacy.matcher import Matcher
from spacy.attrs import TAG
from typing import Any, AnyStr, Callable, Dict, Iterator, IO, List
from utils import convert_name_to_underscore


class TransformStep(AbstractStep):
    """
    Extract NLP features from collected text.
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

        # nlp = spacy.load('xx_ent_wiki_sm')
        nlp = spacy.load('en')
        matcher = Matcher(nlp.vocab)
        matcher.add('Q1', None, [
            {TAG: 'WDT'}, {TAG: 'NN', 'OP': '+'}, {TAG: 'VBZ'}, {TAG: 'JJ'},
            {TAG: 'IN'}, {TAG: 'DT'}, {TAG: 'PRP$'}, {TAG: 'NNP', 'OP': '+'},
            {TAG: '.'}
        ])
        matcher.add('Q2', None, [
            {TAG: 'WP'}, {TAG: 'VBP'}, {TAG: 'PRP'}, {TAG: 'VB'}, {TAG: 'TO'},
            {TAG: 'VB'}, {TAG: '.'}
        ])
        self.__nlp = nlp
        self.__matcher = matcher

    def process_file(self,
                     file: IO[AnyStr],
                     control_data: Dict[str, Any],
                     logger: Logger,
                     accumulator: Dict[str, Any]
                     ) -> None:
        logger.debug('process file: {}'.format(file.name))
        matcher = self.__matcher
        input_doc = json.load(file)
        metadata = input_doc['metadata']
        record_id = metadata['record_id']
        text = input_doc['data']['text']
        sentences = []
        for t in text:
            doc = self.__nlp(t)
            entities = []
            annotated = []
            pos_tags = []
            for ent in doc.ents:
                entity = dict([
                    ('text', ent.text),
                    ('start_char', ent.start_char),
                    ('end_char', ent.end_char),
                    ('label', ent.label_),
                ])
                entities.append(entity)

            for token in doc:
                annotated.append(
                    dict([
                        ('text', token.text),
                        ('lemma', token.lemma_),
                        ('pos', token.pos_),
                        ('tag', token.tag_),
                        ('dep', token.dep_),
                        ('shape', token.shape_),
                        ('is_alpha', token.is_alpha),
                        ('is_stop', token.is_stop),
                    ])
                )
                pos_tags.append(token.tag_)

            # matches = matcher(doc)
            is_question = len(matcher(doc)) > 0

            sentence = dict([
                ('text', t),
                ('annotated', annotated),
                ('entities', entities),
                ('is_question', is_question),
                ('pos_tags', ' '.join(pos_tags))

            ])
            sentences.append(sentence)

        now = datetime.utcnow().isoformat()
        write_root_dir = control_data['job']['write_root_dir']
        step_name = convert_name_to_underscore(self.name)
        output_filename = '{}_{}.json'.format(step_name, record_id)
        output_path = os.path.join(write_root_dir, step_name, output_filename)
        content = {'metadata': metadata, 'data': {'sentences': sentences}}
        accumulator['files_output'].append({
            'filename': output_filename,
            'path': output_path,
            'status': 'processed',
            'time': now
        })
        self.__output_handler(output_path, content)

    def run(self, control_data: Dict[str, Any], logger: Logger, accumulator: Dict[str, Any]) -> None:
        file_paths = [x['path'] for x in control_data[self.source_key]]
        step_name = convert_name_to_underscore(self.name)
        processed_file_paths = []
        if step_name in control_data:
            processed_file_paths = [x['path'] for x in control_data[step_name]
                                    if x['status'] == 'processed']

        for file, path in self.__source_iter(file_paths):
            if not self._overwrite and path in processed_file_paths:
                continue

            self.process_file(file, control_data, logger, accumulator)
