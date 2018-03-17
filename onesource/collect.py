from datetime import datetime
import json
from logging import Logger
import os
from pipeline import AbstractStep, file_iter, output_handler as oh
from typing import Any, Dict, TextIO
from utils import convert_name_to_underscore
import yaml

CONFIG_FILE_PATH = '../config/config.yml'


class CollectStep(AbstractStep):
    """
    Collect required text to extract features.
    """

    @staticmethod
    def load_config() -> Dict[str, Any]:
        # get lists of data keys by `doc_type` to include in output
        with open(CONFIG_FILE_PATH, 'r') as f:
            config = yaml.load(f)

        return config

    def __init__(self,
                 name: str,
                 source_key: str=None,
                 source_iter=file_iter,
                 output_handler=oh):
        super().__init__(name, source_key)
        self.__source_iter = source_iter
        self.__output_handler = output_handler

    def process_file(self, c: Dict[str, Any], logger: Logger, a: Dict[str, Any], f: TextIO) -> str:
        logger.debug('process file: {}'.format(f.name))
        config = CollectStep.load_config()
        input_doc = json.load(f)
        metadata = input_doc['metadata']
        record_id = metadata['record_id']
        doc_type = metadata['doc_type']
        text_props = set(config['doc_types'][doc_type]['text_props'])
        data = input_doc['data']
        structured_content = []
        text = []
        for key in text_props:
            if key in data:
                val = data[key]
                for s in val['structured_content']:
                    structured_content.append(s)

                t = val['text']
                is_list = isinstance(t, list)
                if is_list:
                    for x in t:
                        text.append(x)
                else:
                    text.append(t)

        now = datetime.utcnow().isoformat()
        write_root_dir = c['job']['write_root_dir']
        output_filename = '{}_{}.json'.format(convert_name_to_underscore(self.name), record_id)
        output_path = os.path.join(write_root_dir, output_filename)
        a['files_output'].append({
            'filename': output_filename,
            'path': output_path,
            'status': 'processed',
            'time': now
        })
        content = {'metadata': metadata, 'data': {'structured_content': structured_content, 'text': text}}
        self.__output_handler(output_path, content)
        return output_path

    def run(self, c: Dict[str, Any], logger: Logger, a: Dict[str, Any]):
        file_paths = [x['path'] for x in c[self.source_key]]
        for f in self.__source_iter(file_paths):
            self.process_file(c, logger, a, f)
