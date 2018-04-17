from datetime import datetime
import json
from logging import Logger
import os
from pipeline import AbstractStep, file_iter, text_output_handler as oh
from typing import Any, AnyStr, Callable, Dict, Iterator, IO, List
from utils import convert_name_to_underscore
import yaml

CONFIG_FILE_PATH = '../config/config.yml'

FLUSH_FILE_COUNT = 100


class CombineStep(AbstractStep):
    """
    Combine text from multiple files into a single file.
    """

    def __init__(self,
                 name: str,
                 source_key: str = None,
                 overwrite: bool = False,
                 source_iter: Callable[[List[str]], Iterator[IO[AnyStr]]] = file_iter,
                 output_handler: Callable[[str, List[str]], None] = oh):
        super().__init__(name, source_key, overwrite)
        self.__source_iter = source_iter
        self.__output_handler = output_handler

    def process_file(self,
                     file: IO[AnyStr],
                     control_data: Dict[str, Any],
                     logger: Logger,
                     accumulator: Dict[str, Any]
                     ) -> List[str]:
        logger.debug('process file: {}'.format(file.name))
        config = load_config()
        doc_types_with_text = config['doc_types_with_text']
        input_doc = json.load(file)
        doc_type = input_doc['metadata']['doc_type']
        text = []
        if doc_type in doc_types_with_text:
            data = input_doc['data']
            accumulator['files_processed'].append({
                'path': file.name,
                'time': datetime.utcnow().isoformat()
            })
            if 'structured' in data:
                for x in data['structured']:
                    if x['type'] in ['text', 'heading']:
                        text.append(x['text'])
                    elif x['type'] == 'list':
                        text.extend(x['items'])

        return text

    def run(self, control_data: Dict[str, Any], logger: Logger, accumulator: Dict[str, Any]) -> None:
        file_paths = [x['path'] for x in control_data[self.source_key]]
        write_root_dir = control_data['job']['write_root_dir']
        step_name = convert_name_to_underscore(self.name)
        output_filename = '{}.txt'.format(step_name)
        output_path = os.path.join(write_root_dir, step_name, output_filename)
        accumulator['files_output'].append({
            'filename': output_filename,
            'path': output_path,
            'status': 'processed',
            'time': datetime.utcnow().isoformat()
        })
        processed_file_paths = []
        if step_name in control_data:
            processed_file_paths = [x['path'] for x in control_data[step_name]
                                    if x['status'] == 'processed']

        text = []
        j = 0
        for file, path in self.__source_iter(file_paths):
            if not self._overwrite and path in processed_file_paths:
                continue

            text.extend(self.process_file(file, control_data, logger, accumulator))
            j += 1
            # manage memory use - flush every 100th file
            if j % FLUSH_FILE_COUNT == 0:
                self.__output_handler(output_path, text)
                text = []

        self.__output_handler(output_path, text)


def load_config() -> Dict[str, Any]:
    with open(CONFIG_FILE_PATH, 'r') as f:
        config = yaml.load(f)

    return config
