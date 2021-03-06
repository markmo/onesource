from datetime import datetime
import json
from logging import Logger
import os
from pipeline import AbstractStep, file_iter, text_output_handler as oh
from typing import Any, AnyStr, Callable, Dict, Iterator, IO, List, Optional
from utils import convert_name_to_underscore
import yaml

dir_path = os.path.dirname(os.path.realpath(__file__))
CONFIG_FILE_PATH = os.path.join(dir_path, '../config/config.yml')

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
                 output_handler: Callable[[str, List[str], Optional[bool]], None] = oh):
        super().__init__(name, source_key, overwrite)
        self.__source_iter = source_iter
        self.__output_handler = output_handler

    def process_file(self,
                     file: IO[AnyStr],
                     path: str,
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
            if 'structured_content' in data:
                for x in data['structured_content']:
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
        output = {
            'filename': output_filename,
            'path': output_path,
            'status': 'processed',
            'time': datetime.utcnow().isoformat()
        }
        accumulator['files_output'].append(output)
        processed_file_paths = []
        if step_name in control_data and control_data[step_name]['status'] == 'processed':
            processed_file_paths = control_data[step_name]['input']

        paths = []
        text = []
        j = 0
        for file, path in self.__source_iter(file_paths):
            paths.append(path)
            if not self._overwrite and path in processed_file_paths:
                continue

            text.extend(self.process_file(file, path, control_data, logger, accumulator))
            j += 1
            # manage memory use - flush every 100th file
            if j % FLUSH_FILE_COUNT == 0:
                self.__output_handler(output_path, text, self._overwrite)
                text = []

        self.__output_handler(output_path, text, self._overwrite)
        output['input'] = paths


def load_config() -> Dict[str, Any]:
    with open(CONFIG_FILE_PATH, 'r') as f:
        config = yaml.load(f)

    return config
