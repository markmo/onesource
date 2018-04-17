from datetime import datetime
import json
from logging import Logger
import os
from pipeline import AbstractStep, file_iter, json_output_handler as oh
from typing import Any, AnyStr, Callable, Dict, Iterator, IO, List
from utils import convert_name_to_underscore
import yaml

CONFIG_FILE_PATH = '../config/config.yml'


class CollectStep(AbstractStep):
    """
    Collect required text to extract features.
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

    def process_file(self,
                     file: IO[AnyStr],
                     control_data: Dict[str, Any],
                     logger: Logger,
                     accumulator: Dict[str, Any]
                     ) -> str:
        logger.debug('process file: {}'.format(file.name))
        config = load_config()
        input_doc = json.load(file)
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
                if 'structured_content' in val:
                    for s in val['structured_content']:
                        structured_content.append(s)

                if 'text' in val:
                    t = val['text']
                    if isinstance(t, list):
                        for x in t:
                            text.append(x)
                    else:
                        text.append(t)

        write_root_dir = control_data['job']['write_root_dir']
        step_name = convert_name_to_underscore(self.name)
        output_filename = '{}_{}.json'.format(step_name, record_id)
        output_path = os.path.join(write_root_dir, step_name, output_filename)
        update_control_info_(file.name, output_filename, output_path, accumulator)
        content = {'metadata': metadata, 'data': {'structured_content': structured_content, 'text': text}}
        self.__output_handler(output_path, content)
        return output_path

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


def load_config() -> Dict[str, Any]:
    # get lists of data keys by `doc_type` to include in output
    with open(CONFIG_FILE_PATH, 'r') as f:
        config = yaml.load(f)

    return config


def update_control_info_(source_filename: str,
                         output_filename: str,
                         output_path: str,
                         accumulator: Dict[str, Any]
                         ) -> None:
    now = datetime.utcnow().isoformat()
    accumulator['files_processed'].append({
        'path': source_filename,
        'time': now
    })
    accumulator['files_output'].append({
        'filename': output_filename,
        'path': output_path,
        'status': 'processed',
        'time': now
    })
