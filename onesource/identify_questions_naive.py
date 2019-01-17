from datetime import datetime
import json
from logging import Logger
import numpy as np
import os
from pipeline import AbstractStep, file_iter, json_output_handler as oh
from typing import Any, AnyStr, Callable, Dict, Iterator, IO, List
from utils import convert_name_to_underscore
import yaml

dir_path = os.path.dirname(os.path.realpath(__file__))
CONFIG_FILE_PATH = os.path.join(dir_path, '../config/config.yml')


class IdentifyQuestionsNaiveStep(AbstractStep):
    """
    Identify questions in text.
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
        self.__q_words = [
            'am', 'are', 'can', 'could', 'did', 'does', 'had', 'has', 'have', 'how', 'is', 'may', 'might',
            'shall', 'was', 'were', 'what', 'where', 'which', 'who', 'why', 'will', 'would'
        ]
        self.__add_q_words = [
            'at', 'do', 'from', 'if', 'in', 'on', 'over', 'should', 'to', 'under', 'when'
        ]

    def predict_question(self, text):
        words = [x.lower() for x in text.split()]
        return text.endswith('?') or words[0] in self.__q_words

    def process_file(self,
                     file: IO[AnyStr],
                     path: str,
                     control_data: Dict[str, Any],
                     logger: Logger,
                     accumulator: Dict[str, Any]
                     ) -> str:
        logger.debug('process file: {}'.format(file.name))
        input_doc = json.load(file)
        metadata = input_doc['metadata']
        record_id = metadata['record_id']
        data = input_doc['data']
        if 'structured_content' in data:
            for item in data['structured_content']:
                if 'text' in item:
                    is_question = self.predict_question(item['text'])
                    if is_question:
                        accumulator['found_questions'].append(item['text'])

                    item['is_question'] = is_question

        write_root_dir = control_data['job']['write_root_dir']
        step_name = convert_name_to_underscore(self.name)
        output_filename = '{}_{}.json'.format(step_name, record_id)
        output_path = os.path.join(write_root_dir, step_name, output_filename)
        update_control_info_(file.name, path, output_filename, output_path, accumulator)
        self.__output_handler(output_path, input_doc)
        return output_path

    def run(self, control_data: Dict[str, Any], logger: Logger, accumulator: Dict[str, Any]) -> None:
        file_paths = [x['path'] for x in control_data[self.source_key]]
        step_name = convert_name_to_underscore(self.name)
        processed_file_paths = {}
        if step_name in control_data:
            for x in control_data[step_name]:
                if x['status'] == 'processed':
                    processed_file_paths[x['input']] = x

        accumulator['found_questions'] = []
        for file, path in self.__source_iter(file_paths):
            if not self._overwrite and path in processed_file_paths.keys():
                accumulator['files_output'].append(processed_file_paths[path])
                continue

            self.process_file(file, path, control_data, logger, accumulator)

        np.savetxt('/tmp/found_questions.txt', accumulator['found_questions'], fmt='%s')
        del accumulator['found_questions']


def load_config() -> Dict[str, Any]:
    # get lists of data keys by `doc_type` to include in output
    with open(CONFIG_FILE_PATH, 'r') as f:
        config = yaml.load(f)

    return config


def update_control_info_(source_filename: str,
                         source_path: str,
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
        'input': source_path,
        'path': output_path,
        'status': 'processed',
        'time': now
    })
