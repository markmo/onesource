from datetime import datetime
import json
from logging import Logger
import os
from pipeline import AbstractStep, file_iter, neo4j_output_handler as oh
from typing import Any, AnyStr, Callable, Dict, Iterator, IO, List, Optional
from utils import convert_name_to_underscore
import yaml

dir_path = os.path.dirname(os.path.realpath(__file__))
CONFIG_FILE_PATH = os.path.join(dir_path, '../config/config.yml')

FLUSH_FILE_COUNT = 100


class WriteToNeo4JStep(AbstractStep):
    """
    Write text from multiple files to a database.
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
                     ) -> None:
        logger.debug('process file: {}'.format(file.name))
        config = load_config()
        doc_types_with_text = config['doc_types_with_text']
        input_doc = json.load(file)
        doc_type = input_doc['metadata']['doc_type']
        if doc_type in doc_types_with_text:
            data = input_doc['data']
            accumulator['files_processed'].append({
                'path': file.name,
                'time': datetime.utcnow().isoformat()
            })
            self.__output_handler(None, data, self._overwrite)

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


def load_config() -> Dict[str, Any]:
    with open(CONFIG_FILE_PATH, 'r') as f:
        config = yaml.load(f)

    return config
