from datetime import datetime
import json
from logging import Logger
import os
from pipeline import AbstractStep, file_iter, json_output_handler as oh
from typing import Any, AnyStr, Callable, Dict, IO, Iterator, List
from utils import convert_name_to_underscore


class ExtractQuestionsStep(AbstractStep):
    """
    Extract questions from collected text.
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

    def process_file(self,
                     file: IO[AnyStr],
                     control_data: Dict[str, Any],
                     logger: Logger,
                     accumulator: Dict[str, Any]
                     ) -> None:
        logger.debug('process file: {}'.format(file.name))
        questions = []
        non_questions = []
        input_doc = json.load(file)
        metadata = input_doc['metadata']
        record_id = metadata['record_id']
        text = input_doc['data']['text']
        for t in text:
            words = [x.lower() for x in t.split()]
            if t.endswith('?') or words[0] in self.__q_words:
                questions.append(t)
            else:
                non_questions.append(t)

        now = datetime.utcnow().isoformat()
        write_root_dir = control_data['job']['write_root_dir']
        step_name = convert_name_to_underscore(self.name)
        output_filename = '{}_{}.json'.format(step_name, record_id)
        output_path = os.path.join(write_root_dir, step_name, output_filename)
        content = {'questions': list(set(questions)), 'non_questions': non_questions}
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
