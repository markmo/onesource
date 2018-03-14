from datetime import datetime
import json
from logging import Logger
import os
from pipeline import AbstractStep, file_iter, output_handler as oh
from typing import Any, Dict, TextIO
from utils import convert_name_to_underscore


class ExtractQuestionsStep(AbstractStep):
    """
    Extract questions from collected text.
    """

    def __init__(self,
                 name: str,
                 source_key: str = None,
                 source_iter=file_iter,
                 output_handler=oh):
        super().__init__(name, source_key)
        self.__source_iter = source_iter
        self.__output_handler = output_handler
        self.__q_words = [
            'am', 'are', 'can', 'could', 'did', 'does', 'had', 'has', 'have', 'how', 'is', 'may', 'might',
            'shall', 'was', 'were', 'what', 'where', 'which', 'who', 'why', 'will', 'would'
        ]
        self.__add_q_words = [
            'at', 'do', 'from', 'if', 'in', 'on', 'over', 'should', 'to', 'under', 'when'
        ]

    def process_file(self, f: TextIO, c: Dict[str, Any], logger: Logger, a: Dict[str, Any]):
        logger.debug('process file: {}'.format(f.name))
        questions = []
        non_questions = []
        input_doc = json.load(f)
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
        write_root_dir = c['job']['write_root_dir']
        output_filename = '{}_{}.json'.format(convert_name_to_underscore(self.name), record_id)
        output_path = os.path.join(write_root_dir, output_filename)
        content = {'questions': list(set(questions)), 'non_questions': non_questions}
        a['files_output'].append({
            'filename': output_filename,
            'path': output_path,
            'status': 'processed',
            'time': now
        })
        self.__output_handler(output_path, content)

    def run(self, c: Dict[str, Any], logger: Logger, a: Dict[str, Any]):
        file_paths = [x['path'] for x in c[self.source_key]]
        for f in self.__source_iter(file_paths):
            self.process_file(f, c, logger, a)
