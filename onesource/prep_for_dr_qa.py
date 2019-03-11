from datetime import datetime
import json
from logging import Logger
import os
import pandas as pd
from pipeline import AbstractStep, file_iter, json_lines_output_handler as oh
import re
import spacy
from table_util import infer_schema, table_to_natural_text
from typing import Any, AnyStr, Callable, Dict, Iterator, IO, List, Optional, Union
from utils import convert_name_to_underscore
import uuid


class PrepForDrQAStep(AbstractStep):
    """
    Combine text from multiple files into a single file for use by DrQA.
    """

    def __init__(self,
                 name: str,
                 source_key: str = None,
                 overwrite: bool = False,
                 source_iter: Callable[[List[str]], Iterator[IO[AnyStr]]] = file_iter,
                 output_handler: Callable[[str, List[Dict[str, Any]], Optional[bool]], None] = oh):
        super().__init__(name, source_key, overwrite)
        self.__source_iter = source_iter
        self.__output_handler = output_handler
        self._nlp = spacy.load('en')

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
        texts = []
        data = input_doc['data']
        accumulator['files_processed'].append({
            'path': file.name,
            'time': datetime.utcnow().isoformat()
        })
        if 'structured_content' in data:
            prev_type = None
            for x in data['structured_content']:
                if x['type'] == 'text':
                    texts.append(x['text'])
                elif x['type'] == 'list':
                    items = x['items']
                    if len(items) > 5:
                        texts.extend(items)
                    else:
                        list_intro = ''
                        if prev_type == 'text':
                            list_intro = self.infer_list_intro(texts[-1])
                            if list_intro:
                                texts.pop()
                                list_intro += ': '

                        texts.append(list_intro + '; '.join([normalize_list_item(it) for it in items]))

                elif x['type'] == 'table':
                    df = table_to_dataframe(x)
                    schema = infer_schema(df, n_header_rows=len(x['head']))
                    texts.extend(table_to_natural_text(df, schema))

                prev_type = x['type']

        formatted = []
        for t in texts:
            formatted.append({
                'id': str(uuid.uuid4()),
                'text': t
            })

        write_root_dir = control_data['job']['write_root_dir']
        step_name = convert_name_to_underscore(self.name)
        output_filename = '{}_{}.jsonl'.format(step_name, record_id)
        output_path = os.path.join(write_root_dir, step_name, output_filename)
        update_control_info_(file.name, path, output_filename, output_path, accumulator)
        self.__output_handler(output_path, formatted, self._overwrite)
        return output_path

    def run(self, control_data: Dict[str, Any], logger: Logger, accumulator: Dict[str, Any]) -> None:
        file_paths = [x['path'] for x in control_data[self.source_key]]
        step_name = convert_name_to_underscore(self.name)
        processed_file_paths = {}
        if step_name in control_data:
            for x in control_data[step_name]:
                if x['status'] == 'processed':
                    processed_file_paths[x['input']] = x

        for file, path in self.__source_iter(file_paths):
            if not self._overwrite and path in processed_file_paths:
                accumulator['files_output'].append(processed_file_paths[path])
                continue

            self.process_file(file, path, control_data, logger, accumulator)

    def infer_list_intro(self, text: str) -> Union[str, None]:
        doc = list(self._nlp(text))
        for token in doc[::-1]:
            if token.pos_ == 'PUNCT':
                text = ' '.join([t.text for t in doc[:-1]])
                if token.text == ':':
                    return text
                elif token.text == '.':
                    return None

                continue

            if token.pos_ in {'VERB', 'ADP'}:
                return text

            break

        return None


def normalize_list_item(text: str) -> str:
    if not text:
        return text

    if text[0].isupper() and text[1].islower():
        text = text[0].lower() + text[1:]

    for i, char in enumerate(text[::-1]):
        if re.match(r'\w', char):
            text = text[:-i]
            break

    return text


def table_to_dataframe(table: Dict) -> pd.DataFrame:
    rows = table['head'] + table['body']
    return pd.DataFrame(data=rows)


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
