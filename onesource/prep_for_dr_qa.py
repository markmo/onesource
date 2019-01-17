from datetime import datetime
import json
from logging import Logger
import os
import pandas as pd
from pipeline import AbstractStep, file_iter, text_output_handler as oh
import re
import spacy
from table_util import infer_schema, table_to_natural_text
from typing import Any, AnyStr, Callable, Dict, Iterator, IO, List, Optional, Union
from utils import convert_name_to_underscore
import yaml

dir_path = os.path.dirname(os.path.realpath(__file__))
CONFIG_FILE_PATH = os.path.join(dir_path, '../config/config.yml')

FLUSH_FILE_COUNT = 100


class PrepForDrQAStep(AbstractStep):
    """
    Combine text from multiple files into a single file for use by DrQA.
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
        self._nlp = spacy.load('en')

    def process_file(self,
                     file: IO[AnyStr],
                     path: str,
                     control_data: Dict[str, Any],
                     logger: Logger,
                     accumulator: Dict[str, Any]
                     ) -> List[str]:
        logger.debug('process file: {}'.format(file.name))
        # config = load_config()
        input_doc = json.load(file)
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

        return texts

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


def load_config() -> Dict[str, Any]:
    with open(CONFIG_FILE_PATH, 'r') as f:
        config = yaml.load(f)

    return config
