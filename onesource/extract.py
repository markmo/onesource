from datetime import datetime
from extractors import HeadingExtractor, ListExtractor, TableExtractor, TextExtractor
from io import BytesIO
from logging import Logger
from lxml import etree
import os
from pipeline import AbstractStep, file_iter, output_handler as oh
from typing import Any, Dict, List, TextIO
from utils import convert_name_to_underscore, fix_content, get_iso_datetime_from_millis


class ExtractStep(AbstractStep):
    """
    Read One Source files, extract required data, and output as tidy JSON files.
    """

    def __init__(self,
                 name: str,
                 source_key: str=None,
                 source_iter=file_iter,
                 output_handler=oh,
                 excluded_tags: List[str]=None,
                 max_file_count=100000):
        """

        :param name: human-readable name of step
        :param source_key: `control_data` key for source list
        :param source_iter: data source iterable
        :param output_handler: receives output
        :param excluded_tags: do not extract from these tags
        :param max_file_count: maximum number of files to process
        """
        super().__init__(name, source_key)
        self.__source_iter = source_iter
        self.__output_handler = output_handler
        self.__excluded_tags = excluded_tags or ['GUID']
        self.__max_file_count = max_file_count

    # noinspection SpellCheckingInspection
    def process_file(self, c: Dict[str, Any], logger: Logger, a: Dict[str, Any], f: TextIO) -> str:
        logger.debug('process file: {}'.format(f.name))
        a.update({
            'data': {},
            'is_data': False,
            'metadata': {'doc_type': None, 'record_id': None}
        })
        it = etree.iterparse(f, events=('start', 'end'))
        stream = ((event, el) for event, el in it if el.tag not in self.__excluded_tags)
        for event, el in stream:
            if el.tag == 'CONTENT' and event == 'end':
                a['metadata']['record_id'] = el.get('RECORDID')

            elif el.tag == 'MASTERIDENTIFER' and event == 'end':
                a['metadata']['title'] = el.text

            elif el.tag == 'TYPE' and event == 'end':
                a['metadata']['doc_type'] = el.text

            elif el.tag == 'DOCUMENTID' and event == 'end':
                a['metadata']['doc_id'] = el.text

            elif el.tag == 'VERSION' and event == 'end':
                a['metadata']['version'] = el.text

            elif el.tag == 'AUTHOR' and event == 'end':
                a['metadata']['author'] = el.text

            elif el.tag == 'ENDTIMESTAMP_MILLIS' and event == 'end':
                millis = int(el.text)
                a['metadata']['end_timestamp_millis'] = millis
                a['metadata']['end_time'] = get_iso_datetime_from_millis(millis)

            elif el.tag == 'STARTTIMESTAMP_MILLIS' and event == 'end':
                millis = int(el.text)
                a['metadata']['start_timestamp_millis'] = millis
                a['metadata']['start_time'] = get_iso_datetime_from_millis(millis)

            elif el.tag == 'CREATETIMESTAMP_MILLIS' and event == 'end':
                millis = int(el.text)
                a['metadata']['create_timestamp_millis'] = millis
                a['metadata']['create_time'] = get_iso_datetime_from_millis(millis)

            elif el.tag == 'LASTMODIFIEDTIMESTAMP_MILLIS' and event == 'end':
                millis = int(el.text)
                a['metadata']['last_modified_timestamp_millis'] = millis
                a['metadata']['last_modified_time'] = get_iso_datetime_from_millis(millis)

            elif el.tag == 'RESOURCEPATH' and event == 'end':
                a['metadata']['doc_location_path'] = el.text

            elif el.tag == 'PUBLISHEDTIMESTAMP_MILLIS' and event == 'end':
                millis = int(el.text)
                a['metadata']['published_timestamp_millis'] = millis
                a['metadata']['published_time'] = get_iso_datetime_from_millis(millis)

            elif el.tag == a['metadata']['doc_type']:
                a['is_data'] = (event == 'start')

            elif a['is_data'] and event == 'end' and el.text:
                # treat all text as html
                # lxml will automatically wrap plain text in a para, body and html tags
                structured_content = []
                text_list = []
                list_extractor = ListExtractor(excluded_tags=['table'])
                table_extractor = TableExtractor()
                text_extractor = TextExtractor(excluded_tags=['ul', 'ol', 'table', 'h1', 'h2', 'h3', 'h4'])
                heading_extractor = HeadingExtractor(excluded_tags=['ul', 'ol', 'table'])
                stream = BytesIO(fix_content(el.text).encode('utf-8'))

                for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
                    heading_extractor.extract(elem, ev, structured_content, text_list)
                    text_extractor.extract(elem, ev, structured_content, text_list)
                    list_extractor.extract(elem, ev, structured_content, text_list)
                    table_extractor.extract(elem, ev, structured_content, text_list)

                data = {}
                if len(text_list) == 1:
                    data['text'] = text_list[0]
                else:
                    data['text'] = text_list

                if structured_content:
                    data['structured_content'] = structured_content

                a['data'][el.tag.lower()] = data

        now = datetime.utcnow().isoformat()
        a['files_processed'].append({
            'path': f.name,
            'time': now
        })
        write_root_dir = c['job']['write_root_dir']
        output_filename = '{}_{}.json'.format(convert_name_to_underscore(self.name), a['metadata']['record_id'])
        output_path = os.path.join(write_root_dir, output_filename)
        a['files_output'].append({
            'filename': output_filename,
            'path': output_path,
            'status': 'processed',
            'time': now
        })
        content = {'metadata': a['metadata'], 'data': a['data']}
        self.__output_handler(output_path, content)
        return output_path

    def run(self, c: Dict[str, Any], logger: Logger, a: Dict[str, Any]):
        file_paths = [x['path'] for x in c[self.source_key]]
        a['file_count'] = 0

        for f in self.__source_iter(file_paths):
            if a['file_count'] > self.__max_file_count:
                break

            self.process_file(c, logger, a, f)
            a['file_count'] += 1
