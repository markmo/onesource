from datetime import datetime
from extractors import AbstractExtractor, HeadingExtractor, ListExtractor, TableExtractor, TextExtractor
from io import BytesIO
from logging import Logger
from lxml import etree
import os
from pipeline import AbstractStep, file_iter, output_handler as oh
from typing import Any, AnyStr, Callable, Dict, IO, Iterator, List, Tuple
from utils import clean_text, convert_name_to_underscore, fix_content, get_iso_datetime_from_millis


class ExtractStep(AbstractStep):
    """
    Read One Source files, extract required data, and output as tidy JSON files.
    """

    def __init__(self,
                 name: str,
                 source_key: str = None,
                 source_iter: Callable[[List[str]], Iterator[IO[AnyStr]]] = file_iter,
                 output_handler: Callable[[str, Dict[str, Any]], None] = oh,
                 excluded_tags: List[str] = None,
                 max_file_count: int = 100000):
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

    def element_iterator(self,
                         stream: IO[AnyStr],
                         html: bool = False
                         ) -> Iterator[Tuple[str, etree.ElementBase]]:
        for event, el in etree.iterparse(stream, events=('start', 'end'), html=html):
            if el.tag not in self.__excluded_tags:
                yield event, el

    # noinspection SpellCheckingInspection
    def process_xml_element(self, el: etree.ElementBase, event: str, a: Dict[str, Any]) -> None:
        if el.tag == 'CONTENT' and event == 'end':
            a['metadata']['record_id'] = el.get('RECORDID')

        elif el.tag == 'MASTERIDENTIFER' and event == 'end':
            a['metadata']['title'] = clean_text(el.text)

        elif el.tag == 'TYPE' and event == 'end':
            a['metadata']['doc_type'] = clean_text(el.text)

        elif el.tag == 'DOCUMENTID' and event == 'end':
            a['metadata']['doc_id'] = clean_text(el.text)

        elif el.tag == 'VERSION' and event == 'end':
            a['metadata']['version'] = clean_text(el.text)

        elif el.tag == 'AUTHOR' and event == 'end':
            a['metadata']['author'] = clean_text(el.text)

        elif el.tag == 'ENDTIMESTAMP_MILLIS' and event == 'end':
            millis = int(clean_text(el.text))
            a['metadata']['end_timestamp_millis'] = millis
            a['metadata']['end_time'] = get_iso_datetime_from_millis(millis)

        elif el.tag == 'STARTTIMESTAMP_MILLIS' and event == 'end':
            millis = int(clean_text(el.text))
            a['metadata']['start_timestamp_millis'] = millis
            a['metadata']['start_time'] = get_iso_datetime_from_millis(millis)

        elif el.tag == 'CREATETIMESTAMP_MILLIS' and event == 'end':
            millis = int(clean_text(el.text))
            a['metadata']['create_timestamp_millis'] = millis
            a['metadata']['create_time'] = get_iso_datetime_from_millis(millis)

        elif el.tag == 'LASTMODIFIEDTIMESTAMP_MILLIS' and event == 'end':
            millis = int(clean_text(el.text))
            a['metadata']['last_modified_timestamp_millis'] = millis
            a['metadata']['last_modified_time'] = get_iso_datetime_from_millis(millis)

        elif el.tag == 'RESOURCEPATH' and event == 'end':
            a['metadata']['doc_location_path'] = clean_text(el.text)

        elif el.tag == 'PUBLISHEDTIMESTAMP_MILLIS' and event == 'end':
            millis = int(clean_text(el.text))
            a['metadata']['published_timestamp_millis'] = millis
            a['metadata']['published_time'] = get_iso_datetime_from_millis(millis)

        elif el.tag == a['metadata']['doc_type']:
            a['is_data'] = (event == 'start')

        elif a['is_data'] and event == 'end' and el.text:
            # treat all text as html
            # lxml will automatically wrap plain text in a para, body and html tags
            structured_content = []
            text_list = []
            extractors = [
                ListExtractor(excluded_tags=['table']),
                TableExtractor(),
                TextExtractor(excluded_tags=['ul', 'ol', 'table', 'h1', 'h2', 'h3', 'h4']),
                HeadingExtractor(excluded_tags=['ul', 'ol', 'table'])
            ]
            stream: IO[AnyStr] = BytesIO(fix_content(el.text).encode('utf-8'))
            for ev, elem in self.element_iterator(stream, html=True):
                process_html_element(elem, ev, extractors, structured_content, text_list)

            data = {}
            if len(text_list) == 1:
                data['text'] = text_list[0]
            else:
                data['text'] = text_list

            if structured_content:
                data['structured_content'] = structured_content

            a['data'][el.tag.lower()] = data

    def process_file(self,
                     control_data: Dict[str, Any],
                     logger: Logger,
                     accumulator: Dict[str, Any],
                     file: IO[AnyStr]
                     ) -> str:
        logger.debug('process file: {}'.format(file.name))
        write_root_dir = control_data['job']['write_root_dir']
        accumulator.update({
            'data': {},
            'is_data': False,
            'metadata': {'doc_type': None, 'record_id': None}
        })
        for event, el in self.element_iterator(file):
            self.process_xml_element(el, event, accumulator)

        record_id = accumulator['metadata']['record_id']
        output_filename = '{}_{}.json'.format(convert_name_to_underscore(self.name), record_id)
        output_path = os.path.join(write_root_dir, output_filename)
        update_control_info_(file.name, output_filename, output_path, accumulator)
        self.write_output(accumulator, output_path)

        return output_path

    def write_output(self, accumulator: Dict[str, Any], output_path: str) -> None:
        content = {'metadata': accumulator['metadata'], 'data': accumulator['data']}
        self.__output_handler(output_path, content)

    def run(self, control_data: Dict[str, Any], logger: Logger, accumulator: Dict[str, Any]) -> None:
        file_paths = [x['path'] for x in control_data[self.source_key]]
        accumulator['file_count'] = 0
        for file in self.__source_iter(file_paths):
            if accumulator['file_count'] > self.__max_file_count:
                break

            self.process_file(control_data, logger, accumulator, file)
            accumulator['file_count'] += 1


def process_html_element(el: etree.ElementBase,
                         event: str,
                         extractors: List[AbstractExtractor],
                         structured_content: List[Dict[str, Any]],
                         text_list: List[str]
                         ) -> None:
    for extractor in extractors:
        extractor.extract(el, event, structured_content, text_list)


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
