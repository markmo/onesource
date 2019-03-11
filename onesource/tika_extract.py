from datetime import datetime
from extractors import AbstractExtractor, HeadingExtractor, ListExtractor, TableExtractor, TextExtractor
from io import BytesIO
from logging import Logger
from lxml import etree
import lxml.html
import os
from pipeline import AbstractStep, file_iter, HIDDEN_FILE_PREFIXES, json_output_handler as oh
import spacy
from tika import parser
from typing import Any, AnyStr, Callable, Dict, IO, Iterator, List, Tuple
from utils import convert_name_to_underscore, fix_content, flatten


class TikaExtractStep(AbstractStep):
    """
    Read One Source files, extract required data, and output as tidy JSON files.
    """

    def __init__(self,
                 name: str,
                 source_key: str = None,
                 overwrite: bool = False,
                 source_iter: Callable[[List[str]], Iterator[IO[AnyStr]]] = file_iter,
                 output_handler: Callable[[str, Dict[str, Any]], None] = oh,
                 excluded_tags: List[str] = None,
                 max_file_count: int = 100000,
                 delete: bool = False):
        """

        :param name: human-readable name of step
        :param source_key: `control_data` key for source list
        :param overwrite: overwrite files flag
        :param source_iter: data source iterable
        :param output_handler: receives output
        :param excluded_tags: do not extract from these tags
        :param max_file_count: maximum number of files to process
        """
        super().__init__(name, source_key, overwrite, delete)
        self.__source_iter = source_iter
        self.__output_handler = output_handler
        self.__excluded_tags = excluded_tags or ['GUID']
        self.__max_file_count = max_file_count
        self.__nlp = spacy.load('en_core_web_sm')

    def element_iterator(self,
                         stream: IO[AnyStr],
                         html: bool = False
                         ) -> Iterator[Tuple[str, etree.ElementBase]]:
        for event, el in etree.iterparse(stream, events=('start', 'end'), html=html):
            if el.tag not in self.__excluded_tags:
                yield event, el

    def process_doc(self, text: str, a: Dict[str, Any]) -> None:
        # treat all text as html
        # lxml will automatically wrap plain text in a para, body and html tags
        structured_content = []
        text_list = []
        extractors = [
            ListExtractor(excluded_tags=['table']),
            TableExtractor(),
            TextExtractor(excluded_tags=['ul', 'ol', 'table', 'title', 'h1', 'h2', 'h3', 'h4']),
            HeadingExtractor(excluded_tags=['ul', 'ol', 'table'])
        ]
        stream: IO[AnyStr] = BytesIO(fix_content(text).encode('utf-8'))
        for ev, elem in self.element_iterator(stream, html=True):
            process_html_element(elem, ev, extractors, structured_content, text_list, self.__nlp)

        # re-extract content in single column tables used for layout purposes only
        html = None  # memoize
        k = []
        for i, c in enumerate(structured_content):
            typ = c['type']
            if typ in ['text', 'heading']:
                k.append(1)
            elif typ == 'list':
                k.append(len(c.get('items', [])))
            elif typ == 'table':
                k.append(len(c.get('head', [])) + len(c.get('body', [])))
                if len(c.get('fields', [])) == 1:
                    if not html:
                        # reset stream to reiterate
                        stream.seek(0)

                        # read stream into str and parse as html
                        html = lxml.html.fromstring(stream.read())

                    # find single column layout table
                    contents = html.xpath(('/descendant::table[{0}]/tbody/tr/td/*|' +
                                           '/descendant::table[{0}]/tr/td/*').format(c['index']))
                    root = etree.Element('div')
                    root.extend(contents)
                    sc = []
                    tl = []
                    for evt, ele in etree.iterwalk(root, events=('start', 'end')):
                        process_html_element(ele, evt, extractors, sc, tl, self.__nlp)

                    j = len(c.get('references', []))
                    structured_content = flatten([structured_content[:(i - j)], sc,
                                                  structured_content[(i + 1):]])
                    text_list = flatten([text_list[:sum(k[:(i - j)])], tl, text_list[sum(k[:(i + 1)]):]])

        data = {}
        if len(text_list) == 1:
            data['text'] = text_list[0]
        else:
            data['text'] = text_list

        if structured_content:
            data['structured_content'] = structured_content

        a['data'] = data

    def process_file(self,
                     file: IO[AnyStr],
                     path: str,
                     control_data: Dict[str, Any],
                     logger: Logger,
                     accumulator: Dict[str, Any]
                     ) -> str:
        logger.debug('process file: {}'.format(file.name))
        write_root_dir = control_data['job']['write_root_dir']
        parsed = parser.from_file(path, xmlContent=True)
        ext = os.path.splitext(path)[1].lower()
        if ext == '.docx':
            doc_type = 'Word'
        elif ext == '.pdf':
            doc_type = 'PDF'
        else:
            doc_type = None

        metadata = parsed['metadata']
        record_id = metadata['dc:title'].replace(' ', '_')
        created_date = metadata['dcterms:created'][0]
        last_mod_date = metadata['dcterms:modified'][0]
        author = metadata.get('meta:last-author', '')
        word_count = int(metadata.get('meta:word-count', '-1'))
        accumulator.update({
            'data': {},
            'is_data': False,
            'metadata': {
                'doc_type': doc_type,
                'record_id': record_id,
                'created_date': created_date,
                'last_mod_date': last_mod_date,
                'author': author,
                'word_count': word_count
            }
        })
        self.process_doc(parsed['content'], accumulator)

        step_name = convert_name_to_underscore(self.name)
        output_filename = '{}_{}.json'.format(step_name, record_id)
        output_path = os.path.join(write_root_dir, step_name, output_filename)
        update_control_info_(file.name, path, output_filename, output_path, accumulator)
        self.write_output(accumulator, output_path)
        return output_path

    def write_output(self, accumulator: Dict[str, Any], output_path: str) -> None:
        content = {'metadata': accumulator['metadata'], 'data': accumulator['data']}
        self.__output_handler(output_path, content)

    def run(self, control_data: Dict[str, Any], logger: Logger, accumulator: Dict[str, Any]) -> None:
        file_paths = [x['path'] for x in control_data[self.source_key]]
        step_name = convert_name_to_underscore(self.name)
        processed_file_paths = {}
        # processed_file_paths = []
        if step_name in control_data:
            for x in control_data[step_name]:
                if x['status'] == 'processed':
                    processed_file_paths[x['input']] = x

            # processed_file_paths = [x['input'] for x in control_data[step_name]
            #                         if x['status'] == 'processed']

        accumulator['file_count'] = 0
        for file, path in self.__source_iter(file_paths):
            filename = os.path.basename(path)
            if not filename.startswith(HIDDEN_FILE_PREFIXES):
                if path in processed_file_paths.keys() and not self._overwrite:
                    accumulator['files_output'].append(processed_file_paths[path])
                    continue

                if accumulator['file_count'] > self.__max_file_count:
                    break

                self.process_file(file, path, control_data, logger, accumulator)
                if self._delete:
                    os.remove(path)

                accumulator['file_count'] += 1


def process_html_element(el: etree.ElementBase,
                         event: str,
                         extractors: List[AbstractExtractor],
                         structured_content: List[Dict[str, Any]],
                         text_list: List[str],
                         nlp
                         ) -> None:
    for extractor in extractors:
        extractor.extract(el, event, structured_content, text_list, nlp)


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
