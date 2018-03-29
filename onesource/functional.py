from copy import deepcopy
from extractors import AbstractExtractor, HeadingExtractor, ListExtractor, TableExtractor, TextExtractor
from io import BytesIO
from lxml import etree
from typing import Any, Dict, Iterator, List, Tuple
from utils import clean_text, deep_update_, fix_content, get_iso_datetime_from_millis


# Functional interface

def element_iterator(stream: BytesIO,
                     excluded_tags: List[str],
                     html: bool = False
                     ) -> Iterator[Tuple[str, etree.ElementBase]]:
    for event, el in etree.iterparse(stream, events=('start', 'end'), html=html):
        if el.tag not in excluded_tags:
            yield event, el


def file_iter(file_paths: List[str]):
    for path in file_paths:
        with open(path, 'rb') as file:
            yield file


def process_file(stream: BytesIO,
                 excluded_xml_tags: List[str],
                 excluded_html_tags: List[str],
                 ) -> Dict[str, Any]:
    a = {
        'data': {},
        'is_data': False,
        'metadata': {'doc_type': None, 'record_id': None}
    }
    for event, el in element_iterator(stream, excluded_xml_tags):
        a_update = process_xml_element(el, event, a, excluded_html_tags)
        deep_update_(a, a_update)  # do not append to lists (default)

    return {'data': a['data'], 'metadata': a['metadata']}


def process_html_element(el: etree.ElementBase,
                         event: str,
                         extractors: List[AbstractExtractor]
                         ) -> (List[Dict[str, Any]], List[str]):
    """
    Stateful, so cannot be parallelized.

    :param el: HTML element
    :param event: event type [start, end]
    :param extractors: list of HTML extractors
    :return: tuple of structured content (as dict) and text lines (as list)
    """
    structured_content = []
    text_list = []
    for extractor in extractors:
        extractor.extract(el, event, structured_content, text_list)

    return structured_content, text_list


# noinspection SpellCheckingInspection
def process_xml_element(el: etree.ElementBase,
                        event: str,
                        accumulator: Dict[str, Any],
                        excluded_html_tags: List[str],
                        ) -> Dict[str, Any]:
    """
    Stateful, so cannot be parallelized.

    :param el: XML element
    :param event: event type [start, end]
    :param accumulator: accumulates state
    :param excluded_html_tags: XML tags to exclude
    :return: accumulated content as dict
    """
    a = deepcopy(accumulator)

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
        stream = BytesIO(fix_content(el.text).encode('utf-8'))
        for ev, elem in element_iterator(stream, excluded_html_tags, html=True):
            structured, text = process_html_element(elem, ev, extractors)
            structured_content.extend(structured)
            text_list.extend(text)

        data = {}
        if len(text_list) == 1:
            data['text'] = text_list[0]
        else:
            data['text'] = text_list

        if structured_content:
            data['structured_content'] = structured_content

        a['data'][el.tag.lower()] = data

    return a
