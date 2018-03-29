from typing import Any, Dict, List
from utils import clean_text


class AbstractExtractor(object):
    """
    Interface for extractor types to implement.

    Extractors are stateful, so must be used per file.
    """

    def extract(self, el, ev, structured_content: List[Dict[str, Any]], text_list: List[str]):
        """

        :param el: the current HTML element being processed
        :param ev: the type of event ['start', 'end']
        :param structured_content: a list to append structured content
        :param text_list: a list to append plain text
        :return: None
        """
        raise NotImplementedError


class HeadingExtractor(AbstractExtractor):
    """
    Extracts headings from HTML (H1 - H4) as structured content and plain text.
    """

    def __init__(self, excluded_tags: List[str]=None):
        """

        :param excluded_tags: do not extract headings within these tags
        """
        self.__current_text = ''
        self.__excluded_stack_count = 0
        self.__excluded_tags = excluded_tags or ['ul', 'ol', 'table']
        self.__is_heading = False

    def extract(self, el, ev, structured_content: List[Dict[str, Any]], text_list: List[str]):
        if el.tag in self.__excluded_tags:
            if ev == 'start':
                self.__excluded_stack_count += 1

            elif ev == 'end':
                self.__excluded_stack_count -= 1

        elif not self.__is_excluded():
            if el.tag in ['h1', 'h2', 'h3', 'h4']:
                if ev == 'start':
                    self.__is_heading = True
                    if el.text:
                        self.__current_text += el.text

                elif ev == 'end':
                    self.__is_heading = False
                    if self.__current_text:
                        c = clean_text(self.__current_text)
                        self.__current_text = ''
                        if c:
                            text_list.append(c)
                            structured_content.append({'type': 'heading', 'text': c})

            elif self.__is_heading:
                if ev == 'start' and el.text:
                    self.__current_text += el.text

                if ev == 'end' and el.tail:
                    self.__current_text += el.tail

    def __is_excluded(self):
        return self.__excluded_stack_count > 0


class TextExtractor(AbstractExtractor):
    """
    Extracts block text from HTML (p, div) as structured content and plain text.
    """

    def __init__(self, excluded_tags: List[str]=None):
        """

        :param excluded_tags: do not extract text within these tags
        """
        self.__current_text = ''
        self.__excluded_stack_count = 0
        self.__excluded_tags = excluded_tags or ['ul', 'ol', 'table', 'h1', 'h2', 'h3', 'h4']

    def extract(self, el, ev, structured_content: List[Dict[str, Any]], text_list: List[str]):
        if el.tag in self.__excluded_tags:
            if ev == 'start':
                self.__excluded_stack_count += 1
                if self.__current_text:
                    c = clean_text(self.__current_text)
                    self.__current_text = ''
                    if c:
                        text_list.append(c)
                        structured_content.append({'type': 'text', 'text': c})

            elif ev == 'end':
                self.__excluded_stack_count -= 1
                self.__current_text = ''
                if el.tail:
                    self.__current_text += el.tail

        elif not self.__is_excluded():
            if el.tag == 'br' and ev == 'end':
                if self.__current_text:
                    c = clean_text(self.__current_text)
                    self.__current_text = ''
                    if c:
                        text_list.append(c)
                        structured_content.append({'type': 'text', 'text': c})

                if el.tail:
                    self.__current_text += el.tail

            elif el.tag in ['p', 'div']:
                if ev == 'start':
                    if self.__current_text:
                        c = clean_text(self.__current_text)
                        self.__current_text = ''
                        if c:
                            text_list.append(c)
                            structured_content.append({'type': 'text', 'text': c})

                    if el.text:
                        self.__current_text += el.text

                elif ev == 'end':
                    if self.__current_text:
                        c = clean_text(self.__current_text)
                        self.__current_text = ''
                        if c:
                            text_list.append(c)
                            structured_content.append({'type': 'text', 'text': c})

                    if el.tail:
                        self.__current_text += el.tail

            else:
                if ev == 'start' and el.text:
                    self.__current_text += el.text

                elif ev == 'end' and el.tail:
                    self.__current_text += el.tail

    def __is_excluded(self):
        return self.__excluded_stack_count > 0


class ListExtractor(AbstractExtractor):
    """
    Extracts lists from HTML (ul, ol) as structured content and plain text.
    """

    __heading_tags = ['h1', 'h2', 'h3', 'h4', 'div', 'strong']

    def __init__(self, excluded_tags: List[str]=None):
        self.__excluded_tags = excluded_tags or ['table']
        self.__excluded_stack_count = 0
        self.__current_text = ''
        self.__heading_text = ''
        self.__is_heading = False
        self.__is_items = False
        self.__is_list = False
        self.__list_content = {'type': 'list', 'items': []}

    def extract(self, el, ev, structured_content: List[Dict[str, Any]], text_list: List[str]):
        if el.tag in self.__excluded_tags:
            if ev == 'start':
                self.__excluded_stack_count += 1

            elif ev == 'end':
                self.__excluded_stack_count -= 1

        elif not self.__is_excluded():
            if el.tag in ['ul', 'ol']:
                if ev == 'start':
                    self.__is_list = True

                elif ev == 'end':
                    if self.__current_text:
                        c = clean_text(self.__current_text)
                        if c:
                            text_list.append(c)
                            if self.__is_heading:
                                self.__heading_text += self.__current_text
                            else:
                                structured_content.append({'type': 'text', 'text': c})

                    if self.__heading_text:
                        self.__list_content['heading'] = clean_text(self.__heading_text)

                    structured_content.append(self.__list_content)
                    self.__list_content = {'type': 'list', 'items': []}
                    self.__is_items = False
                    self.__is_heading = False
                    self.__is_list = False
                    self.__heading_text = ''
                    self.__current_text = ''

            elif self.__is_list:
                if el.tag in self.__heading_tags and ev == 'start' and not self.__is_items:
                    self.__is_heading = True

                if el.tag == 'li':
                    if ev == 'start':
                        if self.__current_text:
                            c = clean_text(self.__current_text)
                            if c:
                                text_list.append(c)
                                if self.__is_heading:
                                    self.__heading_text += self.__current_text
                                else:
                                    structured_content.append({'type': 'text', 'text': c})

                            self.__current_text = ''

                        self.__is_heading = False
                        self.__is_items = True
                        if el.text:
                            self.__current_text += el.text

                    elif ev == 'end':
                        if self.__current_text:
                            c = clean_text(self.__current_text)
                            self.__current_text = ''
                            if c:
                                text_list.append(c)
                                self.__list_content['items'].append(c)

                        if el.tail:
                            self.__current_text += el.tail

                elif el.tag == 'br' and ev == 'end':
                    if self.__current_text:
                        c = clean_text(self.__current_text)
                        if c:
                            text_list.append(c)
                            if self.__is_heading:
                                self.__heading_text += self.__current_text + ' '
                            else:
                                structured_content.append({'type': 'text', 'text': c})

                        self.__current_text = ''

                    if el.tail:
                        self.__current_text += el.tail

                elif el.tag in ['p', 'div', 'h1', 'h2', 'h3', 'h4']:
                    if ev == 'start':
                        if self.__current_text:
                            c = clean_text(self.__current_text)
                            if c:
                                text_list.append(c)
                                if self.__is_heading:
                                    self.__heading_text += self.__current_text + ' '
                                else:
                                    structured_content.append({'type': 'text', 'text': c})

                            self.__current_text = ''

                        if el.text:
                            self.__current_text += el.text

                    elif ev == 'end':
                        if self.__current_text:
                            c = clean_text(self.__current_text)
                            if c:
                                text_list.append(c)
                                if self.__is_heading:
                                    self.__heading_text += self.__current_text + ' '
                                else:
                                    structured_content.append({'type': 'text', 'text': c})

                            self.__current_text = ''

                        if el.tail:
                            self.__current_text += el.tail

                else:
                    if ev == 'start' and el.text:
                        self.__current_text += el.text

                    elif ev == 'end' and el.tail:
                        self.__current_text += el.tail

    def __is_excluded(self):
        return self.__excluded_stack_count > 0


class TableExtractor(AbstractExtractor):
    """
    Extracts tables from HTML as structured content and plain text.
    """

    def __init__(self):
        self.__current_table_row = []
        self.__current_text = ''
        self.__is_table = False
        self.__is_table_head = False
        self.__is_table_body = False
        self.__table_content = {'type': 'table', 'head': [], 'body': []}

    def extract(self, el, ev, structured_content: List[Dict[str, Any]], text_list: List[str]):
        if el.tag == 'table':
            if ev == 'start':
                self.__is_table = True

            elif ev == 'end':
                structured_content.append(self.__table_content)
                self.__is_table_body = False
                self.__is_table_head = False
                self.__is_table = False
                self.__current_text = ''
                self.__current_table_row = []
                self.__table_content = {'type': 'table', 'head': [], 'body': []}

        elif self.__is_table:
            # noinspection SpellCheckingInspection
            if el.tag == 'thead' and ev == 'start':
                self.__is_table_head = True
                self.__is_table_body = False

            elif el.tag == 'tbody' and ev == 'start':
                self.__is_table_head = False
                self.__is_table_body = True

            elif el.tag == 'tr' and ev == 'end':
                if self.__is_current_table_row_not_empty():
                    values = [v for _, v in self.__current_table_row]
                    text_list.append(r'\t'.join(values))
                    if not self.__is_table_head and (self.__is_table_body or not self.__is_header_row()):
                        self.__table_content['body'].append(values)
                        self.__is_table_head = False
                        self.__is_table_body = True

                    else:
                        self.__table_content['head'].append(values)

                self.__current_text = ''
                self.__current_table_row = []

            elif el.tag == 'th':
                if ev == 'end':
                    self.__current_table_row.append(('th', clean_text(self.__current_text)))

                self.__current_text = ''

            elif el.tag == 'td':
                if ev == 'end':
                    self.__current_table_row.append(('td', clean_text(self.__current_text)))

                self.__current_text = ''

            if ev == 'start' and el.text:
                self.__current_text += el.text
            elif ev == 'end' and el.tail:
                self.__current_text += el.tail

    def __is_current_table_row_not_empty(self) -> bool:
        return any(v for _, v in self.__current_table_row)

    def __is_header_row(self) -> bool:
        return all(k == 'th' for k, _ in self.__current_table_row)
