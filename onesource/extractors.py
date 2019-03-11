from tableschema import config, Schema, types
from typing import Any, Dict, List
from utils import clean_text, strip_link_markers

LINK_OPEN_MARKER = '[['

LINK_CLOSE_MARKER = ']]'

_INFER_TYPE_ORDER = [
    'duration',
    'geojson',
    'geopoint',
    'object',
    'array',
    'datetime',
    'time',
    'date',
    'integer',
    'number',
    'boolean',
    'string',
    'any',
]


class AbstractExtractor(object):
    """
    Interface for extractor types to implement.

    Extractors are stateful, so must be used per file.
    """

    def extract(self, el, ev, structured_content: List[Dict[str, Any]], text_list: List[str], nlp=None):
        """

        :param el: the current HTML element being processed
        :param ev: the type of event ['start', 'end']
        :param structured_content: a list to append structured content
        :param text_list: a list to append plain text
        :param nlp Spacy model
        :return: None
        """
        raise NotImplementedError


class HeadingExtractor(AbstractExtractor):
    """
    Extracts headings from HTML (H1 - H4) as structured content and plain text.
    """

    def __init__(self, excluded_tags: List[str] = None):
        """

        :param excluded_tags: do not extract headings within these tags
        """
        self.__current_text = ''
        self.__excluded_stack_count = 0
        self.__excluded_tags = ['ul', 'ol', 'table'] if excluded_tags is None else excluded_tags
        self.__is_heading = False
        self.__is_anchor = False
        self.__anchor_text = ''
        self.__anchor_url = None

    def extract(self, el, ev, structured_content: List[Dict[str, Any]], text_list: List[str], nlp=None):
        if el.tag in self.__excluded_tags:
            if ev == 'start':
                self.__excluded_stack_count += 1

            elif ev == 'end':
                self.__excluded_stack_count -= 1

        elif not self.__is_excluded():
            if el.tag in ['title', 'h1', 'h2', 'h3', 'h4']:
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
                            text_list.append(strip_link_markers(c))
                            structured_content.append({'type': 'heading', 'text': c})

            elif self.__is_heading:
                if el.tag == 'a':
                    if ev == 'start':
                        anchor_url = el.get('href')
                        if anchor_url:
                            self.__is_anchor = True
                            self.__current_text += LINK_OPEN_MARKER
                            self.__anchor_url = el.get('href')

                    elif ev == 'end' and self.__is_anchor:
                        self.__is_anchor = False
                        if self.__anchor_text.strip():
                            self.__current_text += LINK_CLOSE_MARKER
                            if self.__anchor_url and self.__anchor_text:
                                structured_content.append({
                                    'type': 'link',
                                    'url': self.__anchor_url,
                                    'text': self.__anchor_text
                                })
                        else:
                            n = self.__current_text.rfind(LINK_OPEN_MARKER)
                            self.__current_text = self.__current_text[:n] + ' '

                        self.__anchor_url = None
                        self.__anchor_text = ''

                if ev == 'start' and el.text:
                    self.__current_text += el.text
                    if self.__is_anchor:
                        self.__anchor_text += el.text

                if ev == 'end' and el.tail:
                    self.__current_text += el.tail
                    if self.__is_anchor:
                        self.__anchor_text += el.tail

    def __is_excluded(self):
        return self.__excluded_stack_count > 0


class TextExtractor(AbstractExtractor):
    """
    Extracts block text from HTML (p, div) as structured content and plain text.
    """

    def __init__(self, excluded_tags: List[str] = None):
        """

        :param excluded_tags: do not extract text within these tags
        """
        self.__current_text = ''
        self.__excluded_stack_count = 0
        if excluded_tags is None:
            self.__excluded_tags = ['ul', 'ol', 'table', 'title', 'h1', 'h2', 'h3', 'h4']
        else:
            self.__excluded_tags = excluded_tags

        self.__is_anchor = False
        self.__anchor_text = ''
        self.__anchor_url = None
        self.__text_continues = False

    def extract(self, el, ev, structured_content: List[Dict[str, Any]], text_list: List[str], nlp=None):
        if el.tag in self.__excluded_tags:
            if ev == 'start':
                self.__excluded_stack_count += 1
                if self.__current_text:
                    c = clean_text(self.__current_text)
                    self.__current_text = ''
                    if c:
                        # if c[0].isupper():
                        #     self.__text_continues = False

                        doc = nlp(c)
                        s = ''
                        for i, sent in enumerate(doc.sents):
                            if i > 0:
                                self.__text_continues = False

                            s = sent.text
                            if self.__text_continues:
                                text_list[-1] += ' ' + strip_link_markers(s)
                                structured_content[-1]['text'] += ' ' + s
                            else:
                                text_list.append(strip_link_markers(s))
                                if detect_heading(s, nlp):
                                    structured_content.append({'type': 'heading', 'text': s})
                                else:
                                    structured_content.append({'type': 'text', 'text': s})

                        if not s.endswith(('.', '?', '!')):
                            self.__text_continues = True
                        else:
                            self.__text_continues = False

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
                        # if c[0].isupper():
                        #     self.__text_continues = False

                        doc = nlp(c)
                        s = ''
                        for i, sent in enumerate(doc.sents):
                            if i > 0:
                                self.__text_continues = False

                            s = sent.text
                            if self.__text_continues:
                                text_list[-1] += ' ' + strip_link_markers(s)
                                structured_content[-1]['text'] += ' ' + s
                            else:
                                text_list.append(strip_link_markers(s))
                                if detect_heading(s, nlp):
                                    structured_content.append({'type': 'heading', 'text': s})
                                else:
                                    structured_content.append({'type': 'text', 'text': s})

                        if not s.endswith(('.', '?', '!')):
                            self.__text_continues = True
                        else:
                            self.__text_continues = False

                if el.tail:
                    self.__current_text += el.tail

            elif el.tag in ['p', 'div']:
                if ev == 'start':
                    if self.__current_text:
                        c = clean_text(self.__current_text)
                        self.__current_text = ''
                        if c:
                            # if c[0].isupper():
                            #     self.__text_continues = False

                            doc = nlp(c)
                            s = ''
                            for i, sent in enumerate(doc.sents):
                                if i > 0:
                                    self.__text_continues = False

                                s = sent.text
                                if self.__text_continues:
                                    text_list[-1] += ' ' + strip_link_markers(s)
                                    structured_content[-1]['text'] += ' ' + s
                                else:
                                    text_list.append(strip_link_markers(s))
                                    if detect_heading(s, nlp):
                                        structured_content.append({'type': 'heading', 'text': s})
                                    else:
                                        structured_content.append({'type': 'text', 'text': s})

                            if not s.endswith(('.', '?', '!')):
                                self.__text_continues = True
                            else:
                                self.__text_continues = False

                    if el.text:
                        self.__current_text += el.text

                elif ev == 'end':
                    if self.__current_text:
                        c = clean_text(self.__current_text)
                        self.__current_text = ''
                        if c:
                            # if c[0].isupper():
                            #     self.__text_continues = False

                            doc = nlp(c)
                            s = ''
                            for i, sent in enumerate(doc.sents):
                                if i > 0:
                                    self.__text_continues = False

                                s = sent.text
                                if self.__text_continues:
                                    text_list[-1] += ' ' + strip_link_markers(s)
                                    structured_content[-1]['text'] += ' ' + s
                                else:
                                    text_list.append(strip_link_markers(s))
                                    if detect_heading(s, nlp):
                                        structured_content.append({'type': 'heading', 'text': s})
                                    else:
                                        structured_content.append({'type': 'text', 'text': s})

                            if not s.endswith(('.', '?', '!')):
                                self.__text_continues = True
                            else:
                                self.__text_continues = False

                    if el.tail:
                        self.__current_text += el.tail

            else:
                if el.tag == 'a':
                    if ev == 'start':
                        anchor_url = el.get('href')
                        if anchor_url:
                            self.__is_anchor = True
                            self.__current_text += LINK_OPEN_MARKER
                            self.__anchor_url = anchor_url

                    elif ev == 'end' and self.__is_anchor:
                        self.__is_anchor = False
                        if self.__anchor_text.strip():
                            self.__current_text += LINK_CLOSE_MARKER
                            if self.__anchor_url and self.__anchor_text:
                                structured_content.append({
                                    'type': 'link',
                                    'url': self.__anchor_url,
                                    'text': self.__anchor_text
                                })
                        else:
                            n = self.__current_text.rfind(LINK_OPEN_MARKER)
                            self.__current_text = self.__current_text[:n] + ' '

                        self.__anchor_url = None
                        self.__anchor_text = ''

                elif el.tag == 'img' and ev == 'start':
                    url = el.get('src')
                    title = el.get('title') or el.get('alt') or url
                    structured_content.append({
                        'type': 'image',
                        'url': url,
                        'title': title
                    })
                    image_ref = f'{{image:{url}}}'
                    self.__current_text += image_ref
                    if self.__is_anchor:
                        self.__anchor_text += image_ref

                if ev == 'start' and el.text:
                    self.__current_text += el.text
                    if self.__is_anchor:
                        self.__anchor_text += el.text

                elif ev == 'end' and el.tail:
                    self.__current_text += el.tail
                    if self.__is_anchor:
                        self.__anchor_text += el.tail

    def __is_excluded(self):
        return self.__excluded_stack_count > 0


class ListExtractor(AbstractExtractor):
    """
    Extracts lists from HTML (ul, ol) as structured content and plain text.
    """

    __heading_tags = ['title', 'h1', 'h2', 'h3', 'h4', 'div', 'strong']

    def __init__(self, excluded_tags: List[str] = None):
        self.__excluded_tags = ['table'] if excluded_tags is None else excluded_tags
        self.__excluded_stack_count = 0
        self.__current_text = ''
        self.__heading_text = ''
        self.__is_heading = False
        self.__is_items = False
        self.__is_list = False
        self.__list_content = {'type': 'list', 'items': []}
        self.__is_anchor = False
        self.__anchor_text = ''
        self.__anchor_url = None

    def extract(self, el, ev, structured_content: List[Dict[str, Any]], text_list: List[str], nlp=None):
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
                            text_list.append(strip_link_markers(c))
                            if self.__is_heading:
                                self.__heading_text += self.__current_text
                            else:
                                structured_content.append({'type': 'text', 'text': c})

                    if self.__heading_text:
                        self.__list_content['heading'] = clean_text(self.__heading_text)

                    if self.__heading_text or self.__list_content['items']:
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
                                text_list.append(strip_link_markers(c))
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
                                text_list.append(strip_link_markers(c))
                                self.__list_content['items'].append(c)

                        if el.tail:
                            self.__current_text += el.tail

                elif el.tag == 'br' and ev == 'end':
                    if self.__current_text:
                        c = clean_text(self.__current_text)
                        if c:
                            text_list.append(strip_link_markers(c))
                            if self.__is_heading:
                                self.__heading_text += self.__current_text + ' '
                            else:
                                structured_content.append({'type': 'text', 'text': c})

                        self.__current_text = ''

                    if el.tail:
                        self.__current_text += el.tail

                elif el.tag in ['p', 'div', 'title', 'h1', 'h2', 'h3', 'h4']:
                    if ev == 'start':
                        if self.__current_text:
                            c = clean_text(self.__current_text)
                            if c:
                                text_list.append(strip_link_markers(c))
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
                                text_list.append(strip_link_markers(c))
                                if self.__is_heading:
                                    self.__heading_text += self.__current_text + ' '
                                else:
                                    structured_content.append({'type': 'text', 'text': c})

                            self.__current_text = ''

                        if el.tail:
                            self.__current_text += el.tail

                else:
                    if el.tag == 'a':
                        if ev == 'start':
                            anchor_url = el.get('href')
                            if anchor_url:
                                self.__is_anchor = True
                                self.__current_text += LINK_OPEN_MARKER
                                self.__anchor_url = el.get('href')

                        elif ev == 'end' and self.__is_anchor:
                            self.__is_anchor = False
                            if self.__anchor_text.strip():
                                self.__current_text += LINK_CLOSE_MARKER
                                if self.__anchor_url and self.__anchor_text:
                                    structured_content.append({
                                        'type': 'link',
                                        'url': self.__anchor_url,
                                        'text': self.__anchor_text
                                    })
                            else:
                                n = self.__current_text.rfind(LINK_OPEN_MARKER)
                                self.__current_text = self.__current_text[:n] + ' '

                            self.__anchor_url = None
                            self.__anchor_text = ''

                    if ev == 'start' and el.text:
                        self.__current_text += el.text
                        if self.__is_anchor:
                            self.__anchor_text += el.text

                    elif ev == 'end' and el.tail:
                        self.__current_text += el.tail
                        if self.__is_anchor:
                            self.__anchor_text += el.tail

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
        self.__table_content = None
        self.__table_stack = []
        self.__table_index = 1
        self.__is_anchor = False
        self.__anchor_text = ''
        self.__anchor_url = None
        self.schema = Schema()

    def extract(self, el, ev, structured_content: List[Dict[str, Any]], text_list: List[str], nlp=None):
        if el.tag == 'table':
            if ev == 'start':
                if self.__is_table:
                    ref = 'table:{}'.format(self.__table_index)
                    self.__current_text += f'{{{ref}}} '
                    self.__table_content.setdefault('references', []).append(ref)
                    self.__table_stack.append((
                        self.__current_table_row,
                        self.__current_text,
                        self.__is_table_head,
                        self.__is_table_body,
                        self.__table_content
                    ))
                self.__current_table_row = []
                self.__current_text = ''
                self.__is_table = True
                self.__is_table_head = False
                self.__is_table_body = False
                self.__table_content = {'type': 'table', 'index': self.__table_index, 'head': [], 'body': []}
                self.__table_index += 1

            elif ev == 'end':
                table = self.__table_content
                if table['body']:
                    if table['head']:
                        headers = table['head']
                        fields = self.schema.infer(table['body'], headers=headers)['fields']
                    else:
                        head = table['body'][0]
                        headers = ['name%d' % (i + 1) for i in range(len(head))]
                        fields = self.schema.infer(table['body'], headers=headers)['fields']
                        if len(table['body']) > 1:
                            dtypes = [field['type'] for field in fields]
                            if any([typ != guess_type(val) for typ, val in zip(dtypes, head)]):
                                table['head'] = [head]
                                table['body'] = table['body'][1:]
                                for field, name in zip(fields, head):
                                    field['name'] = name

                    table['fields'] = fields

                structured_content.append(table)
                if len(self.__table_stack):
                    (self.__current_table_row, self.__current_text,
                        self.__is_table_head, self.__is_table_body,
                        self.__table_content) = self.__table_stack.pop()
                else:
                    self.__is_table_body = False
                    self.__is_table_head = False
                    self.__is_table = False
                    self.__current_text = ''
                    self.__current_table_row = []
                    self.__table_content = None
                    self.__table_index = 1

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
                    text_list.append(strip_link_markers(r'\t'.join(values)))
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

            elif el.tag == 'a':
                if ev == 'start':
                    anchor_url = el.get('href')
                    if anchor_url:
                        self.__is_anchor = True
                        self.__current_text += LINK_OPEN_MARKER
                        self.__anchor_url = el.get('href')

                elif ev == 'end' and self.__is_anchor:
                    self.__is_anchor = False
                    if self.__anchor_text.strip():
                        self.__current_text += LINK_CLOSE_MARKER
                        if self.__anchor_url and self.__anchor_text:
                            structured_content.append({
                                'type': 'link',
                                'url': self.__anchor_url,
                                'text': self.__anchor_text
                            })
                    else:
                        n = self.__current_text.rfind(LINK_OPEN_MARKER)
                        self.__current_text = self.__current_text[:n] + ' '

                    self.__anchor_url = None
                    self.__anchor_text = ''

            if ev == 'start' and el.text:
                self.__current_text += el.text
                if self.__is_anchor:
                    self.__anchor_text += el.text

            elif ev == 'end' and el.tail:
                self.__current_text += el.tail
                if self.__is_anchor:
                    self.__anchor_text += el.tail

    def __is_current_table_row_not_empty(self) -> bool:
        return any(v for _, v in self.__current_table_row)

    def __is_header_row(self) -> bool:
        return all(k == 'th' for k, _ in self.__current_table_row)


def guess_type(value):
    """
    Guess the type for a value
    """
    for name in _INFER_TYPE_ORDER:
        cast = getattr(types, 'cast_%s' % name)
        result = cast('default', value)
        if result != config.ERROR:
            return name


def detect_heading(text, nlp):
    doc = nlp(text)
    for token in doc:
        if not (token.is_stop or token.is_title or token.is_upper or token.is_digit or token.is_punc):
            return False

    return True
