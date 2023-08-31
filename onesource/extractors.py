from collections import defaultdict
import re
from tableschema import config, Schema, types
from typing import Any, Dict, List
from utils import clean_text, remove_bullet_markers, strip_link_markers

BULLET_MARKERS = [u'•', '*', 'o']

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

    def _process_text(self, text: str, structured_content: List[Dict[str, Any]], text_list: List[str], nlp=None):
        # ignore blank text
        if len(text.strip()) == 0:
            return

        # split into sentences
        doc = nlp(text)
        obj_list = []
        list_levels = defaultdict(list)
        i = 0
        for sent in doc.sents:
            # ignore blank sentences
            if len(sent.text.strip()) == 0:
                continue

            # split by bullet marker or list number if present
            list_item_text = []
            cur_text = []
            n = len(sent)
            is_list = False
            for j, token in enumerate(sent):
                next_token = sent[j + 1] if (j + 2) < n else None
                if j == 0:
                    if is_bullet(token):
                        list_item_text = []
                        obj = {'type': 'unordered_list_item', 'tokens': list_item_text}
                        obj_list.append(obj)
                        list_levels[token.shape_].append(obj)
                        is_list = True
                    elif is_ordered_list_item(token, next_token):
                        list_item_text = []
                        obj = {'type': 'ordered_list_item', 'tokens': list_item_text}
                        obj_list.append(obj)
                        list_levels[token.shape_].append(obj)
                        is_list = True

                if is_list:
                    list_item_text.append(token)
                else:
                    cur_text.append(token)

            if len(cur_text) > 0:
                obj_list.append({'type': 'text', 'tokens': cur_text})

            i += 1

        def is_list_item(it):
            return it['type'] in ['unordered_list_item', 'ordered_list_item']

        def is_ordered_li(it):
            return it['type'] == 'ordered_list_item'

        def is_ordered_li_and_not_in_list(its, idx):
            if not is_ordered_li(its[idx]):
                return False

            if idx > 0 and is_ordered_li(its[idx - 1]):
                return False

            if idx < (len(its) - 1) and is_ordered_li(its[idx + 1]):
                return False

            return True

        def is_text_item(it):
            return it['type'] == 'text'

        def get_text(tks):
            return ''.join([t.text_with_ws for t in tks]).rstrip()

        for i, obj in enumerate(obj_list):
            tokens = obj['tokens']
            txt = clean_text(get_text(tokens))
            out = remove_bullet_markers(txt)
            if is_text_item(obj):
                if has_length(text_list) and continues(text_list[-1], txt, nlp):
                    text_list[-1] += ' ' + strip_link_markers(out)
                    if structured_content[-1]['type'] == 'list':
                        structured_content[-1]['items'][-1] += ' ' + out
                    else:
                        structured_content[-1]['text'] += ' ' + out
                else:
                    text_list.append(strip_link_markers(out))
                    if maybe_heading(txt, nlp):
                        structured_content.append({'type': 'heading', 'text': out})
                    else:
                        structured_content.append({'type': 'text', 'text': out})
            elif is_list_item(obj):
                list_marker = tokens[0]
                list_subtype = 'ordered' if is_ordered_li(obj) else 'unordered'
                if (structured_content[-1]['type'] == 'list' and
                        structured_content[-1]['subtype'] == list_subtype):
                    text_list.append(strip_link_markers(out))
                    structured_content[-1]['items'].append(out)
                elif is_ordered_li_and_not_in_list(obj_list, i) and maybe_heading(tokens):
                    text_list.append(strip_link_markers(out))
                    structured_content.append({'type': 'heading', 'text': out})
                elif is_ordered_li_and_not_in_list(obj_list, i):
                    text_list.append(strip_link_markers(out))
                    structured_content.append({'type': 'text', 'text': out})
                elif (structured_content[-1]['type'] in ['text', 'heading'] and
                      not (is_ordered_li(obj) and list_marker.text.lower() not in ['1', 'a', 'i'])):
                    text_list.append(strip_link_markers(out))
                    structured_content[-1] = {
                        'type': 'list',
                        'subtype': list_subtype,
                        'heading': structured_content[-1]['text'],
                        'items': [out]
                    }
                elif is_ordered_li(obj) and list_marker.text.lower() not in ['1', 'a', 'i']:
                    if maybe_heading(tokens):
                        text_list.append(strip_link_markers(out))
                        structured_content.append({'type': 'heading', 'text': out})
                    else:
                        text_list.append(strip_link_markers(out))
                        structured_content.append({'type': 'text', 'text': out})
                else:
                    text_list.append(strip_link_markers(out))
                    structured_content.append({'type': 'list', 'subtype': list_subtype, 'items': [out]})
            else:
                raise NotImplementedError

    def extract(self, el, ev, structured_content: List[Dict[str, Any]], text_list: List[str], nlp=None):
        if el.tag in self.__excluded_tags:
            if ev == 'start':
                self.__excluded_stack_count += 1
                if self.__current_text:
                    c = self.__current_text
                    self.__current_text = ''
                    if c:
                        self._process_text(c, structured_content, text_list, nlp)

            elif ev == 'end':
                self.__excluded_stack_count -= 1
                self.__current_text = ''
                if el.tail:
                    self.__current_text += el.tail

        elif not self.__is_excluded():
            if el.tag == 'br' and ev == 'end':
                if self.__current_text:
                    c = self.__current_text
                    self.__current_text = ''
                    if c:
                        self._process_text(c, structured_content, text_list, nlp)

                if el.tail:
                    self.__current_text += el.tail

            elif el.tag in ['p', 'div']:
                if ev == 'start':
                    if self.__current_text:
                        c = self.__current_text
                        self.__current_text = ''
                        if c:
                            self._process_text(c, structured_content, text_list, nlp)

                    if el.text:
                        self.__current_text += el.text

                elif ev == 'end':
                    if self.__current_text:
                        c = self.__current_text
                        self.__current_text = ''
                        if c:
                            self._process_text(c, structured_content, text_list, nlp)

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
        self.__list_content = {'type': 'list', 'subtype': 'unordered', 'items': []}
        self.__is_anchor = False
        self.__anchor_text = ''
        self.__anchor_url = None
        self.__list_level = 0

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
                    self.__list_level += 1

                elif ev == 'end':
                    # flatten nested lists
                    self.__list_level -= 1
                    if self.__list_level == 0:
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

                        self.__list_content = {'type': 'list', 'subtype': 'unordered', 'items': []}
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


def has_alpha(text, nlp):
    doc = nlp(text)
    for token in doc:
        if token.is_alpha:
            return True

    return False


def has_length(list):
    return len(list) > 0


def maybe_heading(text_or_tokens, nlp=None):
    if nlp is None:
        doc = text_or_tokens
    else:
        doc = nlp(text_or_tokens)

    n = len(doc)
    for i, token in enumerate(doc):
        if '\n' in token.text:
            continue

        next_token = doc[i + 1] if (i + 2) < n else None
        if i == 0:
            if not (token.is_title or token.is_upper or is_ordered_list_item(token, next_token)):
                return False
        elif i == (n - 1):
            if not (token.is_title or token.is_upper or token.is_digit or token.text in ['.', ':', ')']):
                return False
        else:
            if not (token.is_title or token.is_upper or token.is_stop or token.is_punct or
                    token.is_digit or token.text in [',', '-']):
                return False

    return True


def is_bullet(token):
    return token.text in BULLET_MARKERS


def is_list_num(token):
    match = re.match(r'^(\d\.?){1,3}$', token.text.strip())
    return True if match else False


def is_ordered_list_item(token, next_token):
    if next_token and next_token.text in ['.', ')']:
        if is_list_num(token):
            return True

        if token.is_alpha and len(token) == 1:
            return True

        if is_roman_numeral(token):
            return True

    if is_roman_numeral(token):
        return True

    return False


def is_roman_numeral(token):
    """
    Validate if a Spacy Token is a roman numeral

    See https://stackoverflow.com/questions/267399/how-do-you-match-only-valid-roman-numerals-with-a-regular-expression

    :param token: Spacy Token
    :return: (bool)
    """
    if token.is_space or token.text in ['.', ')']:
        return False

    match = re.match(r'^(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})[.)]?$', token.text.strip(), re.IGNORECASE)
    return True if match else False


def continues(leading_text, following_text, nlp):
    if leading_text.endswith(('.', '?', '!')):
        return False

    if leading_text.endswith((',', ';', ':')):
        return True

    is_leading_heading = maybe_heading(leading_text, nlp)
    is_following_heading = maybe_heading(following_text, nlp)
    if is_leading_heading or is_following_heading:
        return False
    else:
        return True
