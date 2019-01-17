from extractors import HeadingExtractor, ListExtractor, TableExtractor, TextExtractor
from io import BytesIO
from lxml import etree
from utils import fix_content


def test_extract_basic_heading():
    structured_content = []
    text_list = []
    heading_extractor = HeadingExtractor()
    content = '<h1>My Heading</h1>'
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        heading_extractor.extract(elem, ev, structured_content, text_list)

    assert text_list[0] == 'My Heading'
    assert structured_content[0]['type'] == 'heading'
    assert structured_content[0]['text'] == 'My Heading'


def test_extract_complex_heading():
    structured_content = []
    text_list = []
    heading_extractor = HeadingExtractor()
    content = '<h1>My <span>Head</span>ing</h1>'
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        heading_extractor.extract(elem, ev, structured_content, text_list)

    assert text_list[0] == 'My Heading'
    assert structured_content[0]['type'] == 'heading'
    assert structured_content[0]['text'] == 'My Heading'


def test_extract_complex_heading_2():
    structured_content = []
    text_list = []
    heading_extractor = HeadingExtractor()
    content = """
    <h2 style="margin-top:0cm;margin-right:30.05pt;margin-bottom:0cm;margin-left:22.4pt;margin-bottom:.0001pt">
        <span style="font-family:calibri,sans-serif; font-size:11pt">
            Please <u>STOP</u> using the AIM process for this issue.
        </span>
    </h2>
    """
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        heading_extractor.extract(elem, ev, structured_content, text_list)

    assert text_list[0] == 'Please STOP using the AIM process for this issue.'
    assert structured_content[0]['type'] == 'heading'
    assert structured_content[0]['text'] == 'Please STOP using the AIM process for this issue.'


def test_extract_embedded_heading():
    structured_content = []
    text_list = []
    heading_extractor = HeadingExtractor()
    content = '<p>First</p><h1>My <span>Head</span>ing</h1><div>Last</div>'
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        heading_extractor.extract(elem, ev, structured_content, text_list)

    assert text_list[0] == 'My Heading'
    assert structured_content[0]['type'] == 'heading'
    assert structured_content[0]['text'] == 'My Heading'


def test_extract_enclosed_heading():
    structured_content = []
    text_list = []
    heading_extractor = HeadingExtractor()
    content = '<div>Start <h1>My <span>Head</span>ing</h1> End</div>'
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        heading_extractor.extract(elem, ev, structured_content, text_list)

    assert text_list[0] == 'My Heading'
    assert structured_content[0]['type'] == 'heading'
    assert structured_content[0]['text'] == 'My Heading'


def test_extract_heading_with_line_break():
    structured_content = []
    text_list = []
    heading_extractor = HeadingExtractor()
    content = '<h1>My<br> Heading</h1>'
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        heading_extractor.extract(elem, ev, structured_content, text_list)

    assert text_list[0] == 'My Heading'
    assert structured_content[0]['type'] == 'heading'
    assert structured_content[0]['text'] == 'My Heading'


def test_exclude_heading_in_list():
    structured_content = []
    text_list = []
    heading_extractor = HeadingExtractor(excluded_tags=['ul', 'ol'])
    content = '''
    <ul>
        <h2>List heading</h2>
        <li>One</li>
        <li>Two</li>
    </ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        heading_extractor.extract(elem, ev, structured_content, text_list)

    assert not text_list
    assert not structured_content


def test_extract_anchor_from_heading():
    structured_content = []
    text_list = []
    heading_extractor = HeadingExtractor()
    content = '<h1>My <a href="link-url">Heading</a></h1>'
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        heading_extractor.extract(elem, ev, structured_content, text_list)

    assert text_list[0] == 'My Heading'
    assert structured_content[0]['type'] == 'link'
    assert structured_content[0]['text'] == 'Heading'
    assert structured_content[0]['url'] == 'link-url'
    assert structured_content[1]['type'] == 'heading'
    assert structured_content[1]['text'] == 'My [[Heading]]'


def test_extract_anchor_from_heading2():
    structured_content = []
    text_list = []
    heading_extractor = HeadingExtractor()
    content = '<h1><a href="link-url">My Heading</a></h1>'
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        heading_extractor.extract(elem, ev, structured_content, text_list)

    assert text_list[0] == 'My Heading'
    assert structured_content[0]['type'] == 'link'
    assert structured_content[0]['text'] == 'My Heading'
    assert structured_content[0]['url'] == 'link-url'
    assert structured_content[1]['type'] == 'heading'
    assert structured_content[1]['text'] == '[[My Heading]]'


def test_extract_anchor_from_heading3():
    structured_content = []
    text_list = []
    heading_extractor = HeadingExtractor()
    content = '<h1>My <a href="link-url">Heading</a> text</h1>'
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        heading_extractor.extract(elem, ev, structured_content, text_list)

    assert text_list[0] == 'My Heading text'
    assert structured_content[0]['type'] == 'link'
    assert structured_content[0]['text'] == 'Heading'
    assert structured_content[0]['url'] == 'link-url'
    assert structured_content[1]['type'] == 'heading'
    assert structured_content[1]['text'] == 'My [[Heading]] text'


def test_extract_basic_text():
    structured_content = []
    text_list = []
    text_extractor = TextExtractor()
    content = '<p>My text</p>'
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        text_extractor.extract(elem, ev, structured_content, text_list)

    assert text_list[0] == 'My text'
    assert structured_content[0]['type'] == 'text'
    assert structured_content[0]['text'] == 'My text'


def test_extract_anchor_from_basic_text():
    structured_content = []
    text_list = []
    text_extractor = TextExtractor()
    content = '<p>My <a href="link-url">text</a></p>'
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        text_extractor.extract(elem, ev, structured_content, text_list)

    assert text_list[0] == 'My text'
    assert structured_content[0]['type'] == 'link'
    assert structured_content[0]['text'] == 'text'
    assert structured_content[0]['url'] == 'link-url'
    assert structured_content[1]['type'] == 'text'
    assert structured_content[1]['text'] == 'My [[text]]'


def test_extract_complex_text():
    structured_content = []
    text_list = []
    text_extractor = TextExtractor()
    content = '<p>My <font color="#ccc">colored</font> <a href="#">text</a> line</p>'
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        text_extractor.extract(elem, ev, structured_content, text_list)

    assert text_list[0] == 'My colored text line'
    assert structured_content[0]['type'] == 'link'
    assert structured_content[0]['text'] == 'text'
    assert structured_content[0]['url'] == '#'
    assert structured_content[1]['type'] == 'text'
    assert structured_content[1]['text'] == 'My colored [[text]] line'


def test_extract_embedded_text():
    structured_content = []
    text_list = []
    text_extractor = TextExtractor()
    content = '''
    <h1>My Heading</h1>
    <p>My <font color="#ccc">colored</font> <a href="#">text</a> line</p>
    <ul><li>List</li></ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        text_extractor.extract(elem, ev, structured_content, text_list)

    assert text_list[0] == 'My colored text line'
    assert structured_content[1]['type'] == 'text'
    assert structured_content[1]['text'] == 'My colored [[text]] line'


def test_extract_enclosed_text():
    structured_content = []
    text_list = []
    text_extractor = TextExtractor()
    content = '''
    <h1>My Heading</h1>
    <div>
    First line
    <p>My <font color="#ccc">colored</font> <a href="#">text</a> line</p>
    <span>Last</span>
    line
    </div>
    <ul><li>List</li></ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        text_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 3
    assert text_list[0] == 'First line'
    assert text_list[1] == 'My colored text line'
    assert text_list[2] == 'Last line'
    assert structured_content[2]['type'] == 'text'
    assert structured_content[2]['text'] == 'My colored [[text]] line'
    assert structured_content[3]['type'] == 'text'
    assert structured_content[3]['text'] == 'Last line'


def test_extract_enclosed_text2():
    structured_content = []
    text_list = []
    text_extractor = TextExtractor()
    content = '''
    <h1>My Heading</h1>
    <div>
    First line
    <p>My <font color="#ccc">colored</font> <a href="#">text</a> line</p>
    Last line
    </div>
    <ul><li>List</li></ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        text_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 3
    assert text_list[0] == 'First line'
    assert text_list[1] == 'My colored text line'
    assert text_list[2] == 'Last line'
    assert structured_content[2]['type'] == 'text'
    assert structured_content[2]['text'] == 'My colored [[text]] line'
    assert structured_content[3]['type'] == 'text'
    assert structured_content[3]['text'] == 'Last line'


def test_extract_trailing_text():
    structured_content = []
    text_list = []
    text_extractor = TextExtractor()
    content = '''
    <h1>My Heading</h1>
    <div>
    First line
    <p>My <font color="#ccc">colored</font> <a href="#">text</a> line</p>
    Last line
    </div>
    Trailing line
    <ul><li>List</li></ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        text_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 4
    assert text_list[3] == 'Trailing line'
    assert structured_content[4]['type'] == 'text'
    assert structured_content[4]['text'] == 'Trailing line'


def test_extract_trailing_text_at_eod():
    structured_content = []
    text_list = []
    text_extractor = TextExtractor()
    content = '''
    <h1>My Heading</h1>
    <div>
    First line
    <p>My <font color="#ccc">colored</font> <a href="#">text</a> line</p>
    Last line
    </div>
    Trailing line
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        text_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 4
    assert text_list[3] == 'Trailing line'
    assert structured_content[4]['type'] == 'text'
    assert structured_content[4]['text'] == 'Trailing line'


def test_extract_preceding_text():
    structured_content = []
    text_list = []
    text_extractor = TextExtractor()
    content = '''
    <h1>My Heading</h1>
    Preceding line
    <div>
    First line
    <p>My <font color="#ccc">colored</font> <a href="#">text</a> line</p>
    Last line
    </div>
    Trailing line
    <ul><li>List</li></ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        text_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 5
    assert text_list[0] == 'Preceding line'
    assert structured_content[0]['type'] == 'text'
    assert structured_content[0]['text'] == 'Preceding line'


def test_extract_text_with_line_break():
    structured_content = []
    text_list = []
    text_extractor = TextExtractor()
    content = '<p>My<br> text</p>'
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        text_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 2
    assert text_list[1] == 'text'
    assert structured_content[1]['type'] == 'text'
    assert structured_content[1]['text'] == 'text'


def test_exclude_text_in_list():
    structured_content = []
    text_list = []
    text_extractor = TextExtractor(excluded_tags=['ul', 'ol', 'table', 'title', 'h1', 'h2', 'h3', 'h4'])
    content = '''
    <ul>
        <h2>List heading</h2>
        <p>Second line</p>
        Third line
        <li>One</li>
        <li>Two</li>
    </ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        text_extractor.extract(elem, ev, structured_content, text_list)

    assert not text_list
    assert not structured_content


def test_extract_basic_unordered_list():
    structured_content = []
    text_list = []
    list_extractor = ListExtractor()
    content = '''
    <ul>
        <li>One</li>
        <li>Two</li>
        <li>Three</li>
    </ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        list_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 3
    assert len(structured_content[0]['items']) == 3
    assert text_list[1] == 'Two'
    assert structured_content[0]['type'] == 'list'
    assert structured_content[0]['items'][2] == 'Three'


def test_extract_anchor_from_basic_unordered_list():
    structured_content = []
    text_list = []
    list_extractor = ListExtractor()
    content = '''
    <ul>
        <li>One</li>
        <li>Two <a href="link-url">embedded</a> link</li>
        <li>Three</li>
    </ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        list_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 3
    assert len(structured_content[1]['items']) == 3
    assert text_list[1] == 'Two embedded link'
    assert structured_content[0]['type'] == 'link'
    assert structured_content[0]['text'] == 'embedded'
    assert structured_content[0]['url'] == 'link-url'
    assert structured_content[1]['type'] == 'list'
    assert structured_content[1]['items'][1] == 'Two [[embedded]] link'


def test_extract_basic_ordered_list():
    structured_content = []
    text_list = []
    list_extractor = ListExtractor()
    content = '''
    <ol>
        <li>One</li>
        <li>Two</li>
        <li>Three</li>
    </ol>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        list_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 3
    assert len(structured_content[0]['items']) == 3
    assert text_list[1] == 'Two'
    assert structured_content[0]['type'] == 'list'
    assert structured_content[0]['items'][2] == 'Three'


def test_extract_complex_list_items():
    structured_content = []
    text_list = []
    list_extractor = ListExtractor()
    content = '''
    <ul>
        <li>First <a href="#">link</a> item</li>
        <li>Two</li>
        <li>Three</li>
    </ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        list_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 3
    assert len(structured_content[1]['items']) == 3
    assert text_list[0] == 'First link item'
    assert structured_content[1]['type'] == 'list'
    assert structured_content[1]['items'][0] == 'First [[link]] item'


def test_extract_list_with_embedded_heading():
    structured_content = []
    text_list = []
    list_extractor = ListExtractor()
    content = '''
    <ul>
        <h2>List heading</h2>
        <li>First <a href="#">link</a> item</li>
        <li>Two</li>
        <li>Three</li>
    </ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        list_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 4
    assert len(structured_content[1]['items']) == 3
    assert text_list[0] == 'List heading'
    assert text_list[1] == 'First link item'
    assert structured_content[1]['type'] == 'list'
    assert structured_content[1]['heading'] == 'List heading'
    assert structured_content[1]['items'][0] == 'First [[link]] item'


def test_extract_text_at_head_of_list():
    structured_content = []
    text_list = []
    list_extractor = ListExtractor()
    content = '''
    <ul>
        <h2>List heading</h2>
        <p>Second line</p>
        Third line
        <li>First <a href="#">link</a> item</li>
        <li>Two</li>
        <li>Three</li>
    </ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        list_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 6
    assert len(structured_content[1]['items']) == 3
    assert text_list[0] == 'List heading'
    assert text_list[1] == 'Second line'
    assert structured_content[1]['type'] == 'list'
    assert structured_content[1]['heading'] == 'List heading Second line Third line'
    assert structured_content[1]['items'][0] == 'First [[link]] item'


def test_extract_text_with_line_breaks_at_head_of_list():
    structured_content = []
    text_list = []
    list_extractor = ListExtractor()
    content = '''
    <ul>
        <h2>List heading</h2>
        <p>Second<br> line</p>
        Third line
        <li>First <a href="#">link</a> item</li>
        <li>Two</li>
        <li>Three</li>
    </ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        list_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 7
    assert len(structured_content[1]['items']) == 3
    assert text_list[0] == 'List heading'
    assert text_list[2] == 'line'
    assert structured_content[1]['type'] == 'list'
    assert structured_content[1]['heading'] == 'List heading Second line Third line'
    assert structured_content[1]['items'][0] == 'First [[link]] item'


def test_extract_text_between_list_items():
    structured_content = []
    text_list = []
    list_extractor = ListExtractor()
    content = '''
    <ul>
        <li>First <a href="#">link</a> item</li>
        <h2>List heading</h2>
        <li>Two</li>
        <li>Three</li>
    </ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        list_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 4
    assert len(structured_content[2]['items']) == 3
    assert text_list[0] == 'First link item'
    assert text_list[1] == 'List heading'
    assert text_list[2] == 'Two'
    assert structured_content[1]['type'] == 'text'
    assert structured_content[1]['text'] == 'List heading'
    assert structured_content[2]['type'] == 'list'
    assert structured_content[2]['items'][0] == 'First [[link]] item'
    assert structured_content[2]['items'][1] == 'Two'
    assert 'heading' not in structured_content[1]


def test_extract_untagged_text_between_list_items():
    structured_content = []
    text_list = []
    list_extractor = ListExtractor()
    content = '''
    <ul>
        <li>First <a href="#">link</a> item</li>
        List heading
        <li>Two</li>
        <li>Three</li>
    </ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        list_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 4
    assert len(structured_content[2]['items']) == 3
    assert text_list[0] == 'First link item'
    assert text_list[1] == 'List heading'
    assert text_list[2] == 'Two'
    assert structured_content[1]['type'] == 'text'
    assert structured_content[1]['text'] == 'List heading'
    assert structured_content[2]['type'] == 'list'
    assert structured_content[2]['items'][0] == 'First [[link]] item'
    assert structured_content[2]['items'][1] == 'Two'
    assert 'heading' not in structured_content[2]


def test_extract_multi_line_text_between_list_items():
    structured_content = []
    text_list = []
    list_extractor = ListExtractor()
    content = '''
    <ul>
        <li>First <a href="#">link</a> item</li>
        <h2>List heading</h2>
        <p>Second<br> line</p>
        Third line
        <li>Two</li>
        <li>Three</li>
    </ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        list_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 7
    assert len(structured_content[5]['items']) == 3
    assert text_list[0] == 'First link item'
    assert text_list[1] == 'List heading'
    assert text_list[3] == 'line'
    assert structured_content[2]['type'] == 'text'
    assert structured_content[2]['text'] == 'Second'
    assert structured_content[5]['type'] == 'list'
    assert structured_content[5]['items'][0] == 'First [[link]] item'
    assert structured_content[5]['items'][1] == 'Two'
    assert 'heading' not in structured_content[2]


def test_extract_text_at_end_of_list():
    structured_content = []
    text_list = []
    list_extractor = ListExtractor()
    content = '''
    <ul>
        <li>First <a href="#">link</a> item</li>
        <li>Two</li>
        <li>Three</li>
        <h2>List heading</h2>
    </ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        list_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 4
    assert len(structured_content[2]['items']) == 3
    assert text_list[2] == 'Three'
    assert text_list[3] == 'List heading'
    assert structured_content[1]['type'] == 'text'
    assert structured_content[1]['text'] == 'List heading'
    assert structured_content[2]['type'] == 'list'
    assert structured_content[2]['items'][2] == 'Three'
    assert 'heading' not in structured_content[2]


def test_extract_untagged_text_at_end_of_list():
    structured_content = []
    text_list = []
    list_extractor = ListExtractor()
    content = '''
    <ul>
        <li>First <a href="#">link</a> item</li>
        <li>Two</li>
        <li>Three</li>
        List heading
    </ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        list_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 4
    assert len(structured_content[2]['items']) == 3
    assert text_list[2] == 'Three'
    assert text_list[3] == 'List heading'
    assert structured_content[1]['type'] == 'text'
    assert structured_content[1]['text'] == 'List heading'
    assert structured_content[2]['type'] == 'list'
    assert structured_content[2]['items'][2] == 'Three'
    assert 'heading' not in structured_content[2]


def test_extract_list_with_heading_and_no_items():
    structured_content = []
    text_list = []
    list_extractor = ListExtractor()
    content = '''
    <ul>
        <h2>List heading</h2>
    </ul>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        list_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 1
    assert len(structured_content[0]['items']) == 0
    assert text_list[0] == 'List heading'
    assert structured_content[0]['type'] == 'list'
    assert structured_content[0]['heading'] == 'List heading'


def test_extract_basic_table():
    structured_content = []
    text_list = []
    table_extractor = TableExtractor()
    content = '''
    <table>
        <tr>
            <th>Column Heading 1</th>
            <th>Column Heading 2</th>
            <th>Column Heading 3</th>
        </tr>
        <tr>
            <td>Row 1 Column 1</td>
            <td>Row 1 Column 2</td>
            <td>Row 1 Column 3</td>
        </tr>
        <tr>
            <td>Row 2 Column 1</td>
            <td>Row 2 Column 2</td>
            <td>Row 2 Column 3</td>
        </tr>
    </table>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        table_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 3
    assert len(structured_content[0]['body'][0]) == 3
    assert text_list[0] == r'Column Heading 1\tColumn Heading 2\tColumn Heading 3'
    assert structured_content[0]['type'] == 'table'
    assert structured_content[0]['head'][0][0] == 'Column Heading 1'
    assert structured_content[0]['body'][0][0] == 'Row 1 Column 1'


def test_extract_anchor_from_basic_table():
    structured_content = []
    text_list = []
    table_extractor = TableExtractor()
    content = '''
    <table>
        <tr>
            <th>Column Heading 1</th>
            <th>Column Heading 2</th>
            <th>Column Heading 3</th>
        </tr>
        <tr>
            <td>Row 1 Column 1</td>
            <td>Row 1 <a href="link-url">Column 2</a></td>
            <td>Row 1 Column 3</td>
        </tr>
        <tr>
            <td>Row 2 Column 1</td>
            <td>Row 2 Column 2</td>
            <td>Row 2 Column 3</td>
        </tr>
    </table>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        table_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 3
    assert len(structured_content[1]['body'][0]) == 3
    assert text_list[0] == r'Column Heading 1\tColumn Heading 2\tColumn Heading 3'
    assert structured_content[0]['type'] == 'link'
    assert structured_content[0]['text'] == 'Column 2'
    assert structured_content[0]['url'] == 'link-url'
    assert structured_content[1]['type'] == 'table'
    assert structured_content[1]['head'][0][0] == 'Column Heading 1'
    assert structured_content[1]['body'][0][1] == 'Row 1 [[Column 2]]'


def test_extract_table_with_column_1_headers():
    structured_content = []
    text_list = []
    table_extractor = TableExtractor()
    content = '''
    <table>
        <tr>
            <th>Column Heading 1</th>
            <th>Column Heading 2</th>
            <th>Column Heading 3</th>
        </tr>
        <tr>
            <th>Row 1 Column 1</th>
            <td>Row 1 Column 2</td>
            <td>Row 1 Column 3</td>
        </tr>
        <tr>
            <th>Row 2 Column 1</th>
            <td>Row 2 Column 2</td>
            <td>Row 2 Column 3</td>
        </tr>
    </table>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        table_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 3
    assert len(structured_content[0]['body'][0]) == 3
    assert text_list[0] == r'Column Heading 1\tColumn Heading 2\tColumn Heading 3'
    assert structured_content[0]['type'] == 'table'
    assert structured_content[0]['head'][0][0] == 'Column Heading 1'
    assert structured_content[0]['body'][0][0] == 'Row 1 Column 1'


def test_extract_table_with_head_and_body_tags():
    structured_content = []
    text_list = []
    table_extractor = TableExtractor()
    # noinspection SpellCheckingInspection
    content = '''
    <table>
        <thead>
            <tr>
                <td>Column Heading 1</td>
                <td>Column Heading 2</td>
                <td>Column Heading 3</td>
            </tr>
        </thead>
        <tbody>
            <tr>
                <th>Row 1 Column 1</th>
                <td>Row 1 Column 2</td>
                <td>Row 1 Column 3</td>
            </tr>
            <tr>
                <th>Row 2 Column 1</th>
                <td>Row 2 Column 2</td>
                <td>Row 2 Column 3</td>
            </tr>
        </tbody>
    </table>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        table_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 3
    assert len(structured_content[0]['body'][0]) == 3
    assert text_list[0] == r'Column Heading 1\tColumn Heading 2\tColumn Heading 3'
    assert structured_content[0]['type'] == 'table'
    assert structured_content[0]['head'][0][0] == 'Column Heading 1'
    assert structured_content[0]['body'][0][0] == 'Row 1 Column 1'


def test_extract_table_with_embedded_tags():
    structured_content = []
    text_list = []
    table_extractor = TableExtractor()
    content = '''
    <table>
        <tr>
            <th>Column Heading 1</th>
            <th>Column Heading 2</th>
            <th>Column Heading 3</th>
        </tr>
        <tr>
            <td><strong>Row 1</strong> <a href="#">Column</a> 1</td>
            <td><ul><li>Row 1</li> <li>Column 2</li></ul></td>
            <td>Row 1 Column 3</td>
        </tr>
        <tr>
            <td><div>Row 2</div> Column 1</td>
            <td>Row 2 Column 2</td>
            <td>Row 2 Column 3</td>
        </tr>
    </table>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        table_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 3
    assert len(structured_content[1]['body'][0]) == 3
    assert text_list[1] == r'Row 1 Column 1\tRow 1 Column 2\tRow 1 Column 3'
    assert text_list[2] == r'Row 2 Column 1\tRow 2 Column 2\tRow 2 Column 3'
    assert structured_content[1]['type'] == 'table'
    assert structured_content[1]['body'][0][0] == 'Row 1 [[Column]] 1'
    assert structured_content[1]['body'][0][1] == 'Row 1 Column 2'
    assert structured_content[1]['body'][1][0] == 'Row 2 Column 1'


def test_extract_table_with_multiple_header_rows():
    structured_content = []
    text_list = []
    table_extractor = TableExtractor()
    content = '''
    <table>
        <tr>
            <th>Column Heading 1</th>
            <th>Column Heading 2</th>
            <th>Column Heading 3</th>
        </tr>
        <tr>
            <th>Row 1 Column 1</th>
            <th>Row 1 Column 2</th>
            <th>Row 1 Column 3</th>
        </tr>
        <tr>
            <th>Row 2 Column 1</th>
            <td>Row 2 Column 2</td>
            <td>Row 2 Column 3</td>
        </tr>
    </table>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        table_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 3
    assert len(structured_content[0]['head']) == 2
    assert len(structured_content[0]['body']) == 1
    assert text_list[0] == r'Column Heading 1\tColumn Heading 2\tColumn Heading 3'
    assert structured_content[0]['type'] == 'table'
    assert structured_content[0]['head'][1][0] == 'Row 1 Column 1'
    assert structured_content[0]['body'][0][0] == 'Row 2 Column 1'


def test_extract_table_with_multiple_header_rows_using_head_tag():
    structured_content = []
    text_list = []
    table_extractor = TableExtractor()
    # noinspection SpellCheckingInspection
    content = '''
    <table>
        <thead>
            <tr>
                <td>Column Heading 1</td>
                <td>Column Heading 2</td>
                <td>Column Heading 3</td>
            </tr>
            <tr>
                <td>Row 1 Column 1</td>
                <td>Row 1 Column 2</td>
                <td>Row 1 Column 3</td>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>Row 2 Column 1</td>
                <td>Row 2 Column 2</td>
                <td>Row 2 Column 3</td>
            </tr>
        </tbody>
    </table>
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        table_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 3
    assert len(structured_content[0]['head']) == 2
    assert len(structured_content[0]['body']) == 1
    assert text_list[0] == r'Column Heading 1\tColumn Heading 2\tColumn Heading 3'
    assert structured_content[0]['type'] == 'table'
    assert structured_content[0]['head'][1][0] == 'Row 1 Column 1'
    assert structured_content[0]['body'][0][0] == 'Row 2 Column 1'


def test_extract_text_and_list_combo():
    structured_content = []
    text_list = []
    text_extractor = TextExtractor(excluded_tags=['ul', 'ol'])
    list_extractor = ListExtractor()
    content = '''
    <h1>My Heading</h1>
    <div>
    First line
    <p>My <font color="#ccc">colored</font> <a href="#">text</a> line</p>
    Last line
    </div>
    <ul>
        <li>First <a href="#">link</a> item</li>
        <li>Two</li>
        <li>Three</li>
    </ul>
    Trailing line
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        text_extractor.extract(elem, ev, structured_content, text_list)
        list_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 8
    assert text_list[4] == 'First link item'
    assert text_list[7] == 'Trailing line'
    assert structured_content[4]['type'] == 'text'
    assert structured_content[4]['text'] == 'Last line'
    assert structured_content[6]['type'] == 'list'
    assert structured_content[6]['items'][0] == 'First [[link]] item'


def test_extract_heading_and_text_combo():
    structured_content = []
    text_list = []
    heading_extractor = HeadingExtractor()
    text_extractor = TextExtractor(excluded_tags=['ul', 'ol', 'title', 'h1', 'h2', 'h3', 'h4'])
    content = '''
    <h1>My Heading</h1>
    <div>
    First line
    <p>My <font color="#ccc">colored</font> <a href="#">text</a> line</p>
    Last line
    </div>
    <ul>
        <li>First <a href="#">link</a> item</li>
        <li>Two</li>
        <li>Three</li>
    </ul>
    Trailing line
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        heading_extractor.extract(elem, ev, structured_content, text_list)
        text_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 5
    assert text_list[0] == 'My Heading'
    assert text_list[2] == 'My colored text line'
    assert text_list[4] == 'Trailing line'
    assert structured_content[0]['type'] == 'heading'
    assert structured_content[0]['text'] == 'My Heading'
    assert structured_content[4]['type'] == 'text'
    assert structured_content[4]['text'] == 'Last line'


def test_extract_text_and_table_combo():
    structured_content = []
    text_list = []
    text_extractor = TextExtractor(excluded_tags=['table'])
    table_extractor = TableExtractor()
    content = '''
    <h1>My Heading</h1>
    <div>
    First line
    <p>My <font color="#ccc">colored</font> <a href="#">text</a> line</p>
    Last line
    </div>
    <table>
        <tr>
            <th>Column Heading 1</th>
            <th>Column Heading 2</th>
            <th>Column Heading 3</th>
        </tr>
        <tr>
            <th>Row 1 Column 1</th>
            <td>Row 1 Column 2</td>
            <td>Row 1 Column 3</td>
        </tr>
        <tr>
            <th>Row 2 Column 1</th>
            <td>Row 2 Column 2</td>
            <td>Row 2 Column 3</td>
        </tr>
    </table>
    Trailing line
    '''
    stream = BytesIO(fix_content(content).encode('utf-8'))
    for ev, elem in etree.iterparse(stream, events=('start', 'end'), html=True):
        text_extractor.extract(elem, ev, structured_content, text_list)
        table_extractor.extract(elem, ev, structured_content, text_list)

    assert len(text_list) == 8
    assert text_list[2] == 'My colored text line'
    assert text_list[5] == r'Row 1 Column 1\tRow 1 Column 2\tRow 1 Column 3'
    assert text_list[7] == 'Trailing line'
    assert structured_content[4]['type'] == 'text'
    assert structured_content[4]['text'] == 'Last line'
    assert structured_content[5]['type'] == 'table'
    assert structured_content[5]['body'][0][0] == 'Row 1 Column 1'
