from extract import ExtractStep
from io import BytesIO
from mock import Mock
from pipeline import Pipeline

# noinspection SpellCheckingInspection
CONTROL_DATA = {
    "files": [
        {
            "filename": "content_a588dacde15047dda4ec82feacf80b24.xml",
            "path": "/Users/markmo/onesource/AgentAssist/Python/test/in1/content_a588dacde15047dda4ec82feacf80b24.xml",
            "status": "processed",
            "time": "2018-03-11T00:30:44.431597"
        }
    ],
    "job": {
        "read_root_dir": "/Users/markmo/onesource/AgentAssist/Python/test/in1",
        "start": "2018-03-11T00:30:44.418791",
        "status": "processed",
        "write_root_dir": "/Users/markmo/onesource/AgentAssist/Python/test/temp"
    }
}


class FakeFile(BytesIO):

    def __init__(self, b):
        super().__init__(b)
        self.name = 'Test'


# noinspection PyUnusedLocal
def mock_file_iter(file_paths):
    # HTML must be wrapped in a CDATA section otherwise treated as XML
    # noinspection SpellCheckingInspection
    file = FakeFile(b"""
    <CONTENT RECORDID="a588dacde15047dda4ec82feacf80b24">
        <MASTERIDENTIFIER><![CDATA[Offers Finder]]></MASTERIDENTIFIER>
        <TYPE><![CDATA[CHANNEL_ANSWERFLOW_STEPS]]></TYPE>
        <DOCUMENTID><![CDATA[AFS4361]]></DOCUMENTID>
        <VERSION>1.0</VERSION>
        <AUTHOR><![CDATA[Mark Moloney]]></AUTHOR>
        <STARTTIMESTAMP_MILLIS>1515717900000</STARTTIMESTAMP_MILLIS>
        <LASTMODIFIEDTIMESTAMP_MILLIS>1515717972263</LASTMODIFIEDTIMESTAMP_MILLIS>
        <RESOURCEPATH>/sites/ACME01/content/live/CHANNEL_ANSWERFLOW_STEPS/4000/AFS4361/en_AU/</RESOURCEPATH>
        <PUBLISHEDTIMESTAMP_MILLIS>1515717972222</PUBLISHEDTIMESTAMP_MILLIS>
        <CHANNEL_ANSWERFLOW_STEPS>
            <STEP_TITLE><![CDATA[Offers Finder]]></STEP_TITLE>
            <REFERENCE_TABLE><![CDATA[
                <h1>My Heading</h1>
                <div>
                    First line
                    <p>My <font color="#ccc">colored</font> <a href="#">text</a> line</p>
                    Last line
                </div>
                Trailing line
                <ul><li>List</li></ul>
                ]]>
            </REFERENCE_TABLE>
        </CHANNEL_ANSWERFLOW_STEPS>
    </CONTENT>
    """)
    yield file


def test_extract():
    mock_output_handler = Mock()
    pipeline = Pipeline(CONTROL_DATA)
    pipeline.add_steps([
        ExtractStep('Extract text', 'files', source_iter=mock_file_iter, output_handler=mock_output_handler)
    ])
    pipeline.run()
    call_args = mock_output_handler.call_args[0]
    content = call_args[1]

    # noinspection SpellCheckingInspection
    assert content['metadata']['doc_type'] == 'CHANNEL_ANSWERFLOW_STEPS'
    assert content['data']['reference_table']['structured_content'][0]['type'] == 'heading'
    assert content['data']['reference_table']['structured_content'][0]['text'] == 'My Heading'
    assert content['data']['reference_table']['text'][1] == 'First line'
