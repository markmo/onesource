from prep_for_dr_qa import table_to_dataframe
import pytest
from table_util import DataType, get_only_member, infer_schema, infer_type, table_to_natural_text, uniqueness_ratio


@pytest.fixture
def sample_table():
    return {
        'head': [],
        'body': [
            ['Region', 'Sales', 'Date'],
            ['Melbourne', '100', '10/1/2019'],
            ['Sydney', '90', '9/1/2019'],
            ['Hobart', '110', '11/1/2019'],
            ['Adelaide', '110', '10/12/2018']
        ]
    }


def test_uniqueness_ratio():
    values = [1, 1, 1, 1, 5, 6, 7, 8, 9, 10]
    assert uniqueness_ratio(values) == 0.7


def test_infer_type():
    assert infer_type('hello')[1] == DataType.STRING
    assert infer_type('42')[1] == DataType.INT
    assert infer_type('0.42')[1] == DataType.FLOAT
    assert infer_type('12/12/2018')[1] == DataType.DATE
    assert infer_type('2019-01-01 12:30:00')[1] == DataType.DATE
    assert infer_type('true')[1] == DataType.BOOL
    assert infer_type('y')[1] == DataType.BOOL
    assert infer_type('')[1] == DataType.NULL


def test_get_only_member():
    data_types = {DataType.INT}
    assert get_only_member(data_types) == DataType.INT


def test_infer_schema(sample_table):
    df = table_to_dataframe(sample_table)
    schema = infer_schema(df)
    assert schema.has_header is True
    assert schema.data_types[0] == DataType.STRING
    assert schema.data_types[1] == DataType.INT
    assert schema.data_types[2] == DataType.DATE


def test_table_to_natural_text(sample_table):
    df = table_to_dataframe(sample_table)
    schema = infer_schema(df)
    text = table_to_natural_text(df, schema)
    print(text)
    assert text[0] == 'Today, the Sales of Melbourne is 100.'
    assert text[3] == 'On Dec 10, the Sales of Adelaide is 110.'
