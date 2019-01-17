from collections import defaultdict
import dateutil
from enum import Enum
import humanize
import pandas as pd
from typing import Any, Dict, List, Set, Tuple, Union

TRUE_VALUES = {'true', 't', 'yes', 'y', 'on'}
FALSE_VALUES = {'false', 'f', 'no', 'n', 'off'}
NULL_VALUES = {'null', 'none', 'na', '_', '-'}


class DataType(Enum):
    BIT = 'bit'
    BOOL = 'bool'
    DATE = 'date'
    FLOAT = 'float'
    INT = 'int'
    NULL = 'null'
    STRING = 'string'


class VarType(Enum):
    BOOL = 'bool'
    CATEGORICAL = 'categorical'
    NA = 'na'
    NUMERIC = 'numeric'
    TEXT = 'text'
    TIME = 'time'


class Schema(object):

    columns: List[str] = []
    n_header_rows = 0
    has_header = False
    has_row_labels = False

    def __init__(self, data_types: Dict[str, DataType], var_types: Dict[str, VarType]):
        self.data_types = data_types
        self.var_types = var_types


def infer_schema(df: pd.DataFrame, n_header_rows: int = 0) -> Schema:
    has_header = False
    has_row_labels = False
    n_header_rows_ = 0
    if 0 < n_header_rows < len(df):
        schema = _infer_schema(df.iloc[n_header_rows:])
        has_header = True
        n_header_rows_ = n_header_rows
    elif len(df) > 1:
        first_row_schema = _infer_schema(df.iloc[[0]])
        first_row_type_set = set(first_row_schema.data_types.values())
        schema = _infer_schema(df.iloc[1:20])
        type_set = set(schema.data_types.values())
        if get_only_member(first_row_type_set) == DataType.STRING and len(type_set) > 1:
            has_header = True
            n_header_rows_ = 1
    else:
        schema = _infer_schema(df)

    if schema.var_types[df.columns[0]] == VarType.TEXT:
        has_row_labels = True

    schema.columns = df.columns
    schema.n_header_rows = n_header_rows_
    schema.has_header = has_header
    schema.has_row_labels = has_row_labels
    return schema


def get_only_member(data_types: Set[DataType]) -> Union[DataType, None]:
    if len(data_types) == 1:
        data_type, = data_types
        return data_type

    return None


def _infer_schema(sample: pd.DataFrame) -> Schema:
    value_types = defaultdict(set)
    for column in sample:
        for value in sample[column].values:
            value_types[column].add(infer_type(value)[1])

    data_types = {}
    for column in sample:
        types = value_types[column]
        non_null_types = types - {DataType.NULL}
        if len(non_null_types) > 1:  # the column has mixed types other than null
            if DataType.STRING in non_null_types:
                data_type = DataType.STRING
            elif DataType.DATE in non_null_types:
                data_type = DataType.STRING
            elif len(non_null_types - {DataType.BOOL, DataType.BIT}) == 0:
                data_type = DataType.BOOL
            elif DataType.BOOL in non_null_types:
                data_type = DataType.STRING
            elif DataType.FLOAT in non_null_types:
                data_type = DataType.FLOAT
            elif DataType.INT in non_null_types:
                data_type = DataType.INT
            else:
                raise ValueError('Invalid types: {}'.format(types))
        elif len(types) > 1:  # null is the only other mixed type
            data_type, = non_null_types
        else:  # the type is consistent
            if DataType.BIT in types:
                data_type = DataType.BOOL
            else:
                data_type, = types

        data_types[column] = data_type

    var_types = {}
    for column in sample:
        data_type = data_types[column]
        if data_type in {DataType.INT, DataType.STRING}:
            if uniqueness_ratio(sample[column].values) < 0.9:
                var_type = VarType.CATEGORICAL
            elif data_type == DataType.INT:
                var_type = VarType.NUMERIC
            else:
                var_type = VarType.TEXT
        elif data_type == DataType.BOOL:
            var_type = VarType.BOOL
        elif data_type == DataType.DATE:
            var_type = VarType.TIME
        elif data_type == DataType.FLOAT:
            var_type = VarType.NUMERIC
        elif data_type == DataType.NULL:
            var_type = VarType.NA
        else:
            raise ValueError('Invalid type: {}'.format(data_type))

        var_types[column] = var_type

    return Schema(data_types, var_types)


def infer_type(value: Any) -> Tuple[Any, DataType]:
    value = str(value)
    if len(value) == 0 or value.lower() in NULL_VALUES:
        return None, DataType.NULL

    if value.lower() in TRUE_VALUES:
        return True, DataType.BOOL

    if value.lower() in FALSE_VALUES:
        return False, DataType.BOOL

    try:
        val = float(value)
        if val == int(val):
            if val in {0, 1}:
                return int(val), DataType.BIT

            return int(val), DataType.INT

        return val, DataType.FLOAT
    except ValueError:
        pass

    try:
        val = dateutil.parser.parse(value, dayfirst=True, fuzzy=True)
        return val, DataType.DATE
    except ValueError:
        return value, DataType.STRING


def uniqueness_ratio(values: List[Any]) -> float:
    if len(values) == 0:
        return 0.

    return len(set(values)) / len(values)


def table_to_natural_text(df: pd.DataFrame, schema: Schema) -> List[str]:
    text = []
    if schema.has_header and schema.has_row_labels:
        time_columns = [col for col, typ in schema.data_types.items() if typ == DataType.DATE]
        if len(time_columns) == 1:
            time_column = time_columns[0]
            header = None
            for i, row in df.iterrows():
                if i == 0:
                    header = row
                    continue

                if i < schema.n_header_rows:
                    continue

                for col in schema.columns[1:]:
                    if col != time_column:
                        value = value_to_natural_text(row[col])
                        date, _ = infer_type(row[time_column])
                        natural_time = humanize.naturaldate(date)
                        if natural_time.isalpha():
                            natural_time = natural_time[0].upper() + natural_time[1:]
                        else:
                            natural_time = 'On ' + natural_time

                        text.append('{}, the {} of {} is {}.'.format(
                            natural_time,
                            label_to_natural_text(header[col]),
                            row[schema.columns[0]],
                            value))

        else:
            header = None
            for i, row in df.iterrows():
                if i == 0:
                    header = row
                    continue

                for col in schema.columns[1:]:
                    value = value_to_natural_text(row[col])
                    text.append('The {} of {} is {}.'.format(
                        label_to_natural_text(header[col]),
                        row[schema.columns[0]],
                        value))

    else:
        for _, row in df.iterrows():
            values = []
            for col in schema.columns:
                values.append(value_to_natural_text(row[col]))

            text.append(' '.join(values))

    return text


def label_to_natural_text(label: str) -> str:
    return label.replace('_', ' ')


def value_to_natural_text(value: str) -> str:
    text, data_type = infer_type(value)
    if data_type == DataType.INT:
        text = humanize.intword(text)
    elif data_type == DataType.FLOAT:
        text = '%.2f' % text
    elif data_type == DataType.BOOL:
        text = 'true' if text else 'false'
    elif data_type == DataType.DATE:
        text = humanize.naturaldate(text)
    elif data_type == DataType.NULL:
        text = 'NA'

    return str(text)
