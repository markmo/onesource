from copy import copy, deepcopy
from datetime import datetime
import itertools
import re
import sys
from typing import Callable


def clean_text(text: str) -> str:
    """
    Remove non-ascii chars and extra whitespace.

    :param text: (str)
    :return: (str)
    """
    if not text:
        return ''

    # replace all contiguous whitespace with a single space
    t = re.sub(r'\s+', ' ', text)

    # trim whitespace at start and end
    t = t.strip()

    return t


def remove_bullet_markers(text: str) -> str:
    """
    Remove bullet chars.

    :param text: (str)
    :return: (str)
    """
    # replace bullet chars
    t = re.sub(r'^[•*o]\s', '', text)

    # replace non-ascii chars
    t = re.sub(r'[^\x00-\x7F]+', '', t)

    return t


def convert_name_to_underscore(name: str) -> str:
    """
    Convert name e.g. 'Extract Step 1' to underscore format e.g. 'extract_step_1'.

    :param name: str
    :return: str
    """
    t = name.strip()
    t = re.sub(r'\s+', '_', t)
    return t.lower()


def deep_update_(target, src, append_to_lists=False) -> None:
    for k, v in src.items():
        if type(v) == list:
            if k in target:
                if append_to_lists:
                    target[k].extend(v)
                else:
                    target[k] = copy(v)
            else:
                target[k] = deepcopy(v)
        elif type(v) == dict:
            if k in target:
                deep_update_(target[k], v)
            else:
                target[k] = deepcopy(v)
        elif type(v) == set:
            if k in target:
                target[k].update(v.copy())
            else:
                target[k] = v.copy()
        else:
            target[k] = copy(v)


start_tag_re = re.compile(r'^\s*<')

end_tag_re = re.compile(r'>\s*$')

variable_re = re.compile(r'^{[\w.]*}$')


def fix_content(content: str) -> str:
    """
    Wrap text in HTML tags if not already.

    :param content: possible HTML as str
    :return: HTML as str or empty str if no content
    """
    if not content:
        return ''

    if start_tag_re.match(content) and not end_tag_re.match(content):
        return '<div>{}</div>'.format(content)
    else:
        return content


def flatten(lst):
    return list(itertools.chain.from_iterable(lst))


def get_iso_datetime_from_millis(millis: int) -> str:
    """
    Convert milliseconds since epoch to ISO formatted datetime
    e.g. '2018-03-10T09:54:49.943163'

    :param millis: int
    :return: datetime
    """
    return datetime.utcfromtimestamp(millis//1000).isoformat()


def is_variable(text: str) -> bool:
    return variable_re.match(text) is not None


def strip_link_markers(text: str) -> str:
    return re.sub(r'(\[\[|]])', '', text)


class Accumulator(dict):

    def __getattr__(self, key):
        try:
            # Throws exception if not in prototype chain
            return object.__getattribute__(self, key)
        except AttributeError:
            try:
                return self[key]
            except KeyError:
                raise AttributeError(key)

    def __setattr__(self, key, value):
        try:
            object.__getattribute__(self, key)
        except AttributeError:
            try:
                self[key] = value
            except Exception:
                raise AttributeError(key)
        else:
            object.__setattr__(self, key, value)

    def __delattr__(self, key):
        try:
            object.__getattribute__(self, key)
        except AttributeError:
            try:
                del self[key]
            except KeyError:
                raise AttributeError(key)
        else:
            object.__delattr__(self, key)

    def to_dict(self):
        return convert_accumulator_to_dict(self)

    @property
    def __dict__(self):
        return self.to_dict()

    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__, dict.__repr__(self))

    def __dir__(self):
        return list(self.keys())

    @classmethod
    def from_dict(cls, d):
        return convert_dict_to_accumulator(d, cls)

    def copy(self):
        return type(self).from_dict(self)


class DefaultAccumulator(Accumulator):
    """
    An Accumulator which returns a user-specified value for missing keys.
    """
    def __init__(self, *args, **kwargs):
        """
        Construct a new DefaultAccumulator. Like collections.defaultdict,
        the first argument is the user-specified default value and
        subsequent arguments are the same as those for dict.

        :param args: default value
        :param kwargs: same as constructor arguments for dict
        """
        # Mimic collections.defaultdict constructor
        if args:
            default = args[0]
            args = args[1:]
        else:
            default = None

        super(DefaultAccumulator, self).__init__(*args, **kwargs)
        self.__default__ = default

    def __getattr__(self, key):
        """
        Gets value for key if exists, otherwise returns the default value.

        :param key: dict key
        :return:
        """
        try:
            super(DefaultAccumulator, self).__getattr__(key)
        except AttributeError:
            return self.__default__

    def __setattr__(self, key, value):
        if key == '__default__':
            object.__setattr__(self, key, value)
        else:
            return super(DefaultAccumulator, self).__setattr__(key, value)

    def __getitem__(self, key):
        """
        Gets value for key if exists, otherwise returns the default value.

        :param key: dict key
        :return:
        """
        try:
            return super(DefaultAccumulator, self).__getitem__(key)
        except KeyError:
            return self.__default__

    @classmethod
    def from_dict(cls, d, default=None):
        return convert_dict_to_accumulator(d, factory=lambda d_: cls(default, d_))

    def copy(self):
        return type(self).from_dict(self, default=self.__default__)

    def __repr__(self):
        return '{0}({1!r}, {2})'.format(type(self).__name__, self.__undefined__, dict.__repr__(self))


def convert_accumulator_to_dict(x):
    if isinstance(x, dict):
        return {k: convert_accumulator_to_dict(v) for k, v in x.items()}
    elif isinstance(x, (list, tuple)):
        return type(x)(convert_accumulator_to_dict(v) for v in x)
    else:
        return x


def convert_dict_to_accumulator(x, factory=Accumulator):
    if isinstance(x, dict):
        # noinspection PyArgumentList
        return factory((k, convert_dict_to_accumulator(v, factory)) for k, v in x.items())
    elif isinstance(x, (list, tuple)):
        return type(x)(convert_dict_to_accumulator(v, factory) for v in x)
    else:
        return x


# Hack to implement tail call optimization

class TailRecursionException(BaseException):

    def __init__(self, args, kwargs):
        self.args = args
        self.kwargs = kwargs


def tailrec(fn: Callable):
    """
    This function decorates a function with tail call optimization. It does
    this by throwing an exception if it is it's own grandparent, and catching
    such exceptions to fake the tail call optimization. For example::

        @tailrec
            def factorial(n, acc=1):
                if n == 0:
                    return acc
                return factorial(n - 1, n * acc)

    This function fails if the decorated function recurses in a non-tail
    context.
    """

    def func(*args, **kwargs):
        # noinspection PyProtectedMember
        f = sys._getframe()
        if f.f_back and f.f_back.f_back and f.f_back.f_back.f_code == f.f_code:
            raise TailRecursionException(args, kwargs)
        else:
            while True:
                try:
                    return fn(*args, **kwargs)
                except TailRecursionException as e:
                    args = e.args
                    kwargs = e.kwargs

    func.__doc__ = fn.__doc__
    return func


class MakeIter(object):

    def __init__(self, generator, *args, **kwargs):
        self.generator = generator
        self.args = args
        self.kwargs = kwargs

    def __call__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        return self

    def __iter__(self):
        return self.generator(*self.args, **self.kwargs)
