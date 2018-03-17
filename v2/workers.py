from functools import partial
from logging import Logger
from pipeline import output_handler as oh
import ray
from typing import Any, Callable, Dict, List, TextIO
from v2_extract import extract_text


ray.init()


@ray.remote
def extract(c: Dict[str, Any], a: Dict[str, Any], f: TextIO) -> str:
    # l.debug('processing file: {}'.format(f.name))
    return extract_text(c, a, ['GUID'], oh, f)


# @ray.remote
# def collect(c: Dict[str, Any], l: Logger, a: Dict[str, Any], f: TextIO) -> str:
#     l.debug('processing file: {}'.format(f.name))
#     collect_step.process_file(c, l, a, f)


def file_iterator(file_paths: List[str]):
    for path in file_paths:
        with open(path, 'rb') as file:
            yield file


def process_file(path: str, processor: Callable) -> str:
    with open(path, 'rb') as file:
        out = processor(file)

    return out


def start(c: Dict[str, Any], l: Logger):
    files_processed = []
    files_output = []

    # working storage
    accumulator = {
        'files_processed': files_processed,
        'files_output': files_output
    }

    # collect_partial = partial(collect.remote, c, l, accumulator)
    file_paths = [x['path'] for x in c['files']]
    for f in file_iterator(file_paths):
        output_path = extract.remote(f, c, accumulator)
        # process_file(output_path, collect_partial)
