from collect import CollectStep
from extract import ExtractStep
import logging
from logging import Logger
import ray
from typing import Any, Dict, TextIO


ray.init()

logger = logging.getLogger()

extract_step = ExtractStep('Extract text', 'files')
collect_step = CollectStep('Collect text')


@ray.remote
def extract(f: TextIO, c: Dict[str, Any], l: Logger, a: Dict[str, Any]):
    l.debug('processing file: {}'.format(f.name))
    extract_step.process_file(f, c, logger, a)


@ray.remote
def collect(f: TextIO, c: Dict[str, Any], l: Logger, a: Dict[str, Any]):
    l.debug('processing file: {}'.format(f.name))
    collect_step.process_file(f, c, logger, a, config)


