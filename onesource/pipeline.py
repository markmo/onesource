from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime
import json
import logging
from logging import Logger
import os
import tempfile
import traceback
from typing import Any, Dict, List, TextIO
from utils import convert_name_to_underscore


class AbstractStep(object):
    """
    Interface for step types to implement.
    """

    def __init__(self, name: str, source_key: str=None):
        """

        :param name: human-readable name of step
        :param source_key: `control_data` key for source list
        """
        self.name = name
        self.__source_key = source_key

    def process_file(self, c: Dict[str, Any], logger: Logger, a: Dict[str, Any], f: TextIO) -> str:
        raise NotImplementedError

    @property
    def source_key(self):
        return self.__source_key

    @source_key.setter
    def source_key(self, source_key: str):
        """

        :param source_key: `control_data` key for source list
        :return: None
        """
        self.__source_key = source_key

    def run(self, control_data: Dict[str, Any], logger: Logger, accumulator: Dict[str, Any]):
        """
        Must be overridden.

        :param control_data: data loaded from control file
        :param logger: Logger
        :param accumulator: working storage for job control or to accumulate output data
        :return: None
        """
        raise NotImplementedError


class Pipeline(object):
    """
    Executes a sequence of steps in a job.
    """

    def __init__(self,
                 control_data: Dict[str, Any],
                 logger: Logger=logging.getLogger(),
                 temp_path: str=None):
        """

        :param control_data: data loaded from control file, passed to each step
        :param logger: Logger
        :param temp_path: path to directory to write control files
        """
        self.__control_data = control_data
        self.__logger = logger
        self.__steps: List[AbstractStep] = []
        self.__temp_path = temp_path

    def add_step(self, step: AbstractStep) -> 'Pipeline':
        # set name of previous step as source key for this step
        if self.__steps and not step.source_key:
            step.source_key = convert_name_to_underscore(self.__steps[-1].name)

        self.__steps.append(step)
        return self

    def add_steps(self, steps: List[AbstractStep]) -> 'Pipeline':
        for step in steps:
            self.add_step(step)

        return self

    def run(self):
        control_data = self.__control_data
        logger = self.__logger
        temp_path = self.__temp_path

        # noinspection PyBroadException
        try:
            for step in self.__steps:
                step_name = convert_name_to_underscore(step.name)
                files_processed = []
                files_output = []

                # working storage
                accumulator = {
                    'files_processed': files_processed,
                    'files_output': files_output
                }

                control_data = write_control_file_start_step(step_name, control_data, temp_path)

                logged = logged_decorator(logger, step.name)
                tracked = tracked_decorator(step_name, control_data, accumulator, temp_path)

                with logged, tracked:
                    # noinspection PyBroadException
                    try:
                        step.run(control_data, logger, accumulator)
                    except Exception as e:
                        # logger.error(e)
                        # traceback.print_exc()

                        # allow decorators to handle, same effect as `break`
                        raise e

                write_control_file(step_name, control_data, accumulator, temp_path, is_done=True)

        except Exception:
            # logger.error(e)
            # traceback.print_exc()

            # we've already logged so exit gracefully
            pass


def file_iter(file_paths: List[str]):
    for path in file_paths:
        with open(path, 'rb') as file:
            yield file


# Using context managers to implement orthogonal concerns of running a step
# e.g. logging step execution, and tracking progress - a kind of aspect-
# oriented programming (https://en.wikipedia.org/wiki/Aspect-oriented_programming)


@contextmanager
def logged_decorator(logger: Logger, step_name: str):
    """
    Log execution of a pipeline step

    :param logger: Logger
    :param step_name: pipeline step name
    :return:
    """
    logger.debug('Start %s', step_name)
    try:
        yield
    except Exception as e:
        logger.error('Abnormal end %s: %s', step_name, e)
        traceback.print_exc()
        raise e

    logger.debug('End %s', step_name)


@contextmanager
def tracked_decorator(step_name: str,
                      control_data: Dict[str, Any],
                      accumulator: Dict[str, Any],
                      temp_path: str):
    """

    :param step_name: name of pipeline step in underscore format
    :param control_data: data loaded from the control file
    :param accumulator: working storage for job control or to accumulate output data
    :param temp_path: path to temp dir
    :return:
    """
    try:
        yield
    finally:
        write_control_file(step_name, control_data, accumulator, temp_path)


def output_handler(output_path: str, content: Dict[str, Any]) -> None:
    """
    Write output from step

    :param output_path: path to output file
    :param content: JSON content
    :return:
    """
    with open(output_path, 'w') as output_file:
        json.dump(content, output_file)


def get_temp_path(read_root_dir: str) -> str:
    """
    Derive path of control file from location of input files.
    Each control file corresponds to a unique input location.

    :param read_root_dir: location of input files
    :return: location of control file
    """
    control_filename = os.path.abspath(read_root_dir).replace('/', '-')[1:]
    temp_dir = tempfile.gettempdir()
    return os.path.join(temp_dir, control_filename + '.json')


def write_control_file_start_step(step_name: str, control_data: Dict[str, Any], temp_path: str=None) -> Dict[str, Any]:
    """
    Update control file for start of step.

    :param step_name: name of pipeline step in underscore format
    :param control_data: data loaded from current control file
    :param temp_path: output location of control file
    :return: updated control data
    """
    if not control_data:
        return {}

    if not temp_path:
        temp_path = get_temp_path(control_data['job']['read_root_dir'])

    # `control_data` is immutable
    data = deepcopy(control_data)
    if 'steps' not in data['job']:
        data['job']['steps'] = []

    if step_name not in data['job']['steps']:
        data['job']['steps'].append(step_name)

    with open(temp_path, 'w') as output_file:
        json.dump(data, output_file)

    return data


def write_control_file(step_name: str,
                       control_data: Dict[str, Any],
                       accumulator: Dict[str, Any],
                       temp_path: str=None,
                       is_done: bool=False) -> Dict[str, Any]:
    """
    Write job control data at current moment to a file at `temp_path`.

    :param step_name: name of pipeline step in underscore format
    :param control_data: data loaded from current control file
    :param accumulator: working storage for job control or to accumulate output data
    :param temp_path: output location of control file
    :param is_done: pipeline step is complete
    :return: updated control data
    """
    if not control_data:
        return {}

    if not temp_path:
        temp_path = get_temp_path(control_data['job']['read_root_dir'])

    files_processed = accumulator['files_processed']
    files_output = accumulator['files_output']

    # TODO
    # have to mutate as called within contextmanager
    # data = deepcopy(control_data)
    data = control_data

    processed = {x['path']: x['time'] for x in files_processed}
    files = []
    for file in control_data['files']:
        path = file['path']
        if path in processed:
            time = processed[path]
            file['status'] = 'processed'
            file['time'] = time

        files.append(file)

    data['files'] = files
    data[step_name] = files_output
    if is_done:
        data['job']['status'] = 'processed'
        data['job']['end'] = datetime.utcnow().isoformat()

    with open(temp_path, 'w') as output_file:
        json.dump(data, output_file)

    return data
