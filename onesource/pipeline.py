from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime
import json
import logging
from logging import Logger
from multiprocessing import Process
import os
import re
import psycopg2
from py2neo import Database, Graph
from py2neo.data import Node, Relationship
from py2neo.ogm import GraphObject, Property
import tempfile
import traceback
from typing import Any, AnyStr, Dict, IO, Iterator, List, Tuple
from utils import convert_name_to_underscore
import uuid

DB_NAME = 'postgres'
DB_USER = 'd777710'
HIDDEN_FILE_PREFIXES = ('~', '.')


class AbstractStep(object):
    """
    Interface for step types to implement.
    """

    def __init__(self, name: str, source_key: str = None, overwrite: bool = False):
        """

        :param name: human-readable name of step
        :param source_key: `control_data` key for source list
        :param overwrite: overwrite files flag
        """
        self.name = name
        self.__source_key = source_key
        self._overwrite = overwrite

    def process_file(self,
                     file: IO[AnyStr],
                     path: str,
                     control_data: Dict[str, Any],
                     logger: Logger,
                     accumulator: Dict[str, Any]
                     ) -> str:
        raise NotImplementedError

    @property
    def source_key(self) -> str:
        return self.__source_key

    @source_key.setter
    def source_key(self, source_key: str) -> None:
        """

        :param source_key: `control_data` key for source list
        :return: None
        """
        self.__source_key = source_key

    def run(self, control_data: Dict[str, Any], logger: Logger, accumulator: Dict[str, Any]) -> None:
        """
        Must be overridden.

        :param control_data: data loaded from control file
        :param logger: Logger
        :param accumulator: working storage for job control or to accumulate output data
        :return: None
        """
        raise NotImplementedError


# class Container(object):
#
#     def __init__(self):
#         self.__steps = []
#
#     def __call__(self, steps: List[AbstractStep]):
#         self.add_steps(steps)
#         return self
#
#     def add_step(self, step: AbstractStep):
#         if not step:
#             return self
#
#         # set name of previous step as source key for this step
#         if self.__steps and not step.source_key:
#             step.source_key = convert_name_to_underscore(self.__steps[-1].name)
#
#         self.__steps.append(step)
#         return self
#
#     def add_steps(self, steps: List[AbstractStep]):
#         for step in steps:
#             self.add_step(step)
#
#         return self


class InvalidStateException(Exception):
    pass


class Parallel(AbstractStep):

    def __init__(self, name: str = None, source_key: str = None, temp_path: str = None):
        if not name:
            name = 'Parallel execution'

        super().__init__(name, source_key)
        self.__temp_path = temp_path
        self.__uninitialized_steps: List[AbstractStep] = []
        self.__initialized_steps: List[AbstractStep] = []

    def __call__(self, steps: List[AbstractStep]):
        self.add_steps(steps)
        return self

    @property
    def steps(self):
        if not self.__initialized_steps:
            self.__initialize_steps()

        return self.__initialized_steps

    @property
    def temp_path(self) -> str:
        return self.__temp_path

    @temp_path.setter
    def temp_path(self, temp_path: str) -> None:
        self.__temp_path = temp_path

    def add_step(self, step: AbstractStep):
        if step:
            self.__uninitialized_steps.append(step)

        return self

    def add_steps(self, steps: List[AbstractStep]):
        for step in steps:
            self.add_step(step)

        return self

    def process_file(self,
                     file: IO[AnyStr],
                     path: str,
                     control_data: Dict[str, Any],
                     logger: Logger,
                     accumulator: Dict[str, Any]
                     ) -> str:
        raise NotImplementedError

    def run(self, control_data: Dict[str, Any], logger: Logger, accumulator: Dict[str, Any]) -> None:
        if not self.__temp_path:
            raise InvalidStateException('temp_path not set')

        # for debugging purposes
        # for step in self.steps:
        #     control_data = run_step(step, control_data, logger, accumulator, self.__temp_path)

        def __run(s):
            nonlocal control_data
            try:
                control_data = run_step(s, control_data, logger, accumulator, self.__temp_path)
            except Exception as e:
                logger.error(e)
                traceback.print_exc()
                raise e

        for step in self.steps:
            __run(step)
        # processes = [Process(target=__run, args=step) for step in self.steps]
        # processes = [Process(target=run_step, args=(step, control_data, logger, accumulator, self.temp_path))
        #              for step in self.steps]
        # [process.start() for process in processes]
        # [process.join() for process in processes]  # wait for all parallel steps to finish

    def __initialize_steps(self):
        for step in self.__uninitialized_steps:
            if not step.source_key:
                if self.__initialized_steps:
                    # set name of previous step as source key for this step
                    step.source_key = convert_name_to_underscore(self.__initialized_steps[-1].name)
                else:
                    # otherwise use the source key of the parent, which is
                    # from the step before the parent
                    step.source_key = self.source_key

            self.__initialized_steps.append(step)


class Pipeline(object):
    """
    Executes a sequence of steps in a job.
    """

    def __init__(self,
                 control_data: Dict[str, Any],
                 logger: Logger = logging.getLogger(),
                 temp_path: str = None,
                 overwrite: bool = False):
        """

        :param control_data: data loaded from control file, passed to each step
        :param logger: Logger
        :param temp_path: path to directory to write control files
        :param overwrite: overwrite files flag
        """
        self.__control_data = control_data
        self.__logger = logger
        self.__temp_path = temp_path
        self.__overwrite = overwrite
        self.__steps: List[AbstractStep] = []

    def __call__(self, steps: List[AbstractStep]):
        self.add_steps(steps)
        return self

    def add_step(self, step: AbstractStep):
        if not step:
            return self

        # set name of previous step as source key for this step
        if self.__steps and not step.source_key:
            step.source_key = convert_name_to_underscore(self.__steps[-1].name)

        self.__steps.append(step)
        return self

    def add_steps(self, steps: List[AbstractStep]):
        for step in steps:
            self.add_step(step)

        return self

    def run(self) -> None:
        control_data = self.__control_data
        logger = self.__logger
        temp_path = self.__temp_path

        # noinspection PyBroadException
        try:
            for step in self.__steps:
                files_processed = []
                files_output = []

                # working storage
                accumulator = {
                    'files_processed': files_processed,
                    'files_output': files_output
                }

                if isinstance(step, Parallel):
                    if not step.temp_path:
                        step.temp_path = temp_path

                    step.run(control_data, logger, accumulator)

                else:
                    # as control_data is mutated within the context manager's scope,
                    # it must be returned or else changes to it is lost
                    control_data = run_step(step, control_data, logger, accumulator, temp_path)

            write_control_file_end(control_data, temp_path)

        except Exception as e:
            # logger.error(e)
            # traceback.print_exc()

            write_control_file_end(control_data, temp_path, e)
            # we've already logged so exit gracefully
            pass


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


def file_iter(file_paths: List[str]) -> Iterator[Tuple[IO[AnyStr], str]]:
    for path in file_paths:
        with open(path, 'rb') as file:
            yield file, path


def json_output_handler(output_path: str, content: Dict[str, Any]) -> None:
    """
    Write output from step

    :param output_path: path to output file
    :param content: JSON content
    :return:
    """
    # Adopting the "easier to ask for forgiveness than permission" (EAFP) style
    # preferred in Python to avoid potential race condition between checking
    # existence of dir and making it.
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as output_file:
        json.dump(content, output_file)


def text_output_handler(output_path: str, text: List[str], overwrite: bool = False) -> None:
    """
    Write text output from step.

    :param output_path: path to output file
    :param text: lines
    :param overwrite: (bool) overwrite file contents if true otherwise append to file
    :return:
    """
    if os.path.exists(output_path):
        # overwrite else append
        mode = 'w' if overwrite else 'a'
        with open(output_path, mode) as output_file:
            if not overwrite:
                output_file.write('\n')

            output_file.write('\n'.join(text))  # writelines doesn't write newlines (wtf)
    else:
        # create and write
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as output_file:
            output_file.write('\n'.join(text))


def database_output_handler(output_path: str, content: Dict[str, Any], overwrite: bool = False) -> None:
    """
    Write text output from step.

    :param output_path: path to output file
    :param content: JSON content
    :param overwrite: (bool) overwrite file contents if true otherwise append to file
    :return:
    """
    conn = psycopg2.connect('dbname={} user={}'.format(DB_NAME, DB_USER))
    cur = conn.cursor()
    data = content['structured_content']
    prev_item = None
    prev_heading = None
    links = {}
    for item in data:
        node_type = item['type']
        if node_type in ['text', 'heading', 'link']:
            node_text = item['text']
            node_id = insert_node(cur, node_text, node_type, item.get('url', None), item.get('is_question', None))
            if prev_item:
                insert_edge(cur, prev_item['node_id'], node_id, 'before')
                insert_edge(cur, node_id, prev_item['node_id'], 'after')

            prev_item = dict(item, node_id=node_id)
            if node_type == 'heading':
                prev_heading = prev_item
            elif prev_heading:
                insert_edge(cur, node_id, prev_heading['node_id'], 'has_heading')

            if node_type == 'link':
                links[node_text] = item
            else:
                found_links = re.findall(r'\[\[(.*?)\]\]', node_text)
                for link in found_links:
                    if link[0] in links:
                        insert_edge(cur, node_id, links[link[0]]['node_id'], 'has_link')

        elif node_type == 'list':
            list_node_id = insert_node(cur, None, 'list', None, None)
            if prev_item:
                insert_edge(cur, prev_item['node_id'], list_node_id, 'before')
                insert_edge(cur, list_node_id, prev_item['node_id'], 'after')

            if prev_heading:
                insert_edge(cur, list_node_id, prev_heading['node_id'], 'has_heading')

            prev_item = dict(item, node_id=list_node_id)

            prev_list_item = None
            for i, list_item in enumerate(item['items']):
                list_item_node_id = insert_node(cur, list_item, 'list_item', None, item.get('is_question', None), i)
                insert_edge(cur, list_item_node_id, list_node_id, 'item_of')
                if prev_list_item:
                    insert_edge(cur, prev_list_item['node_id'], list_item_node_id, 'before')
                    insert_edge(cur, list_item_node_id, prev_list_item['node_id'], 'after')

                prev_list_item = dict(node_id=list_item_node_id)
                found_links = re.findall(r'\[\[(.*?)\]\]', list_item)
                for link in found_links:
                    if link[0] in links:
                        insert_edge(cur, list_item_node_id, links[link[0]]['node_id'], 'has_link')

        elif node_type == 'table':
            table_node_id = insert_node(cur, None, 'table', None, None)
            if prev_item:
                insert_edge(cur, prev_item['node_id'], table_node_id, 'before')
                insert_edge(cur, table_node_id, prev_item['node_id'], 'after')

            if prev_heading:
                insert_edge(cur, table_node_id, prev_heading['node_id'], 'has_heading')

            prev_item = dict(item, node_id=table_node_id)

            prev_row = None
            prev_cell = None
            row_node_id = None
            for i, head_cell in enumerate(item['head']):
                if isinstance(head_cell, list):
                    row_node_id = insert_node(cur, None, 'table_head_row', None, None, i)
                    insert_edge(cur, row_node_id, table_node_id, 'head_row_of')
                    if prev_row:
                        insert_edge(cur, prev_row['node_id'], row_node_id, 'before')
                        insert_edge(cur, row_node_id, prev_row['node_id'], 'after')

                    prev_row = dict(node_id=row_node_id)

                    prev_cell = None
                    for j, cell in enumerate(head_cell):
                        cell_node_id = insert_node(cur, cell, 'table_head_cell', None, item.get('is_question', None), j)
                        insert_edge(cur, cell_node_id, row_node_id, 'cell_of')
                        if prev_cell:
                            insert_edge(cur, prev_cell['node_id'], cell_node_id, 'before')
                            insert_edge(cur, cell_node_id, prev_cell['node_id'], 'after')

                        prev_cell = dict(node_id=cell_node_id)

                else:
                    if not row_node_id:
                        row_node_id = insert_node(cur, None, 'table_head_row', None, None)
                        insert_edge(cur, row_node_id, table_node_id, 'head_row_of')
                        if prev_row:
                            insert_edge(cur, prev_row['node_id'], row_node_id, 'before')
                            insert_edge(cur, row_node_id, prev_row['node_id'], 'after')

                    cell_node_id = insert_node(cur, head_cell, 'table_head_cell', None, item.get('is_question', None),
                                               i)
                    insert_edge(cur, cell_node_id, row_node_id, 'cell_of')
                    if prev_cell:
                        insert_edge(cur, prev_cell['node_id'], cell_node_id, 'before')
                        insert_edge(cur, cell_node_id, prev_cell['node_id'], 'after')

                    prev_cell = dict(node_id=cell_node_id)

            prev_row = None
            for i, row in enumerate(item['body']):
                row_node_id = insert_node(cur, None, 'table_body_row', None, None, i)
                insert_edge(cur, row_node_id, table_node_id, 'body_row_of')
                if prev_row:
                    insert_edge(cur, prev_row['node_id'], row_node_id, 'before')
                    insert_edge(cur, row_node_id, prev_row['node_id'], 'after')

                prev_row = dict(node_id=row_node_id)

                prev_cell = None
                for j, cell in enumerate(row):
                    cell_node_id = insert_node(cur, cell, 'table_body_cell', None, item.get('is_question', None), j)
                    insert_edge(cur, cell_node_id, row_node_id, 'cell_of')
                    if prev_cell:
                        insert_edge(cur, prev_cell['node_id'], cell_node_id, 'before')
                        insert_edge(cur, cell_node_id, prev_cell['node_id'], 'after')

                    prev_cell = dict(node_id=cell_node_id)

    conn.commit()
    cur.close()
    conn.close()


class ContentNode(GraphObject):
    node_id = Property()
    node_type = Property()
    node_text = Property()
    url = Property()
    is_question = Property()
    seq_index = Property()


BEFORE = Relationship.type('before')
AFTER = Relationship.type('after')
HAS_HEADING = Relationship.type('has_heading')
HAS_LINK = Relationship.type('has_link')
ITEM_OF = Relationship.type('member_of')
CELL_OF = Relationship.type('cell_of')
HEAD_ROW_OF = Relationship.type('head_row_of')
BODY_ROW_OF = Relationship.type('body_row_of')


def neo4j_output_handler(output_path: str, content: Dict[str, Any], overwrite: bool = False) -> None:
    """
    Write text output from step.

    :param output_path: path to output file
    :param content: JSON content
    :param overwrite: (bool) overwrite file contents if true otherwise append to file
    :return:
    """
    graph = Graph('bolt://localhost:7687', auth=('neo4j', 'password'))
    data = content['structured_content']
    prev_item = None
    prev_heading = None
    links = {}
    for item in data:
        node_type = item['type']
        if node_type in ['text', 'heading', 'link']:
            node_text = item['text']
            node = insert_neo4j_node(graph, node_text, node_type, item.get('url', None), item.get('is_question', None))
            if prev_item:
                insert_neo4j_edge(graph, prev_item, node, 'before')
                insert_neo4j_edge(graph, node, prev_item, 'after')

            prev_item = node
            if node_type == 'heading':
                prev_heading = prev_item
            elif prev_heading:
                insert_neo4j_edge(graph, node, prev_heading, 'has_heading')

            if node_type == 'link':
                links[node_text] = item
            else:
                found_links = re.findall(r'\[\[(.*?)\]\]', node_text)
                for link in found_links:
                    if link[0] in links:
                        insert_neo4j_edge(graph, node, links[link[0]], 'has_link')

        elif node_type == 'list':
            list_node = insert_neo4j_node(graph, None, 'list', None, None)
            if prev_item:
                insert_neo4j_edge(graph, prev_item, list_node, 'before')
                insert_neo4j_edge(graph, list_node, prev_item, 'after')

            if prev_heading:
                insert_neo4j_edge(graph, list_node, prev_heading, 'has_heading')

            prev_item = list_node

            prev_list_item = None
            for i, list_item in enumerate(item['items']):
                list_item_node = insert_neo4j_node(graph, list_item, 'list_item', None, item.get('is_question', None), i)
                insert_neo4j_edge(graph, list_item_node, list_node, 'item_of')
                if prev_list_item:
                    insert_neo4j_edge(graph, prev_list_item, list_item_node, 'before')
                    insert_neo4j_edge(graph, list_item_node, prev_list_item, 'after')

                prev_list_item = list_item_node
                found_links = re.findall(r'\[\[(.*?)\]\]', list_item)
                for link in found_links:
                    if link[0] in links:
                        insert_neo4j_edge(graph, list_item_node, links[link[0]], 'has_link')

        elif node_type == 'table':
            table_node = insert_neo4j_node(graph, None, 'table', None, None)
            if prev_item:
                insert_neo4j_edge(graph, prev_item, table_node, 'before')
                insert_neo4j_edge(graph, table_node, prev_item, 'after')

            if prev_heading:
                insert_neo4j_edge(graph, table_node, prev_heading, 'has_heading')

            prev_item = table_node

            prev_row = None
            prev_cell = None
            row_node = None
            for i, head_cell in enumerate(item['head']):
                if isinstance(head_cell, list):
                    row_node = insert_neo4j_node(graph, None, 'table_head_row', None, None, i)
                    insert_neo4j_edge(graph, row_node, table_node, 'head_row_of')
                    if prev_row:
                        insert_neo4j_edge(graph, prev_row, row_node, 'before')
                        insert_neo4j_edge(graph, row_node, prev_row, 'after')

                    prev_row = row_node

                    prev_cell = None
                    for j, cell in enumerate(head_cell):
                        cell_node = insert_neo4j_node(graph, cell, 'table_head_cell', None, item.get('is_question', None), j)
                        insert_neo4j_edge(graph, cell_node, row_node, 'cell_of')
                        if prev_cell:
                            insert_neo4j_edge(graph, prev_cell, cell_node, 'before')
                            insert_neo4j_edge(graph, cell_node, prev_cell, 'after')

                        prev_cell = cell_node

                else:
                    if not row_node:
                        row_node = insert_neo4j_node(graph, None, 'table_head_row', None, None)
                        insert_neo4j_edge(graph, row_node, table_node, 'head_row_of')
                        if prev_row:
                            insert_neo4j_edge(graph, prev_row, row_node, 'before')
                            insert_neo4j_edge(graph, row_node, prev_row, 'after')

                    cell_node = insert_neo4j_node(graph, head_cell, 'table_head_cell', None, item.get('is_question', None), i)
                    insert_neo4j_edge(graph, cell_node, row_node, 'cell_of')
                    if prev_cell:
                        insert_neo4j_edge(graph, prev_cell, cell_node, 'before')
                        insert_neo4j_edge(graph, cell_node, prev_cell, 'after')

                    prev_cell = cell_node

            prev_row = None
            for i, row in enumerate(item['body']):
                row_node = insert_neo4j_node(graph, None, 'table_body_row', None, None, i)
                insert_neo4j_edge(graph, row_node, table_node, 'body_row_of')
                if prev_row:
                    insert_neo4j_edge(graph, prev_row, row_node, 'before')
                    insert_neo4j_edge(graph, row_node, prev_row, 'after')

                prev_row = row_node

                prev_cell = None
                for j, cell in enumerate(row):
                    cell_node = insert_neo4j_node(graph, cell, 'table_body_cell', None, item.get('is_question', None), j)
                    insert_neo4j_edge(graph, cell_node, row_node, 'cell_of')
                    if prev_cell:
                        insert_neo4j_edge(graph, prev_cell, cell_node, 'before')
                        insert_neo4j_edge(graph, cell_node, prev_cell, 'after')

                    prev_cell = cell_node


def insert_node(cur, node_text, node_type, url, is_question, seq_index=0):
    node_id = str(uuid.uuid4())
    try:
        cur.execute('INSERT INTO content_node(node_id, node_text, node_type, url, is_question, seq_index) '
                    'VALUES (%s, %s, %s, %s, %s, %s)', (node_id, node_text, node_type, url, is_question, seq_index))
    except Exception as e:
        logging.error('DB err: {}'.format(e))
        logging.error('node_id={}, node_text={}, node_type={}, url={}, is_question={}, seq_index={}'
                      .format(node_id, node_text, node_type, url, is_question, seq_index))
        raise e

    return node_id


def insert_edge(cur, node_id1, node_id2, edge_type):
    try:
        cur.execute('INSERT INTO content_edge(node_id1, node_id2, edge_type) VALUES (%s, %s, %s)',
                    (node_id1, node_id2, edge_type))
    except Exception as e:
        logging.error('DB err: {}'.format(e))
        logging.error('node_id1={}, node_id2={}, edge_type={}'.format(node_id1, node_id2, edge_type))
        raise e


def insert_neo4j_node(graph, node_text, node_type, url, is_question, seq_index=0):
    node_id = str(uuid.uuid4())
    try:
        node = Node(node_id,
                    node_type=node_type,
                    node_text=node_text,
                    url=url,
                    is_question=is_question,
                    seq_index=seq_index)
        graph.create(node)
    except Exception as e:
        logging.error('Neo4J err: {}'.format(e))
        logging.error('node_id={}, node_text={}, node_type={}, url={}, is_question={}, seq_index={}'
                      .format(node_id, node_text, node_type, url, is_question, seq_index))
        raise e

    return node


def insert_neo4j_edge(graph, node1, node2, edge_type):
    try:
        rel = Relationship(node1, edge_type, node2)
        graph.create(rel)
    except Exception as e:
        logging.error('Neo4J err: {}'.format(e))
        logging.error('node_id1={}, node_id2={}, edge_type={}'.format(node1, node2, edge_type))
        raise e

    return rel


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


def run_step(step: AbstractStep,
             control_data: Dict[str, Any],
             logger: Logger,
             accumulator: Dict[str, Any],
             temp_path: str
             ) -> Dict[str, Any]:
    step_name = convert_name_to_underscore(step.name)
    control_data = write_control_file_start(step_name, control_data, temp_path)

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

    return control_data


def write_control_file_start(step_name: str,
                             control_data: Dict[str, Any],
                             temp_path: str = None
                             ) -> Dict[str, Any]:
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
                       temp_path: str = None
                       ) -> Dict[str, Any]:
    """
    Write job control data at current moment to a file at `temp_path`.

    :param step_name: name of pipeline step in underscore format
    :param control_data: data loaded from current control file
    :param accumulator: working storage for job control or to accumulate output data
    :param temp_path: output location of control file
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

    with open(temp_path, 'w') as output_file:
        json.dump(data, output_file)

    return data


def write_control_file_end(control_data: Dict[str, Any],
                           temp_path: str = None,
                           error: Exception = None
                           ) -> Dict[str, Any]:
    """
    Write job control data at end of job.

    :param control_data: data loaded from current control file
    :param temp_path: output location of control file
    :param error: optional exception object
    :return: updated control data
    """
    if not control_data:
        return {}

    if not temp_path:
        temp_path = get_temp_path(control_data['job']['read_root_dir'])

    data = deepcopy(control_data)
    data['job']['end'] = datetime.utcnow().isoformat()
    if error:
        data['job']['status'] = 'error'
        data['job']['message'] = repr(error)
    else:
        data['job']['status'] = 'processed'

    with open(temp_path, 'w') as output_file:
        json.dump(data, output_file)

    return data
