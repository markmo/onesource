from copy import deepcopy
from datetime import datetime
from functional import file_iter, load_config, process_file_collect, process_file_extract
from io import BytesIO
import json
from logging import Logger
import os
from pipeline import get_temp_path, write_control_file_start
import ray
from typing import Any, Callable, Dict, List

CONFIG_FILE_PATH = '../config/config.yml'
STEP_EXTRACT = 'extract'
STEP_COLLECT = 'collect'


ray.init()


@ray.remote
def extract_file(filename: str,
                 data: bytes,
                 excluded_xml_tags: List[str],
                 excluded_html_tags: List[str],
                 log: List[str]
                 ) -> Dict[str, Any]:
    log.append('process file: {}'.format(filename))
    return process_file_extract(BytesIO(data), excluded_xml_tags, excluded_html_tags)


@ray.remote
def collect_data(filename: str,
                 input_doc: Dict[str, Any],
                 config: Dict[str, Any],
                 log: List[str]
                 ) -> Dict[str, Any]:
    log.append('collect data: {}'.format(filename))
    return process_file_collect(input_doc, config)


# I don't think queues are really needed. I believe the Ray scheduler
# takes care of allocating tasks to available workers.

def create_q() -> str:
    return ray.put([])


def enq(q_id, f):
    q = ray.get(q_id)
    q[:-1] = q[1:]
    q[-1] = f
    return ray.put(q)


def deq(q_id):
    q = ray.get(q_id)
    f = q[0]
    q[:-1] = q[1:-1]
    return f, ray.put(q)


def q_has_items(q_id):
    q = ray.get(q_id)
    return len(q)


def create_log() -> str:
    return ray.put([])


def print_log(log_id: str, logger: Logger) -> None:
    log = ray.get(log_id)
    for entry in log:
        logger.debug(entry)
    log.clear()


def create_excluded_tags(excluded_tags: List[str]) -> str:
    return ray.put(excluded_tags)


def start(control_data: Dict[str, Any],
          temp_path: str,
          output_handler: Callable[[str, Dict[str, Any]], None],
          logger: Logger
          ) -> Dict[str, Any]:
    file_count = 0
    files_processed = []
    files_output_extract = []
    files_output_collect = []
    write_root_dir = control_data['job']['write_root_dir']
    config = load_config(CONFIG_FILE_PATH)

    excluded_xml_tags_id = create_excluded_tags(['GUID'])
    excluded_html_tags_id = create_excluded_tags([])
    log_id = create_log()
    config_id = ray.put(config)

    steps_initialized = {}
    remaining_ids = []
    file_paths = [x['path'] for x in control_data['files']]
    for file, _ in file_iter(file_paths):
        # logger.debug('process file: {}'.format(file.name))
        extract_id = extract_file.remote(file.name, file.read(),
                                         excluded_xml_tags_id, excluded_html_tags_id,
                                         log_id)
        remaining_ids.append(extract_id)
        files_processed.append({
            'path': file.name,
            'time': datetime.utcnow().isoformat()
        })
        collect_id = collect_data.remote(file.name, extract_id, config_id, log_id)
        remaining_ids.append(collect_id)

    while remaining_ids:
        ready_ids, remaining_ids = ray.wait(remaining_ids)
        for accumulator_id in ready_ids:
            accumulator = ray.get(accumulator_id)
            step = accumulator['step']
            record_id = accumulator['metadata']['record_id']
            output_filename = '{}_{}.json'.format(step, record_id)
            output_path = os.path.join(write_root_dir, output_filename)
            if step not in steps_initialized:
                control_data = write_control_file_start(step, control_data, temp_path)
                steps_initialized[step] = True

            if step == STEP_EXTRACT:
                files_output_extract.append({
                    'filename': output_filename,
                    'path': output_path,
                    'status': 'processed',
                    'time': datetime.utcnow().isoformat()
                })

            elif step == STEP_COLLECT:
                files_output_collect.append({
                    'filename': output_filename,
                    'path': output_path,
                    'status': 'processed',
                    'time': datetime.utcnow().isoformat()
                })

            output_handler(output_path, accumulator)
            file_count += 1
            if file_count % 50 == 0:
                print_log(log_id, logger)
                write_control_file(control_data,
                                   files_processed, files_output_extract, files_output_collect,
                                   temp_path)

    print_log(log_id, logger)
    write_control_file(control_data,
                       files_processed, files_output_extract, files_output_collect,
                       temp_path, is_done=True)

    return {
        'file_count': file_count,
        'files_processed': files_processed,
        'files_output_extract': files_output_extract,
        'files_output_collect': files_output_collect
    }


def write_control_file(control_data: Dict[str, Any],
                       files_processed: List[Dict[str, Any]],
                       files_output_extract: List[Dict[str, Any]],
                       files_output_collect: List[Dict[str, Any]],
                       temp_path: str = None,
                       is_done: bool = False
                       ) -> None:
    if not control_data:
        return

    if not temp_path:
        temp_path = get_temp_path(control_data['job']['read_root_dir'])

    data = deepcopy(control_data)
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
    data[STEP_EXTRACT] = files_output_extract
    data[STEP_COLLECT] = files_output_collect
    if is_done:
        data['job']['status'] = 'processed'
        data['job']['end'] = datetime.utcnow().isoformat()

    with open(temp_path, 'w') as output_file:
        json.dump(data, output_file)
