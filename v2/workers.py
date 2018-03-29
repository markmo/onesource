from datetime import datetime
from functional import file_iter, process_file
from io import BytesIO
from logging import Logger
import os
import ray
from typing import Any, Callable, Dict, List


ray.init()


@ray.remote
def extract_file(filename: str,
                 data: bytes,
                 excluded_xml_tags: List[str],
                 excluded_html_tags: List[str],
                 log: List[str]
                 ) -> Dict[str, Any]:
    log.append('process file: {}'.format(filename))
    return process_file(BytesIO(data), excluded_xml_tags, excluded_html_tags)


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


def create_excluded_tags(excluded_tags: List[str]) -> str:
    return ray.put(excluded_tags)


def start(control_data: Dict[str, Any], step_name: str, output_handler: Callable, logger: Logger):
    file_count = 0
    files_processed = []
    files_output = []
    write_root_dir = control_data['job']['write_root_dir']

    excluded_xml_tags_id = create_excluded_tags(['GUID'])
    excluded_html_tags_id = create_excluded_tags([])
    log_id = create_log()

    remaining_ids = []
    file_paths = [x['path'] for x in control_data['files']]
    for file in file_iter(file_paths):
        logger.debug('process file: {}'.format(file.name))

        accumulator_id = extract_file.remote(file.name, file.read(),
                                             excluded_xml_tags_id, excluded_html_tags_id,
                                             log_id)
        remaining_ids.append(accumulator_id)
        files_processed.append({
            'path': file.name,
            'time': datetime.utcnow().isoformat()
        })

    while remaining_ids:
        ready_ids, remaining_ids = ray.wait(remaining_ids)
        for accumulator_id in ready_ids:
            accumulator = ray.get(accumulator_id)
            record_id = accumulator['metadata']['record_id']
            output_filename = '{}_{}.json'.format(step_name, record_id)
            output_path = os.path.join(write_root_dir, output_filename)
            files_output.append({
                'filename': output_filename,
                'path': output_path,
                'status': 'processed',
                'time': datetime.utcnow().isoformat()
            })
            output_handler(output_path, accumulator)
            file_count += 1

    print_log(log_id, logger)

    return {
        'file_count': file_count,
        'files_processed': files_processed,
        'files_output': files_output
    }
