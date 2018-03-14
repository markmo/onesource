from argparse import ArgumentParser
from collect import CollectStep
from datetime import datetime
from extract import ExtractStep
from extract_questions import ExtractQuestionsStep
import json
import logging
from logging import Logger
import os
from pipeline import Pipeline
import sys
import tempfile
from transform import TransformStep


def create_and_run_job(read_root_dir: str, write_root_dir: str, temp_dir: str, overwrite: bool, logger: Logger=None):
    if not os.path.exists(read_root_dir):
        sys.exit("read dir '{}' not found".format(read_root_dir))

    if not os.path.exists(write_root_dir):
        sys.exit("write dir '{}' not found".format(write_root_dir))

    if not os.path.exists(temp_dir):
        sys.exit("temp dir '{}' not found".format(temp_dir))

    if not logger:
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler(sys.stdout)
        # noinspection SpellCheckingInspection
        formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    files = []
    paths = []
    now = datetime.utcnow().isoformat()
    for root, _, fs in os.walk(read_root_dir):
        for filename in fs:
            file_path = os.path.join(root, filename)
            abspath = os.path.abspath(file_path)
            paths.append(abspath)
            files.append({
                'filename': filename,
                'path': abspath,
                'time': now,
                'status': 'started'
            })

    control_filename = os.path.abspath(read_root_dir).replace('/', '-')[1:]
    temp_path = os.path.join(temp_dir, control_filename + '.json')
    if not overwrite and os.path.isfile(temp_path):
        with open(temp_path, 'r') as control_file:
            control_data = json.load(control_file)
            control_paths = [x['path'] for x in control_data['files']]
            diff = set(control_paths).symmetric_difference(set(paths))
            if diff:
                sys.exit('control fileset differs from input fileset')

    else:
        control_data = {
            'job': {
                'start': now,
                'status': 'started',
                'read_root_dir': os.path.abspath(read_root_dir),
                'write_root_dir': os.path.abspath(write_root_dir)
            },
            'files': files
        }
        with open(temp_path, 'w') as output_file:
            json.dump(control_data, output_file)

    # setup pipeline
    pipe = Pipeline(control_data, logger, temp_path)
    pipe.add_steps([
        ExtractStep('Extract text', 'files'),
        CollectStep('Collect text'),
        # TransformStep('Transform text'),
        ExtractQuestionsStep('Extract questions')
    ])
    pipe.run()


if __name__ == "__main__":
    # read args
    parser = ArgumentParser(description='Text processing pipeline for OneSource')
    parser.add_argument('--read', dest='read_root_dir', help='read root dir')
    parser.add_argument('--write', dest='write_root_dir', help='write root dir')
    parser.add_argument('--temp', dest='temp_dir', help='temp dir', default=tempfile.gettempdir())
    parser.add_argument('--overwrite', dest='overwrite', type=bool, help='overwrite any processed files', default=False)
    args = parser.parse_args()

    create_and_run_job(args.read_root_dir, args.write_root_dir, args.temp_dir, args.overwrite)
