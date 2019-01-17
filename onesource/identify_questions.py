from datetime import datetime
import json
from logging import Logger
import numpy as np
import os
from pipeline import AbstractStep, file_iter, json_output_handler as oh
import tensorflow as tf
from tensorflow.contrib import learn
from typing import Any, AnyStr, Callable, Dict, Iterator, IO, List
from utils import convert_name_to_underscore
import yaml

dir_path = os.path.dirname(os.path.realpath(__file__))
CONFIG_FILE_PATH = os.path.join(dir_path, '../config/config.yml')
MODEL_CHECKPOINT_PATH = ('/Users/d777710/src/DeepLearning/dltemplate/src/tf_model/'
                         'question_detector/runs/1544951256/checkpoints')


class IdentifyQuestionsStep(AbstractStep):
    """
    Identify questions in text.
    """

    def __init__(self,
                 name: str,
                 source_key: str = None,
                 overwrite: bool = False,
                 source_iter: Callable[[List[str]], Iterator[IO[AnyStr]]] = file_iter,
                 output_handler: Callable[[str, Dict[str, Any]], None] = oh):
        super().__init__(name, source_key, overwrite)
        self.__source_iter = source_iter
        self.__output_handler = output_handler
        vocab_path = os.path.join(MODEL_CHECKPOINT_PATH, '..', 'vocab')
        self.vocab_processor = learn.preprocessing.VocabularyProcessor.restore(vocab_path)
        checkpoint_file = tf.train.latest_checkpoint(MODEL_CHECKPOINT_PATH)
        graph = tf.Graph()
        with graph.as_default():
            session_conf = tf.ConfigProto(allow_soft_placement=True, log_device_placement=False)
            self.sess = tf.Session(config=session_conf)
            with self.sess.as_default():
                # Load the saved meta graph and restore variables
                saver = tf.train.import_meta_graph('{}.meta'.format(checkpoint_file))
                saver.restore(self.sess, checkpoint_file)

                # Get the placeholders from the graph by name
                self.input_x = graph.get_operation_by_name('input_x').outputs[0]
                self.keep_prob = graph.get_operation_by_name('keep_prob').outputs[0]

                # Tensors we want to evaluate
                self.preds = graph.get_operation_by_name('output/predictions').outputs[0]

    def predict_question(self, text):
        x = np.array(list(self.vocab_processor.transform([text])))
        preds = self.sess.run(self.preds, {self.input_x: x, self.keep_prob: 1.0})
        return int(preds[0]) == 1

    def process_file(self,
                     file: IO[AnyStr],
                     path: str,
                     control_data: Dict[str, Any],
                     logger: Logger,
                     accumulator: Dict[str, Any]
                     ) -> str:
        logger.debug('process file: {}'.format(file.name))
        input_doc = json.load(file)
        metadata = input_doc['metadata']
        record_id = metadata['record_id']
        data = input_doc['data']
        if 'structured_content' in data:
            for item in data['structured_content']:
                if 'text' in item:
                    is_question = self.predict_question(item['text'])
                    if is_question:
                        accumulator['found_questions'].append(item['text'])

                    item['is_question'] = is_question

        write_root_dir = control_data['job']['write_root_dir']
        step_name = convert_name_to_underscore(self.name)
        output_filename = '{}_{}.json'.format(step_name, record_id)
        output_path = os.path.join(write_root_dir, step_name, output_filename)
        update_control_info_(file.name, path, output_filename, output_path, accumulator)
        self.__output_handler(output_path, input_doc)
        return output_path

    def run(self, control_data: Dict[str, Any], logger: Logger, accumulator: Dict[str, Any]) -> None:
        file_paths = [x['path'] for x in control_data[self.source_key]]
        step_name = convert_name_to_underscore(self.name)
        processed_file_paths = {}
        if step_name in control_data:
            for x in control_data[step_name]:
                if x['status'] == 'processed':
                    processed_file_paths[x['input']] = x

        accumulator['found_questions'] = []
        for file, path in self.__source_iter(file_paths):
            if not self._overwrite and path in processed_file_paths.keys():
                accumulator['files_output'].append(processed_file_paths[path])
                continue

            self.process_file(file, path, control_data, logger, accumulator)

        np.savetxt('/tmp/found_questions.txt', accumulator['found_questions'], fmt='%s')
        del accumulator['found_questions']


def load_config() -> Dict[str, Any]:
    # get lists of data keys by `doc_type` to include in output
    with open(CONFIG_FILE_PATH, 'r') as f:
        config = yaml.load(f)

    return config


def update_control_info_(source_filename: str,
                         source_path: str,
                         output_filename: str,
                         output_path: str,
                         accumulator: Dict[str, Any]
                         ) -> None:
    now = datetime.utcnow().isoformat()
    accumulator['files_processed'].append({
        'path': source_filename,
        'time': now
    })
    accumulator['files_output'].append({
        'filename': output_filename,
        'input': source_path,
        'path': output_path,
        'status': 'processed',
        'time': now
    })
