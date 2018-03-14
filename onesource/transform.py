from datetime import datetime
import json
from logging import Logger
import os
from pipeline import AbstractStep, file_iter, output_handler as oh
import spacy
from spacy.matcher import Matcher
from spacy.attrs import TAG
from typing import Any, Dict, TextIO
from utils import convert_name_to_underscore


class TransformStep(AbstractStep):
    """
    Extract NLP features from collected text.
    """

    def __init__(self,
                 name: str,
                 source_key: str = None,
                 source_iter=file_iter,
                 output_handler=oh):
        super().__init__(name, source_key)
        self.__source_iter = source_iter
        self.__output_handler = output_handler

        # nlp = spacy.load('xx_ent_wiki_sm')
        nlp = spacy.load('en')
        matcher = Matcher(nlp.vocab)
        matcher.add('Q1', None, [
            {TAG: 'WDT'}, {TAG: 'NN', 'OP': '+'}, {TAG: 'VBZ'}, {TAG: 'JJ'},
            {TAG: 'IN'}, {TAG: 'DT'}, {TAG: 'PRP$'}, {TAG: 'NNP', 'OP': '+'},
            {TAG: '.'}
        ])
        matcher.add('Q2', None, [
            {TAG: 'WP'}, {TAG: 'VBP'}, {TAG: 'PRP'}, {TAG: 'VB'}, {TAG: 'TO'},
            {TAG: 'VB'}, {TAG: '.'}
        ])
        self.__nlp = nlp
        self.__matcher = matcher

    def process_file(self, f: TextIO, c: Dict[str, Any], logger: Logger, a: Dict[str, Any]):
        logger.debug('process file: {}'.format(f.name))
        matcher = self.__matcher
        input_doc = json.load(f)
        metadata = input_doc['metadata']
        record_id = metadata['record_id']
        text = input_doc['data']['text']
        sentences = []
        for t in text:
            doc = self.__nlp(t)
            entities = []
            annotated = []
            pos_tags = []
            for ent in doc.ents:
                entity = dict([
                    ('text', ent.text),
                    ('start_char', ent.start_char),
                    ('end_char', ent.end_char),
                    ('label', ent.label_),
                ])
                entities.append(entity)

            for token in doc:
                annotated.append(
                    dict([
                        ('text', token.text),
                        ('lemma', token.lemma_),
                        ('pos', token.pos_),
                        ('tag', token.tag_),
                        ('dep', token.dep_),
                        ('shape', token.shape_),
                        ('is_alpha', token.is_alpha),
                        ('is_stop', token.is_stop),
                    ])
                )
                pos_tags.append(token.tag_)

            # matches = matcher(doc)
            is_question = len(matcher(doc)) > 0

            sentence = dict([
                ('text', t),
                ('annotated', annotated),
                ('entities', entities),
                ('is_question', is_question),
                ('pos_tags', ' '.join(pos_tags))

            ])
            sentences.append(sentence)

        now = datetime.utcnow().isoformat()
        write_root_dir = c['job']['write_root_dir']
        output_filename = '{}_{}.json'.format(convert_name_to_underscore(self.name), record_id)
        output_path = os.path.join(write_root_dir, output_filename)
        content = {'metadata': metadata, 'data': {'sentences': sentences}}
        a['files_output'].append({
            'filename': output_filename,
            'path': output_path,
            'status': 'processed',
            'time': now
        })
        self.__output_handler(output_path, content)

    def run(self, c: Dict[str, Any], logger: Logger, a: Dict[str, Any]):
        file_paths = [x['path'] for x in c[self.source_key]]
        for f in self.__source_iter(file_paths):
            self.process_file(f, c, logger, a)
