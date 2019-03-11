import csv
from datetime import datetime
from dateutil.parser import parse
from duckling import DucklingWrapper
from flair.data import Sentence
from flair.models import SequenceTagger
from flashtext import KeywordProcessor
import json
from logging import Logger
import os
from pathlib import Path
from pipeline import AbstractStep, file_iter, json_output_handler as oh
import re
from typing import Any, AnyStr, Callable, Dict, Iterator, IO, List, Tuple
from utils import convert_name_to_underscore

ENTITY_DATE = 'date'
ENTITY_NUMBER = 'number'
ENTITY_PERSON = 'person'
ENABLED_SYSTEM_ENTITIES = {ENTITY_DATE, ENTITY_NUMBER, ENTITY_PERSON}


class ExtractEntitiesStep(AbstractStep):
    """
    Extract entities from collected text.
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
        root_path = Path(__file__).parent.parent
        entities_path = str(root_path / 'config/entities.csv')
        self.entity_reverse_lookup, synonyms, self.regexprs = load_entities(entities_path)
        self.keyword_processor = prepare_keyword_processor(synonyms)
        duckling_entities = {ENTITY_DATE, ENTITY_NUMBER}
        tagger_entities = {ENTITY_PERSON}
        if len(duckling_entities.intersection(ENABLED_SYSTEM_ENTITIES)) > 0:
            self.d = DucklingWrapper()

        if len(tagger_entities.intersection(ENABLED_SYSTEM_ENTITIES)) > 0:
            self.tagger = SequenceTagger.load('ner')

    def process_file(self,
                     file: IO[AnyStr],
                     path: str,
                     control_data: Dict[str, Any],
                     logger: Logger,
                     accumulator: Dict[str, Any]
                     ) -> None:
        logger.debug('process file: {}'.format(file.name))
        input_doc = json.load(file)
        metadata = input_doc['metadata']
        record_id = metadata['record_id']
        data = input_doc['data']
        text = data['text']
        nlp_text = []
        for t in text:
            entities = []
            keywords_found = self.keyword_processor.extract_keywords(t, span_info=True)
            for keyword in keywords_found:
                entities.append({
                    'entity': self.entity_reverse_lookup[keyword[0]],
                    'location': keyword[1:],
                    'value': keyword[0],
                    'confidence': 1.0
                })

            matches = match_regexprs(t, self.regexprs)
            for match in matches:
                match['entity'] = self.entity_reverse_lookup[match['value']]

            entities.extend(matches)
            entities.extend(self.match_system_entities(t))

            # is the span of an entity contained within the span
            # of another entity
            def is_contained(entity):
                start, end = entity['location']
                for ent in entities:
                    s, e = ent['location']
                    # exclude exact span matches
                    if (start == s and end < e) or (start > s and end == e) or (start > s and end < e):
                        return True

                return False

            def is_valid(entity):
                # remove spurious dates
                if entity['entity'] == 'sys-date':
                    start, end = entity['location']
                    if (end - start) < 8:
                        return False

                    value = entity['value']
                    if isinstance(value, str):
                        try:
                            date = parse(value)
                        except ValueError:
                            return False

                        year = date.year
                        if year < 1990 or year > 2025:
                            return False

                return True

            # keep the entity with the longest span where an entity
            # is contained within the span of another
            pruned_entities = [ent for ent in entities if not is_contained(ent) and is_valid(ent)]
            nlp_text.append({
                'text': t,
                'entities': pruned_entities
            })

        now = datetime.utcnow().isoformat()
        write_root_dir = control_data['job']['write_root_dir']
        step_name = convert_name_to_underscore(self.name)
        output_filename = '{}_{}.json'.format(step_name, record_id)
        output_path = os.path.join(write_root_dir, step_name, output_filename)
        data = {}
        data['nlp_text'] = nlp_text
        content = {'metadata': metadata, 'data': data}
        accumulator['files_output'].append({
            'filename': output_filename,
            'input': path,
            'path': output_path,
            'status': 'processed',
            'time': now
        })
        self.__output_handler(output_path, content)

    def run(self, control_data: Dict[str, Any], logger: Logger, accumulator: Dict[str, Any]) -> None:
        file_paths = [x['path'] for x in control_data[self.source_key]]
        step_name = convert_name_to_underscore(self.name)
        processed_file_paths = {}
        if step_name in control_data:
            for x in control_data[step_name]:
                if x['status'] == 'processed':
                    processed_file_paths[x['input']] = x

        for file, path in self.__source_iter(file_paths):
            if not self._overwrite and path in processed_file_paths.keys():
                accumulator['files_output'].append(processed_file_paths[path])
                continue

            self.process_file(file, path, control_data, logger, accumulator)

    def match_system_entities(self, utter):
        matches = []
        if ENTITY_DATE in ENABLED_SYSTEM_ENTITIES:
            results = self.d.parse_time(utter)
            for result in results:
                matches.append({
                    'entity': 'sys-date',
                    'location': [result['start'], result['end']],
                    'value': result['value']['value'],
                    'confidence': 1.0
                })

        if ENTITY_NUMBER in ENABLED_SYSTEM_ENTITIES:
            results = self.d.parse_number(utter)
            for result in results:
                matches.append({
                    'entity': 'sys-number',
                    'location': [result['start'], result['end']],
                    'value': result['value']['value'],
                    'confidence': 1.0
                })

        sentence = None

        if ENTITY_PERSON in ENABLED_SYSTEM_ENTITIES:
            if sentence is None:
                sentence = Sentence(utter)
                self.tagger.predict(sentence)

            for entity in sentence.get_spans('ner'):
                if entity.tag == 'PER':
                    matches.append({
                        'entity': 'sys-person',
                        'location': [entity.start_pos, entity.end_pos],
                        'value': entity.text,
                        'confidence': entity.score
                    })

        return matches


def load_entities(file_path: str) -> Tuple[Dict[str, str], Dict[str, list], Dict[str, list]]:
    entity_reverse_lookup = {}
    synonyms = {}
    regexprs = {}
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) > 1:  # otherwise entity specification incomplete
                row = [x.strip() for x in row]  # strip any whitespace around cell values
                entity_name = row[0]
                entity_value = row[1]
                if entity_value != '__sys':
                    entity_reverse_lookup[entity_value] = entity_name
                    if len(row) > 2 and row[2].startswith('/'):
                        # A regular expr
                        values = re.split(r'/\s*,\s*/', row[2][1:-1])  # strip start and end '/.../' markers
                        regexprs[entity_value] = values
                    else:
                        # A synonym
                        values = [entity_value, *re.split(r'\s*,\s*', row[2])]  # include the entity_value
                        synonyms[entity_value] = values

    return entity_reverse_lookup, synonyms, regexprs


def match_regexprs(utter: str, regexprs: Dict[str, list]) -> List[Dict[str, Any]]:
    matches = []
    for entity_value, exprs in regexprs.items():
        for expr in exprs:
            for match in re.finditer(expr, utter):
                groups = [{
                    'group': 'group_0',
                    'location': list(match.span())
                }]
                for i, g in enumerate(match.groups()):
                    groups.append({
                        'group': 'group_{}'.format(i + 1),
                        'location': list(match.span(i + 1))
                    })

                entity = {
                    'location': list(match.span()),
                    'value': entity_value,
                    'confidence': 1.0,
                    'groups': groups
                }
                matches.append(entity)

    return matches


def prepare_keyword_processor(synonyms: Dict[str, list]) -> KeywordProcessor:
    """
    28x faster than a compiled regexp for 1,000 keywords
    https://github.com/vi3k6i5/flashtext

    :param synonyms: dict of entity synonyms
    :return:
    """
    kp = KeywordProcessor(case_sensitive=True)
    kp.add_keywords_from_dict(synonyms)
    return kp
