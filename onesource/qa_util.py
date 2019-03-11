import json
from pathlib import Path
import re


def load_qa_pairs(data_path):
    result = []
    with open(data_path, 'r') as f:
        for line in f.readlines():
            intent, entities_csv, *qa_pairs = line.split('\t')
            entities = re.split(r'\s*,\s*', entities_csv)
            it = iter([x.strip() for x in qa_pairs])
            pairs = zip(it, it)
            result.append((intent, entities, list(pairs)))

    return result


def save_as_squad_format(pairs, output_path):
    with open(output_path, 'w') as f:
        for q, a in pairs:
            pair = {'question': q, 'answer': [a]}
            f.write(json.dumps(pair))
            f.write('\n')


def prepare_qa_data():
    root_dir = Path(__file__).parent.parent
    data_path = root_dir / 'config/qa_pairs.txt'
    pairs = [p for _, _, pairs in load_qa_pairs(data_path) for p in pairs]
    output_path = root_dir / 'data/qa_pairs.jsonl'
    save_as_squad_format(pairs, output_path)
