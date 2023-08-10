# -*- coding: utf-8 -*-
import json
import re

import pandas as pd
from pygtrans import Translate

translator = Translate(source='en', proxies={'https': 'http://localhost:14362', 'http': 'http://localhost:14362'})

inp_df0 = pd.read_csv('data/atomic2020_data-feb2021/train.tsv', header=None, sep='\t')
inp_df1 = pd.read_csv('data/atomic2020_data-feb2021/dev.tsv', header=None, sep='\t')
inp_df2 = pd.read_csv('data/atomic2020_data-feb2021/test.tsv', header=None, sep='\t')
inp_df = pd.concat([inp_df0, inp_df1, inp_df2])

all_nodes = set()
for row in inp_df.itertuples():
    _0, start, _2, end = row[:4]

    if not isinstance(start, str) or not isinstance(end, str):
        continue

    all_nodes.add(start)
    all_nodes.add(end)
all_nodes = list(all_nodes)

N = 10000
for i in range(len(all_nodes) // N + 1):
    nodes = all_nodes[N * i:N * (i + 1)]
    trans = translator.translate(nodes)

    node2tran = {node: tran.translatedText for node, tran in zip(nodes, trans)}
    with open(f"node2tran/node2tran_{i}.json", 'w', encoding='utf-8') as f:
        json.dump(node2tran, f, ensure_ascii=False)

node2tran = {}
for i in range(len(all_nodes) // N + 1):
    node2tran.update(json.load(open(f"node2tran/node2tran_{i}.json", encoding='utf-8')))


def remove_punctuation(text):
    # 使用正则表达式匹配标点符号并替换为空格
    text = re.sub(r'[^\w\s_]', '', text)
    text = text.replace(' ', '')
    return text


records = []
for row in inp_df.itertuples():
    if not isinstance(row[1], str) or not isinstance(row[3], str):
        continue

    start_node = row[1]
    end_node = row[3]

    start_node = node2tran[start_node]
    end_node = node2tran[end_node]

    start_node = remove_punctuation(start_node)
    end_node = remove_punctuation(end_node)

    start_node = f"/c/zh-cn/{start_node}"
    end_node = f"/c/zh-cn/{end_node}"

    relation = f"/r/{row[2]}"
    uri = f"/c/[{relation}/,{start_node}/,{end_node}/]"
    jsons = json.dumps(
        {
            'dataset': '/d/atomic2020',
            'license': 'cc:by-sa/4.0',
            'sources': [{"contributor": "/s/resource/atomic2020"}],
        }
    )
    record = {'uri': uri, 'relation': relation, 'start': start_node, 'end': end_node, 'json': jsons}
    records.append(record)

out_df = pd.DataFrame.from_records(records)
out_df.to_csv('data/conceptnet_cn_atomic2020_part.tsv', sep='\t', header=False, index=False)
