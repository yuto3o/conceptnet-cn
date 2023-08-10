# -*- coding: utf-8 -*-
import json

import opencc
import pandas as pd
from tqdm import tqdm

inp_df = pd.read_csv('data/C3KG/C3KG_add_emotion.tsv', header=0, sep='\t')
records = []

for row in tqdm(inp_df.itertuples(), total=len(inp_df)):

    if len(row) != 4:
        continue

    head = getattr(row, 'head')
    relation = getattr(row, 'relation')
    tail = getattr(row, 'tail')

    start_node = head
    end_node = tail
    relation = f"/r/{relation}"

    uri = f"/c/[{relation}/,{start_node}/,{end_node}/]"
    jsons = json.dumps(
        {
            'dataset': '/d/c3kg',
            'license': 'apache-2.0',
            'sources': [{"contributor": "/s/resource/c3kg"}],
        }
    )

    records.append(
        {'uri': uri, 'relation': relation, 'start': start_node, 'end': end_node, 'json': jsons}
    )

out_df = pd.DataFrame.from_records(records)
out_df.to_csv('data/conceptnet_cn_c3kg_part.tsv', sep='\t', header=False, index=False)