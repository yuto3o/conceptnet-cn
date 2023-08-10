# -*- coding: utf-8 -*-
import json
import math

import opencc
import pandas as pd
from tqdm import tqdm

converter = opencc.OpenCC('tw2sp.json')

inp_df = pd.read_csv('data/Chinese_ConceptNet/data/ConceptNet_expand_synonyms.csv', header=0, sep=',')
records = []

for row in tqdm(inp_df.itertuples(), total=len(inp_df)):

    if len(row) != 7:
        continue

    id = getattr(row, 'ID')
    start_node = getattr(row, 'Start')
    end_node = getattr(row, 'End')
    relation = getattr(row, 'Relation')
    surface = getattr(row, 'SurfaceText')
    weight = getattr(row, 'Weight')

    if not isinstance(start_node, str) or not isinstance(end_node, str) or not isinstance(relation, str):
        continue

    start_node = converter.convert(start_node)
    end_node = converter.convert(end_node)

    start_node = f"/c/zh-cn/{start_node}"
    end_node = f"/c/zh-cn/{end_node}"
    relation = f"/r/{relation}"

    uri = f"/c/[{relation}/,{start_node}/,{end_node}/]"
    jsons = json.dumps(
        {
            'dataset': '/d/chinese_conceptnet',
            'license': 'cc:by-sa/4.0',
            'sources': [{"contributor": "/s/resource/chinese_conceptnet", 'surface': surface, 'id': id}],
            'weight': weight
        }
    )

    records.append(
        {'uri': uri, 'relation': relation, 'start': start_node, 'end': end_node, 'json': jsons}
    )

out_df = pd.DataFrame.from_records(records)
out_df.to_csv('data/conceptnet_cn_chinese_conceptnet_part.tsv', sep='\t', header=False, index=False)
