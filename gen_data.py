import networkx as nx
import re
import pandas as pd
import itertools
import random
import numpy as np

from math import prod
from random import randrange
import json
import math
from dbml_json_handling import *


class RanProduct:
    def __init__(self, iterables):
        self.its = list(map(list, iterables))
        self.n = prod(map(len, self.its))

    def index(self, i):
        if i not in range(self.n):
            raise ValueError(f"index {i} not in range({self.n})")
        result = []
        for it in reversed(self.its):
            i, r = divmod(i, len(it))
            result.append(it[r])
        return tuple(reversed(result))

    def pickran(self):
        return self.index(randrange(self.n))

    def picklistran(self, k):
        l = random.sample(range(self.n), k)
        r = []
        for i in l:
            r.append(self.index(i))
        return r


def gen_pk_fk(erd, tables_col_info, ref_cols):
    start_nodes = [node for node in erd.nodes if erd.in_degree(node) == 0]

    min_num_rows = 10
    # num of PK in parent table have its FK in child table / num of all PK
    matching_rate = 0.8
    # for per FK on child table, how many occurences of it
    combination_rate = 0.12

    data = dict()

    for start_node in start_nodes:
        stack = [start_node]
        while len(stack) > 0:
            if len(stack) > 20:
                raise Exception("Infinite loop detected")
            node = stack[-1]
            if node in data.keys():
                stack.pop()
                continue

            parent_nodes = list(erd.neighbors(node))
            waiting_nodes = [
                parent_node
                for parent_node in parent_nodes
                if parent_node not in data.keys()
            ]
            if waiting_nodes:
                stack.extend(waiting_nodes)
            else:
                print(f"== Gen: {node}")
                print(f"Dependency tables: {parent_nodes}")
                data[node] = pd.DataFrame()

                # define fk
                node_rows = min_num_rows
                if parent_nodes:
                    parent_rows = []
                    fk_all_tb = []
                    for p in parent_nodes:
                        parent_keys = list(data[p][p])
                        random.shuffle(parent_keys)
                        parent_keys = parent_keys[
                            : int(len(parent_keys) * matching_rate)
                        ]
                        fk_all_tb.append(parent_keys)
                        parent_rows.append(len(parent_keys))

                    combination_rows = max(
                        max(parent_rows), int(math.prod(parent_rows) * combination_rate)
                    )

                    ranproduct = RanProduct(fk_all_tb)

                    all_fk_combination = list(ranproduct.picklistran(combination_rows))
                    random.shuffle(all_fk_combination)

                    node_rows = max(node_rows, len(all_fk_combination))
                    for i, parent_table in enumerate(parent_nodes):
                        fk_col_val = [int(fk[i]) for fk in all_fk_combination][
                            :node_rows
                        ]
                        fk_col_val.extend(
                            [None for i in range(len(all_fk_combination), min_num_rows)]
                        )
                        data[node][parent_table] = fk_col_val
                        data[node][parent_table] = data[node][parent_table].astype(
                            pd.Int64Dtype()
                        )

                # define pk
                data[node][node] = [i for i in range(node_rows)]

                print(f"{node_rows} row(s)")

                stack.pop()

    for node in erd.nodes:
        col_name_mapping = {}
        for col in data[node].columns:
            if col == node and "?" in tables_col_info[node].keys():
                col_name_mapping[col] = tables_col_info[node]["?"]
            elif col != node:
                col_name_mapping[col] = ref_cols[f"{col}_to_{node}"][1]

        data[node] = data[node][col_name_mapping.keys()]
        data[node] = data[node].rename(columns=col_name_mapping)
    return data


def sanitize_value(value):
    # Remove any special characters from the string value
    return re.sub(r"[^a-zA-Z0-9_]+", "", value)


def create_sample_data(column_types, num_rows):
    data_samples = []

    for _ in range(num_rows):
        row = {}
        for column_name, data_type in column_types.items():
            if "int" in data_type:
                row[column_name] = random.randint(
                    1, 100
                )  # Random int between 1 and 100
            elif "varchar" in data_type:
                sanitized_name = sanitize_value(column_name)
                row[column_name] = (
                    f"{sanitized_name}_{random.randint(1, 1000)}"  # Randomized suffix
                )
            elif "date" in data_type:
                row[column_name] = (
                    f"2024-11-{random.randint(1, 30):02d}"  # Random datetime
                )
            elif "datetime" in data_type or "timestamp" in data_type:
                row[column_name] = (
                    f"2024-11-{random.randint(1, 30):02d} {random.randint(0, 23):02d}:{random.randint(0, 59):02d}:00"  # Random datetime
                )
            elif "decimal" in data_type or "float" in data_type:
                row[column_name] = round(
                    random.uniform(1.0, 100.0), 2
                )  # Random decimal between 1.0 and 100.0
            elif "boolean" in data_type:
                row[column_name] = random.choice([True, False])  # Random True/False
            else:
                raise Exception(f"Data type: {data_type} cannot be defined")
        data_samples.append(row)

    return pd.DataFrame(data_samples)


def gen_data(json_data, list_output=False):
    tables_col_info = extract_tables(json_data)
    tables = list(tables_col_info.keys())

    many_one_relationships, ref_cols = extract_ref(json_data)

    print(f"Tables:\n {tables}\n")
    print(f"Many one relationships:\n {many_one_relationships}\n")

    erd = nx.DiGraph()
    erd.add_nodes_from(tables)
    erd.add_edges_from(many_one_relationships)

    data = gen_pk_fk(erd, tables_col_info, ref_cols)

    for table in tables:
        for col in data[table].columns:
            if 'int' not in tables_col_info[table][col]:
                data[table][col]=data[table][col].astype(str).replace("<NA>", np.nan)

        no_data_col = {
            name: dtype
            for name, dtype in tables_col_info[table].items()
            if name != "?" and name not in data[table].columns
        }

        data_for_no_data_col = create_sample_data(no_data_col, len(data[table]))

        data[table] = data[table].join(data_for_no_data_col)
        data[table] = data[table][[x for x in tables_col_info[table].keys() if x!='?']]
        if list_output:
            data[table] = data[table].values.tolist()

    return data


# with open("schema.json", "r") as rf:
#     json_data = json.load(rf)
# data = gen_data(json_data)

# for key, val in data.items():
#     print(key + "-------")
#     print(val)
