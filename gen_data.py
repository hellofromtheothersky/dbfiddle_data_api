import networkx as nx
import re
import pandas as pd
import random
import numpy as np

from math import prod
from random import randrange
import math
from dbml_json_handling import *
import requests
import itertools

min_num_rows = 5
max_num_rows = 99
# num of PK in parent table have its FK in child table / num of all PK
matching_rate = 0.8
# for per FK on child table, how many occurences of it
permutation_rate = 0.12
max_num_rows_permutation=32


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
                node_num_rows = min_num_rows
                if parent_nodes:
                    num_rows_per_parent = []
                    all_parent_keys_set = []
                    for p in parent_nodes:
                        parent_keys = list(data[p][p])
                        random.shuffle(parent_keys)
                        parent_keys = parent_keys[
                            : int(len(parent_keys) * matching_rate)
                        ]
                        all_parent_keys_set.append(parent_keys)
                        num_rows_per_parent.append(len(parent_keys))

                    null_permutations = [
                        "".join(seq)
                        for seq in itertools.product("01", repeat=len(parent_nodes))
                    ][
                        1:max_num_rows_permutation
                    ]  # ignore the first one, which is all non null value
                    selected_num_permutation_rows = min(
                        max(
                            max(num_rows_per_parent),
                            int(math.prod(num_rows_per_parent) * permutation_rate),
                        ),
                        max_num_rows - len(null_permutations)
                    )

                    node_num_rows = selected_num_permutation_rows + len(
                        null_permutations
                    )

                    ranproduct = RanProduct(all_parent_keys_set)

                    selected_fk_permutation = list(
                        ranproduct.picklistran(selected_num_permutation_rows)
                    )
                    random.shuffle(selected_fk_permutation)

                    for parent_table_i, parent_table in enumerate(parent_nodes):
                        fk_col_val = [
                            fk[parent_table_i] for fk in selected_fk_permutation
                        ]
                        for null_permutation in null_permutations:
                            if null_permutation[parent_table_i] == "1":
                                fk_col_val.append(None)
                            else:
                                fk_col_val.append(
                                    random.choice(all_parent_keys_set[parent_table_i])
                                )

                        data[node][parent_table] = fk_col_val
                        data[node][parent_table] = data[node][parent_table].astype(
                            pd.Int64Dtype()
                        )

                # define pk
                data[node][node] = [i + 1 for i in range(node_num_rows)]

                print(f"{node_num_rows} row(s)")

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


def extract_json_to_dict(text):
    # Use a regular expression to find the JSON part inside the triple backticks
    json_pattern = r"```json\n(.*?)\n```"
    match = re.search(json_pattern, text, re.DOTALL)

    if match:
        json_text = match.group(1)  # Extract the matched JSON text
        try:
            # Convert the JSON text to a Python dictionary
            return json.loads(json_text)
        except json.JSONDecodeError as e:
            print("Failed to decode JSON:", e)
            return None
    else:
        print("No JSON found in the text.")
        return None


def create_ai_data(table_name, column_types, num_rows, custom_prompt):
    print("AI processing for: ", table_name)
    question = (
        """
        Generate """
        + str(num_rows)
        + """ sample value for each column in each table suitable with its data type, 
        the input is a dict in this format {"table1": {"col1": datatype}}, the output is a json format, 
        for example, {"table1": {"col1": [list of sample value]}}:

        Another requirements, it will tell you how to define data for some specific columns in specific tables clearly (ignore if it is blank)
        """
        + custom_prompt
        + """

        Input:

        """
        + str({table_name: column_types})
    )

    # Define the API endpoint and payload
    url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key=AIzaSyAHVdtv1XA47pkIGwF6hoXt2w-aV_EiuEo"
    headers = {"Content-Type": "application/json"}
    data = {"contents": [{"parts": [{"text": question}]}]}

    # Make the POST request
    response = requests.post(url, headers=headers, json=data)

    # Check the response
    if response.status_code == 200:
        data = extract_json_to_dict(
            response.json()["candidates"][0]["content"]["parts"][0]["text"]
        )
        df = pd.DataFrame()
        for col, col_data in data[table_name].items():
            df[col] = col_data
        return df
    else:
        raise Exception(
            f"Request failed with status code: {response.status_code}: {response.text}"
        )


def create_sample_data(column_types, num_rows):
    data_samples = []

    for _ in range(num_rows):
        row = {}
        for column_name, data_type in column_types.items():
            data_type = data_type.lower()
            if "int" in data_type or "uniqueidentifier" in data_type:
                row[column_name] = random.randint(
                    1, 1000
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


def gen_data(json_data, list_output=False, ai_data=True, custom_prompt=""):
    tables_col_info = extract_tables(json_data)
    print(
        {
            k: {k1: v1 for k1, v1 in v.items() if k1 != "?"}
            for k, v in tables_col_info.items()
        }
    )
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
            if "int" not in tables_col_info[table][col]:
                data[table][col] = data[table][col].astype(str).replace("<NA>", np.nan)

        no_data_col = {
            name: dtype
            for name, dtype in tables_col_info[table].items()
            if name != "?" and name not in data[table].columns
        }

        if ai_data:
            data_for_no_data_col = create_ai_data(
                table, no_data_col, len(data[table]), custom_prompt
            )
        else:
            data_for_no_data_col = create_sample_data(no_data_col, len(data[table]))

        data[table] = data[table].join(data_for_no_data_col)
        data[table] = data[table][
            [x for x in tables_col_info[table].keys() if x != "?"]
        ]
        if list_output:
            data[table] = [val for key, val in data[table].T.to_dict().items()]
    return data


# with open("schema.json", "r") as rf:
#     json_data = json.load(rf)
# data = gen_data(json_data)

# for key, val in data.items():
#     print(key + "-------")
#     print(val)
