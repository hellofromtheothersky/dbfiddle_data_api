import pandas as pd

# Sample data with different data types


def infer_schema(json_data):
    df = pd.DataFrame(json_data["data"])

    for col in df.columns:
        if df[col].dtype == object:
            try:
                df[col] = pd.to_numeric(df[col])
            except ValueError:
                try:
                    df[col] = pd.to_datetime(df[col])
                except ValueError:
                    pass
    a = dict(df.dtypes)
    for k, v in a.items():
        v = str(v)
        if "int" in v:
            map_dtype = "int"
        elif "float" in v:
            map_dtype = "decimal"
        elif "object" in v:
            map_dtype = "varchar"
        elif "date" in v:
            map_dtype = "timestamp"
        a[k] = map_dtype

    col = "\n  ".join([col + " " + dtype for col, dtype in a.items()])
    return f"Table {json_data['name']} {{\n  {col}\n}}"


# print(infer_schema(    json_data = {
#         "name": "sample",
#         "data":
#         {
#         "A": ["1", "2", "3", "4", "5"],
#         "B": ["a", "b", "c", "d", "e"],
#         "C": ["1.1", "2.2", "3.3", "4.4", "5.5"],
#         "D": ["2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04", "2022-01-05"],
#         "E": [0, 1, 2, 3, 4.1],
#     }}
# ))
