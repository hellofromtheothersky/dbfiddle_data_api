import json
import requests
import re


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


def sql_name_format(s):
    return '"' + s.strip("[]\"'") + '"'


def gen_schema(query):
    question = (
        """
        Infer table and column, data type, and relationships between tables from this sql query.

        Note:
        - Reorder column of table, set the column contain ID column, or column that used to reference another table on the top
        - CTE is not a table, ignore CTE, but still focus on table refered in that CTE
        - If a column data type cannot be defined clearly, make it varchar(100), otherwise, base on the function applying on it, for example, sum(), avg(), min(), max() is int/float, getdate() is datetime, left/right, count() is varchar
        - Dont just focus on column specified in SELECT clause, focus on columns specified in WHERE clause, JOIN condition, ORDER BY, GROUP BY too
        - For table and column type, return me in format of json, table -> columns: for example {"table1" : {"col1": int}, "table1" : {"col1": int}}
        - For relationships, return me a dict too, 
            + To define relationship:
                * Firstly, based on the join condition, if two tables have a join, then they MUST have relationship
                * Secondly, to know which table is referencing another one, you use your knowledge about entity in real life, for example: customer can have many orders, but one order cannot belong to many customers, then orders is referencing customers
                    ^ another example, one people can have many email address, phone number ...
            + For example 
                * Given that, table a is referencing table_b and table_c; table_a join table_b on table_a.hello=table_b.nihao and table_a join table_c on table_a.goodbye=table_c.sayonara the return is {"table_a": {"table_b": ["hello", "nihao"], "table_c": ["goodbye", "sayonara"]}}; 
                * Given that, table b is referencing table_a;table_a join table_b on table_a.hello=table_b.nihao, the return is {"table_b": {"table_a": ["nihao", "hello"]}};
            + CONSTRAINTS: the number of join operation equal to relation you find, ignore any relationship with CTE
        - Overall, the results is formated {"tables" : {}, "relations": {}}  

        Query:

        """
        + query
    )

    # Define the API endpoint and payload
    url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key=AIzaSyAHVdtv1XA47pkIGwF6hoXt2w-aV_EiuEo"
    headers = {"Content-Type": "application/json"}
    data = {"contents": [{"parts": [{"text": question}]}]}

    # Make the POST request
    response = requests.post(url, headers=headers, json=data)

    # Check the response
    if response.status_code == 200:
        schema = extract_json_to_dict(
            response.json()["candidates"][0]["content"]["parts"][0]["text"]
        )
        dbml = ""
        for table, col_info in schema["tables"].items():
            table = sql_name_format(table)
            col_dbml = "\n  ".join(
                [f"{sql_name_format(col)} {dtype}" for col, dtype in col_info.items()]
            )
            dbml += f"Table {table}{{\n  {col_dbml}\n}}\n"
        for child_table, parent_table_set in schema["relations"].items():
            dbml += (
                "\n".join(
                    [
                        f"Ref: {sql_name_format(child_table)}.{sql_name_format(ref_cols[0])} > {sql_name_format(parent_table)}.{sql_name_format(ref_cols[1])}"
                        for parent_table, ref_cols in parent_table_set.items()
                    ]
                )
                + "\n"
            )
        return dbml
    else:
        raise Exception(
            f"Request failed with status code: {response.status_code}: {response.text}"
        )


# print(gen_schema(
#     "SELECT OrderNumber, CompanyName, ProductName FROM Product P JOIN Supplier S ON S.Id = P.SupplierId JOIN OrderItem I ON P.Id = I.ProductId JOIN [Order] O ON O.Id = I.OrderId ORDER BY OrderNumber"
# ))
