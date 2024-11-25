import json

def extract_tables(json_data):
    tables_dict = {}
    tables = [None]

    for key, val in json_data["tables"].items():
        tables_dict[val["name"]] = {}
        tables.append(val["name"])

    for key, val in json_data["fields"].items():
        if val["pk"]:
            tables_dict[tables[val["tableId"]]]["?"] = val["name"]
        tables_dict[tables[val["tableId"]]][val["name"]] = val["type"]["type_name"]
    return tables_dict


def extract_ref(json_data):
    many_one_relationships = []
    refs_col = {}
    for key, val in json_data["refs"].items():
        endpoint_type_one = str(val["endpointIds"][0])
        endpoint_type_many = str(val["endpointIds"][1])

        if json_data["endpoints"][endpoint_type_one]["relation"] != "1":
            endpoint_type_one, endpoint_type_many = (
                endpoint_type_many,
                endpoint_type_one,
            )

        many_one_relationships.append(
            [
                json_data["endpoints"][endpoint_type_many]["tableName"],
                json_data["endpoints"][endpoint_type_one]["tableName"],
            ]
        )
        refs_col[
            f"{many_one_relationships[-1][1]}_to_{many_one_relationships[-1][0]}"
        ] = [
            json_data["endpoints"][endpoint_type_one]["fieldNames"][0],
            json_data["endpoints"][endpoint_type_many]["fieldNames"][0],
        ]

    return many_one_relationships, refs_col
