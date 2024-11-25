from gen_data import gen_data
import json

with open("schema.json", "r") as rf:
    json_data = json.load(rf)
data = gen_data(json_data)

for key, val in data.items():
    print(key + "-------")
    print(val)
