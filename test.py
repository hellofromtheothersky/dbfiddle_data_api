from gen_data import gen_data
import json

with open("schema.json", "r") as rf:
    json_data = json.load(rf)
data = gen_data(json_data, ai_data=True, custom_prompt="product name in product table is about dress or pant; companyname in supplier is vietnamese companies")

for key, val in data.items():
    print(key + "-------")
    print(val)
