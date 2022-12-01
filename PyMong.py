import json
import requests
import pandas as pd
from pymongo import MongoClient

client = MongoClient('localhost', 2717)

db = client.test

req_url = "https://api.openaq.org/v1/latest?country=IN&limit=10000"

r = requests.get(req_url)

files = r.json()

locations = []
cities = []
countries = []
values = []
for x in files["results"]:
    for y in x["measurements"]:
        value = y["value"]
        locations.append(x["location"])
        cities.append(x["city"])
        values.append(value)
        countries.append(x["country"])
dict = {'location': locations, 'city': cities, 'country':countries, 'value':values}

df = pd.DataFrame(dict)

print(df)

db.pollution.insert_many(df.to_dict("records"))