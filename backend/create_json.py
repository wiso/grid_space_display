#!/usr/bin/env python

import json
import logging

import cufflinks as cf
import pandas as pd
import plotly.express as px
from pandas.io.pytables import HDFStore

logging.basicConfig(level=logging.INFO)
cf.go_offline()


logging.info("opening data")
store = HDFStore("store.h5")
keys = list(store.keys())
if not keys:
    raise IOError("store is empty")

logging.info("reading data")
data = []
for k in keys:
    try:
        d = store.get(k)
        d["timestamp"] = pd.to_datetime(k.split("_")[1], format="%d%m%Y")
        data.append(d)
    except Exception as e:
        print("Problem reading", k)
        print(e)
store.close()

data = pd.concat(data)
data = data.set_index(["timestamp", "owner"])


logging.info("doing stack plot")
data_to_plot = data.groupby(level=[0, 1])["size"].sum()
index_sorting = data_to_plot.groupby(level=1).sum().sort_values(ascending=False).index.to_list()
category_order = {"owner": index_sorting}
fig = px.area(
    data_to_plot.reset_index(),
    x="timestamp",
    y="size",
    color="owner",
    category_orders=category_order,
)

with open("data.json", "w", encoding="utf-8") as f_json_data:
    plt_json = fig.to_json(validate=True, pretty=True)
    f_json_data.write(plt_json)


logging.info("doing pie plot")

latest_data = data[
    data.index.get_level_values("timestamp") == data.index.get_level_values("timestamp").max()
].reset_index()

latest_data.loc[latest_data["size"] < latest_data["size"].sum() / 100, "owner"] = "Others"


fig = px.pie(latest_data.reset_index(), values="size", names="owner", hole=0.4)

with open("data_pie.json", "w", encoding="utf-8") as f_json_data:
    plt_json = fig.to_json(pretty=True)
    f_json_data.write(plt_json)


logging.info("doing scatter plot")
json_merged = {}
for k, df_v in data.groupby(level=1):
    plot = px.scatter(df_v.reset_index(), x="timestamp", y="size")
    json_str = plot.to_json(validate=True, pretty=True)
    my_dict = json.loads(json_str)
    json_merged[k] = my_dict

with open("data_scatter.json", "w", encoding="utf-8") as f:
    json.dump(json_merged, f, indent=4)
