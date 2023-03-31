import json

import cufflinks as cf
import numpy as np
import pandas as pd
from pandas.io.pytables import HDFStore
import plotly.express as px


cf.go_offline()


store = HDFStore("store.h5")

data = []
for k in list(store.keys()):
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

# stack plot
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

# pie plot
latest_data = data[
    data.index.get_level_values("timestamp") == data.index.get_level_values("timestamp").max()
]
dataplot = latest_data.reset_index().iplot(
    kind="pie",
    labels="owner",
    values="size",
    hole=0.4,
    sort=True,
    textinfo="percent",
    asFigure=True,
)
dataplot["data"][0]["text"] = ["%.2f Gb" % xx for xx in dataplot["data"][0]["values"]]
dataplot["data"][0]["hoverinfo"] = "text+label"
data.iplot(data=dataplot["data"])


with open("data_pie.json", "w", encoding="utf-8") as f_json_data:
    plt_json = dataplot.to_json(pretty=True)
    f_json_data.write(plt_json)
