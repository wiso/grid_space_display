import pandas as pd
import numpy as np
import cufflinks as cf
cf.go_offline()

import json
class NumpyEncoder(json.JSONEncoder):

    def default(self, obj):
        """If input object is an ndarray it will be converted into a dict
        holding dtype, shape and the data, base64 encoded.
        """
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        # Let the base class default method raise the TypeError
        return json.JSONEncoder(self, obj)

from pandas.io.pytables import HDFStore
store = HDFStore('store.h5')

data = []
for k in store.keys():
    try:
        d = store.get(k)
        d['timestamp'] = pd.to_datetime(k.split("_")[1], format='%d%m%Y')
        data.append(d)
    except Exception as e:
        print "Problem reading", k
        print e
store.close()

data = pd.concat(data)
data = data.set_index(['timestamp', 'owner'])

# stack plot
data_to_plot = data['size'].unstack().fillna(0)
dataplot = data_to_plot.iplot(kind='scatter', fill=True, asFigure=True)
data.iplot(data=dataplot['data'])

f_json_data = open('data_scatter.json', 'w')
json_data = json.dump(dataplot['data'], f_json_data, cls=NumpyEncoder)
f_json_data.close()

# pie plot
latest_data = data[data.index.get_level_values('timestamp') == data.index.get_level_values('timestamp').max()]
dataplot = latest_data.reset_index().iplot(kind='pie', labels='owner', values='size', hole='0.4', sort=True, textinfo='percent', asFigure=True)
dataplot['data'][0]['text'] = ["%.2f Gb" % xx for xx in dataplot['data'][0]['values']]
dataplot['data'][0]['hoverinfo'] = 'text+label'
data.iplot(data=dataplot['data'])

f_json_data = open('data_pie.json', 'w')
json_data = json.dump(dataplot['data'], f_json_data, cls=NumpyEncoder)
f_json_data.close()
