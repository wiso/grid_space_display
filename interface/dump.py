import numpy as np
import pandas as pd
# WARNING for pandas >=0.17: https://github.com/pydata/pandas/issues/11786
import logging
import datetime
import urllib2

# TODO: solve this
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

Gb = 1024. ** 3
Tb = 1024. ** 4

def group_by_owner(data):
    default_actions = {'owner':'count', 'life_days': 'mean', 'age_days': 'mean', 'last_accessed_days': 'mean', 'size': lambda x: np.sum(x) / Tb}
    actions = {k: default_actions[k] for k in data.columns if k in default_actions}
    result = data.groupby("owner").agg(actions)
    result.columns = ['ndatasets'] + list(result.columns)[1:]
    return result

def get_data(rse, date, **kwargs):
    datestr = date.strftime('%d-%m-%Y')
    url = "https://rucio-hadoop.cern.ch/consistency_datasets?rse=%s&date=%s" % (rse, datestr)
    print url
    return to_pandas(url, dateformat='ms' if date >= datetime.datetime(2015, 7, 31) else 'string', **kwargs)

def to_pandas(filename, dateformat='ms', noderived=False):
    def conv(s):
        if "panda" in s:
            s = s.replace("panda,", "").replace(",panda", "")
        if "root" in s:
            s = s.replace("root,", "").replace(",root", "")
        if "," in s:
            items = s.split(",")
            if len(set(items)) == 1:
                s = items[0]
        return s
    try:
        if noderived:
            data =  pd.read_csv(filename, sep='\t', header=None,
                                converters={3: conv},
                                usecols=[0,1,2,3,4]
                                )
            data.columns = ["RSE", "scope", "name", "owner", "size"]
        else:
            if dateformat == 'ms':
                names = ("RSE", "scope", "name", "owner", "size", "creation_date", "last_accessed_date")
                data =  pd.read_csv(filename, sep='\t', header=None,
                                    parse_dates=["creation_date", "last_accessed_date"],
                                    date_parser=lambda _:pd.to_datetime(float(_), unit='ms'),
                                    converters={"owner": conv},
                                    names=names)

            elif dateformat == 'string':
                names = ("RSE", "scope", "name", "owner", "size", "creation_date", "last_accessed_date")

                data =  pd.read_csv(filename, sep='\t', header=None,
                                    parse_dates=['creation_date', 'last_accessed_date'],
                                    converters={"owner": conv},
                                    names=names)
    except Exception as ex:
        if type(ex) != urllib2.HTTPError:
            print "cannot parse file from %s" % filename
        raise

    if not noderived:
        now = datetime.datetime.now()
        data.loc[data['creation_date'].notnull(), 'age_days'] = (now - data['creation_date']).dt.days
        data.loc[data['last_accessed_date'].notnull(), 'last_accessed_days'] = (now - data['last_accessed_date']).dt.days
        data['life_days'] = data['age_days'] - data['last_accessed_days']

    return data


def fetch_safe(date, rse):
    try:
        d = get_data(rse, date, noderived=True)
        do = group_by_owner(d)
        do = do.reset_index()
        do['timestamp'] = date
        return do
    except urllib2.HTTPError:
        pass
    except:
        print "problem parsing data for %s" % date
        raise

def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Dump storage usage.')
    parser.add_argument('--rse', default="INFN-MILANO-ATLASC_LOCALGROUPDISK")
    parser.add_argument('--yesterday', action='store_true')
    parser.add_argument('--ndays', type=int, default=1)
    parser.add_argument('--start', type=valid_date, help='start date, format= YYYY-MM-DD')
    parser.add_argument('--end', type=valid_date, help='end date, format= YYYY-MM-DD')
    parser.add_argument('--nquery', type=int, help='number of concurrent query', default=20)
    args = parser.parse_args()

    if args.rse is None:
        milano_rse = "INFN-MILANO-ATLASC_LOCALGROUPDISK"
        print "WARNING: rse not specified, use %s" % milano_rse
        args.rse = milano_rse

    if args.yesterday:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        datelist = pd.date_range(end=yesterday, periods=args.ndays).tolist()
    elif args.start is not None and args.end is not None:
        datelist = pd.date_range(start=args.start, end=args.end).tolist()
    elif args.start is not None and args.ndays is not None:
        datelist = pd.date_range(start=args.start, periods=args.ndays).tolist()
    elif args.end is not None and args.ndays is not None:
        datelist = pd.date_range(end=args.end, periods=args.ndays).tolist()


    print datelist
    import multiprocessing.dummy

    p = multiprocessing.dummy.Pool(args.nquery)
    from functools import partial
    datas_owner = p.map(partial(fetch_safe, rse=args.rse), datelist)

    from pandas.io.pytables import HDFStore
    store = HDFStore('store.h5')

    for date, data in zip(datelist, datas_owner):
        store[date.strftime('userdata_%d%m%Y')] = data
    print store
    store.close()
    print store
