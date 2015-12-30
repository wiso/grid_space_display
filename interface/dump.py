import numpy as np
import pandas as pd
# WARNING for pandas >=0.17: https://github.com/pydata/pandas/issues/11786
import logging
import datetime
import urllib2
import sys

logging.basicConfig(level=logging.INFO)

# TODO: solve this
import ssl
if hasattr(ssl, '_create_default_https_context'):
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
            data = pd.read_csv(filename, sep='\t', header=None,
                               converters={3: conv},
                               usecols=[0,1,2,3,4]
                              )
            data.columns = ["RSE", "scope", "name", "owner", "size"]
        else:
            if dateformat == 'ms':
                names = ("RSE", "scope", "name", "owner", "size", "creation_date", "last_accessed_date")
                data = pd.read_csv(filename, sep='\t', header=None,
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
    d = get_data(rse, date, noderived=True)
    do = group_by_owner(d)
    do = do.reset_index()
    do['timestamp'] = date
    return do


def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Dump storage usage.')
    parser.add_argument('--rse')
    parser.add_argument('--yesterday', action='store_true')
    parser.add_argument('--ndays', type=int, default=1)
    parser.add_argument('--start', type=valid_date, help='start date, format= YYYY-MM-DD')
    parser.add_argument('--end', type=valid_date, help='end date, format= YYYY-MM-DD')
    parser.add_argument('--nquery', type=int, help='number of concurrent query', default=50)
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

    import multiprocessing.dummy
    from multiprocessing.dummy import Lock

    p = multiprocessing.dummy.Pool(args.nquery)

    class Monitor(object):
        def __init__(self, ntotal):
            self.nrunning = 0
            self.ndone = 0
            self.nerror = 0
            self.ntotal = ntotal
            self.msg_errors = []
            self.edit_lock = Lock()
            self.writing_lock = Lock()
            self.display()

        def start(self):
            with self.edit_lock:
                self.nrunning += 1
                self.display()

        def done(self):
            with self.edit_lock:
                self.ndone += 1
                self.nrunning -= 1
                self.display()

        def error(self, msg=None):
            with self.edit_lock:
                self.nerror += 1
                self.nrunning -= 1
                self.ndone += 1
                if msg is not None:
                    self.msg_errors.append(msg)
                self.display()

        def display(self):
            msg = "\r{} ({})/{} errors: {}".format(self.ndone, self.nrunning, self.ntotal, self.nerror)
            with self.writing_lock:
                sys.stdout.write('\r' + ' '*len(msg))
                sys.stdout.flush()
                sys.stdout.write(msg)
                sys.stdout.flush()

        def close(self):
            print
            print "list of errors"
            for msg in self.msg_errors:
                print msg

    from functools import partial

    monitor = Monitor(len(datelist))

    def wrap_monitor(f):
        def w(*args, **kwargs):
            monitor.start()
            try:
                result = f(*args, **kwargs)
                monitor.done()
                return result
            except Exception as ex:
                msg = ex.msg
                if isinstance(ex, urllib2.HTTPError):
                    msg = "code %s for url %s" % (ex.code, ex.url)
                monitor.error(msg)
        return w

    fmap = wrap_monitor(partial(fetch_safe, rse=args.rse))
    datas_owner = p.map(fmap, datelist)
    monitor.close()

    from pandas.io.pytables import HDFStore
    store = HDFStore('store.h5')

    for date, data in zip(datelist, datas_owner):
        if data is None:
            logging.warning("no data returned for %s" % date)
            continue
        store[date.strftime('userdata_%d%m%Y')] = data
    store.close()

    logging.info("trying to open output")
    store = HDFStore('store.h5')
    for k in store.keys():
        print store.get(k)
    store.close()
