#!/usr/bin/env python

# to compress: ptrepack --chunkshape=auto --propindexes --complevel=9 --complib=blosc store.h5 c.h5

import datetime
import logging
import multiprocessing.dummy
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from multiprocessing.dummy import Lock

import numpy as np
import pandas as pd

# WARNING for pandas >=0.17: https://github.com/pydata/pandas/issues/11786

logging.basicConfig(level=logging.INFO)

# TODO: solve this

if hasattr(ssl, "_create_default_https_context"):
    ssl._create_default_https_context = ssl._create_unverified_context

Gb = 1024.0**3
Tb = 1024.0**4


def group_by_owner(data):
    default_actions = {
        "owner": "count",
        "life_days": "mean",
        "age_days": "mean",
        "last_accessed_days": "mean",
        "size": lambda x: np.sum(x) / Tb,
    }
    actions = {k: default_actions[k]
               for k in data.columns if k in default_actions}
    result = data.groupby("owner").agg(actions)
    result.columns = ["ndatasets"] + list(result.columns)[1:]
    return result


def get_data(rse, date, **kwargs):
    datestr = date.strftime("%d-%m-%Y")
    url = f"https://rucio-hadoop.cern.ch/consistency_datasets?rse={rse}&date={datestr}"
    return to_pandas(
        url, dateformat="ms" if date >= datetime.datetime(2015, 7, 31) else "string", **kwargs
    )


def to_pandas(filename, dateformat="ms", noderived=False):
    def conv(s):
        s = set(s.split(","))
        to_remove = "panda", "root"
        for name in to_remove:
            if name in s:
                s.remove(name)
        s = ",".join(s)
        return s

    try:
        if noderived:
            data = pd.read_csv(
                filename, sep="\t", header=None, converters={3: conv}, usecols=[0, 1, 2, 3, 4]
            )
            data.columns = ["RSE", "scope", "name", "owner", "size"]
        else:
            if dateformat == "ms":
                names = (
                    "RSE",
                    "scope",
                    "name",
                    "owner",
                    "size",
                    "creation_date",
                    "last_accessed_date",
                    "rule_id",
                    "n_replicas",
                    "update_date"
                )
                data = pd.read_csv(
                    filename,
                    sep="\t",
                    header=None,
                    parse_dates=["creation_date",
                                 "last_accessed_date", "update_date"],
                    date_parser=lambda _: pd.to_datetime(float(_), unit="ms"),
                    converters={"owner": conv},
                    names=names,
                )

            elif dateformat == "string":
                names = (
                    "RSE",
                    "scope",
                    "name",
                    "owner",
                    "size",
                    "creation_date",
                    "last_accessed_date",
                )

                data = pd.read_csv(
                    filename,
                    sep="\t",
                    header=None,
                    parse_dates=["creation_date", "last_accessed_date"],
                    converters={"owner": conv},
                    names=names,
                )
    except Exception as ex:
        if not isinstance(ex, urllib.error.HTTPError):
            print(f"cannot parse file from {filename}: {ex}")

        raise

    if not noderived:
        now = datetime.datetime.now()
        data.loc[data["creation_date"].notnull(), "age_days"] = (
            now - data["creation_date"]
        ).dt.days
        data.loc[data["last_accessed_date"].notnull(), "last_accessed_days"] = (
            now - data["last_accessed_date"]
        ).dt.days
        data["life_days"] = data["age_days"] - data["last_accessed_days"]

    return data


def fetch_safe(date, rse):
    d = get_data(rse, date, noderived=True)
    do = group_by_owner(d)
    do = do.reset_index()
    return do


def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError as exc:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentError(msg) from exc


write_lock = Lock()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Dump storage usage.")
    parser.add_argument("--rse")
    parser.add_argument("--yesterday", action="store_true")
    parser.add_argument("--ndays", type=int, default=1,
                        help="numer of days to dump, default=1")
    parser.add_argument("--start", type=valid_date,
                        help="start date, format= YYYY-MM-DD")
    parser.add_argument("--end", type=valid_date,
                        help="end date, format= YYYY-MM-DD")
    parser.add_argument("--nquery", type=int,
                        help="number of concurrent query", default=50)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    if args.rse is None:
        MILANO_RSE = "INFN-MILANO-ATLASC_LOCALGROUPDISK"
        print("WARNING: rse not specified, use %s" % MILANO_RSE)
        args.rse = MILANO_RSE

    if args.yesterday:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        datelist = pd.date_range(end=yesterday, periods=args.ndays).tolist()
    elif args.start is not None and args.end is not None:
        datelist = pd.date_range(start=args.start, end=args.end).tolist()
    elif args.start is not None and args.ndays is not None:
        datelist = pd.date_range(start=args.start, periods=args.ndays).tolist()
    elif args.end is not None and args.ndays is not None:
        datelist = pd.date_range(end=args.end, periods=args.ndays).tolist()
    else:
        raise ValueError("please specify a date range")

    if len(datelist) == 0:
        raise ValueError("date range not valid")

    logging.info("dumping from %s to %s", datelist[0], datelist[-1])

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
            msg = f"\r{self.ndone} ({self.nrunning})/{self.ntotal} errors: {self.nerror}"
            with self.writing_lock:
                sys.stdout.write("\r" + " " * len(msg))
                sys.stdout.flush()
                sys.stdout.write(msg)
                sys.stdout.flush()

        def close(self):
            print()
            print("list of errors")
            for msg in self.msg_errors:
                print(msg)
            print()

    from functools import partial

    def wrap_monitor(f, monitor):
        def w(*args, **kwargs):
            monitor.start()
            try:
                result = f(*args, **kwargs)
                monitor.done()
                return result
            except Exception as ex:
                msg = str(ex)
                if isinstance(ex, urllib.error.HTTPError):
                    msg = f"code {ex.code} for url {ex.url}"
                monitor.error(msg)

        return w

    def wrap_write(f, storage, overwrite=False):
        def w(date, *args, **kwargs):
            key = date.strftime("userdata_%d%m%Y")
            if not overwrite and key in storage:
                return
            value = f(date, *args, **kwargs)
            if value is not None:
                with write_lock:
                    storage[key] = value
                    storage.flush(fsync=True)

        return w

    monitor = Monitor(len(datelist))
    from pandas import HDFStore

    store = HDFStore("store.h5", complevel=9)
    fmap = wrap_monitor(
        wrap_write(partial(fetch_safe, rse=args.rse),
                   store, overwrite=args.overwrite), monitor
    )
    with multiprocessing.dummy.Pool(args.nquery) as pool:
        pool.map(fmap, datelist)
    monitor.close()
    logging.info("closing file")
    store.close()

    logging.info("dumping latest day")
    for date in reversed(sorted(datelist)):
        logging.info("trying downloading %s", date)
        try:
            latest_dataset = get_data(args.rse, date, noderived=False)
        except urllib.error.HTTPError as ex:
            logging.warning(
                "download failed with code %s for url %s", ex.code, ex.url)
            continue
        latest_dataset.to_json(
            "data_all.json", orient='records', date_format='iso')
        break
