# examples INFN-MILANO-ATLASC_LOCALGROUPDISK
#          user08.UmbertoDeSanctis.ganga.006102.Wenu1jet.120605

import subprocess
from operator import itemgetter
from itertools import groupby
from datetime import datetime
import threading
import Queue
import sys
import time
from xml.dom.minidom import getDOMImplementation
from optparse import OptionParser
import logging

from os import path
from math import sqrt
import random

printing_lock = threading.Lock()

use_dq2get_api = True
try:
    from dq2.popularity.client.popularityClient import popularityClient
    from dq2.clientapi.DQ2 import DQ2
except ImportError:
    use_dq2get_api = False

class DQ2SetupError(Exception):
    def __str__(self):
        return "you have to configure dq2 environment"

def generate_xml(data, sitename):
    impl = getDOMImplementation()
    newdoc = impl.createDocument(None, "DiskUsage", None)
    top_element = newdoc.documentElement
    top_element.setAttribute("version", "0.2.2")
    top_element.setAttribute("creator", sys.argv[0])
    import socket
    top_element.setAttribute("host", socket.gethostname())
    top_element.appendChild(newdoc.createComment("NO WARRANTY"))
    metadata = newdoc.createElement("metadata")
    top_element.appendChild(metadata)
    d = newdoc.createElement("time")
    metadata.appendChild(d)
    d.appendChild(newdoc.createTextNode("%s" % datetime.today()))
    sitename_node = newdoc.createElement("sitename")
    sitename_node.appendChild(newdoc.createTextNode(sitename))
    metadata.appendChild(sitename_node)
    data_node = newdoc.createElement("data")
    top_element.appendChild(data_node)
    
    for datum in data:
        owner_node = newdoc.createElement("owner")
        owner_node.setAttribute("name", datum["owner"])
        owner_node.setAttribute("files", datum["nfiles"].__str__())
        owner_node.setAttribute("size", datum["size"].__str__())
        data_node.appendChild(owner_node)
        files_info = datum["filesinfo"]
        userfiles_node = newdoc.createElement("datasets")
        userfiles_node.setAttribute('ndatasets', str(datum["nfiles"]))
        userfiles_node.setAttribute('size', str(datum['size']))
        for ff in files_info:
            file_node = newdoc.createElement("dataset")
            file_node.setAttribute('last_operation', str(ff['lastoperation']).strip())
            file_node.setAttribute('size', str(ff['datasetsize']).strip())
            file_node.setAttribute('creationdate', str(ff['creationdate']).strip())
            file_node.appendChild(newdoc.createTextNode(ff['name']))
            userfiles_node.appendChild(file_node)
        owner_node.appendChild(userfiles_node)

    return newdoc.toxml()


def get_type(s, thetype, specialvalue=type(None)):
    try:
        return thetype(s)
    except ValueError:
        logging.error("attribute %s is not a %s", s, thetype)
        if specialvalue is not type(None):
            return specialvalue
        else:
            return thetype()

def get_datetime(s):
    try:
        s = s.strip()
    except AttributeError:
        logger.error("attribute error on string %s", s)
        return None

    try:
        if "." in s:
            return datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')
        else:
            return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        logger.error("error converting string '%s' to datetime" % s)
        return None
    except AttributeError:
        logger.error("Attribute error converting string '%s' to datetime"
                     " (python bug, see http://bugs.python.org/issue7980)" % s)
        return None

class Worker(threading.Thread):
    def __init__(self, queue, out_queue, site, use_client=False):
        threading.Thread.__init__(self)
        logger.info("worker initializing")
        self.queue = queue
        self.out_queue = out_queue
        self.site = site
        self.ndone = 0
        self.use_client = use_client
        if use_client:
            logger.info("creating DQ2 client")
            self.dq = DQ2()
            def _get_metadata(dsn, site):
                return get_metadata(dsn, site, self.dq)
            self.doquery = _get_metadata
        else:
            self.doquery = get_metadata

    def run(self):
        logger.info("worker starting main loop")
        while True:
            i, dataset_metadata = self.queue.get()
            if (self.ndone + 1) < 3:
                logger.info("starting event %d", self.ndone + 1)
#            with printing_lock:
#                print >> sys.stderr, "\rdataset %d\t(%s)  " % (i, self.name),
#                sys.stderr.flush()
            m = self.doquery(dataset_metadata["dsn"], self.site)
            m.update(dataset_metadata)
            self.ndone += 1
            if self.ndone % 100 == 0 or self.ndone < 3:
                logger.info("done %d task", self.ndone)
            self.out_queue.put(m)
            self.queue.task_done()

def get_list_site_command(site):
    logger.info("downloading list of datasets with dq2-list-dataset-site2 command")

    cmd = "dq2-list-dataset-site2 -eH %s" % site
    logger.info("executing %s", cmd)
    popen = subprocess.Popen(cmd, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
    all_datasets = list(popen.stdout)
    if len(all_datasets) == 1 and 'command not found' in all_datasets[0]:
        raise DQ2SetupError()
    logger.info("found %s datasets", len(all_datasets))

    logger.info("parse metadata into dictionary")
    datasets_metadata = parse_output(all_datasets)
    return datasets_metadata

def get_list_site_api(site):
    p = popularityClient()
    answer = p.listDatasetsInSite(site)
    return answer

get_list_site = get_list_site_api if use_dq2get_api else get_list_site_command

def get_metadata_api(dataset, site, dq):
    ret = dq.listMetaDataReplica(site, dataset)
    res = {'name': dataset}
    for at in ret:
        if at == 'immutable':  
            if ret[at] == 1:
                res['state'] = 'closed'
            elif ret[at] == 0:
                res['state'] = 'open'
            elif ret[at] == 2:
                res['state'] = 'deleted'
                    
        elif at == 'transferState':
            if int(ret[at]) == 0:
                res['transfer state'] = 'active'
            else:
                res['transfer state'] = 'inactive'
                    
        elif at == 'checkState' :  
            if ret[at] == 6:
                res['consistency check'] = 'checked'
            elif ret[at] == 1:
                res['consistency check'] = 'checked'
                out += string.ljust('consistency check', 25) +' : '+ str('waiting') +'\n'                    
            elif ret[at] == 2:
                res['consistency check'] = 'checking'
            elif ret[at] == 0:
                res['consistency check'] = 'Not checked'
        elif at == 'checkdate' :  
            res['consistencymodifieddate'] = str(ret[at])
        elif at == 'transferdate' :  
            res['transfermodifieddate'] = str(ret[at])
        else:
            res[at.lower()] = str(ret[at])
    return res

def get_metadata_command(dataset, site):
    result = {}
    cmd = "dq2-get-replica-metadata %s %s" % (dataset, site)
    popen = subprocess.Popen(cmd, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
    for keyvalue in filter(lambda x: x!='\n', popen.stdout):
        splitted = keyvalue.split("  :")
        if len(splitted) == 2:
            result.update({splitted[0].strip(): splitted[1].strip()})
        else:
            logger.error("cannot parse", keyvalue)
    return result

get_metadata = get_metadata_api if use_dq2get_api else get_metadata_command

def parse_output(data):
    """
    data = [first_line, second_line, ...]
    """
    logger.info("parsing %d line", len(data))
    result = []
    for line in data:
        l = line.split(",")
        if len(l) != 6:
            if l != ['']:
                logger.error("error parsing %s" % l)
            continue
        result.append({"dsn": l[0],
                       "replicas": get_type(l[1], int, None),
                       "lastoperation": get_datetime(l[2]),
                       "creationdate": get_datetime(l[3]),
                       "replica_creationdate": get_datetime(l[4]),
                       "datasetsize": get_type(l[5], int, None)})
    return result

if __name__ == "__main__":
    usage = "usage: %prog [options] site"
    parser = OptionParser(description='Dump information about a space token using dq2 tools',
                          usage = usage)
    parser.add_option('--workers', type=int, help='# number of worker', default=70)
    parser.add_option('--random-workers', action='store_true', default=False, help="use a random number of workes: gaus(N, sqrt(N)) where N is the workers argument")
    parser.add_option('--debug-small', action='store_true', default=False,
                      help='run only on a small subsample (only for debugging)')
    parser.add_option('--output-dir', default=".", help='output directory to store the xml file')
    (options, args) = parser.parse_args()

    if (len(args) == 1):
        options.site = args[0]
    else:
        raise ValueError("you need to specify the site"
                         " (for example INFN-MILANO-ATLASC_LOCALGROUPDISK)"
                         " as positional argument")

    datetime_today = datetime.today()

    # configure logger
    logger = logging.getLogger(__name__)
    fh = logging.FileHandler("log_%s" % datetime_today)
    formatter = logging.Formatter('[%(asctime)s] (%(threadName)-11s) %(levelname)s: %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('[%(relativeCreated)d] %(levelname)s: %(message)s'))
    logger.addHandler(ch)
    logger.setLevel(logging.INFO)


    logger.info("starting")
    logger.info("options: %s", options)
    if options.debug_small:
        logger.warning("using only a small sample")
    
    # downloading the list of all dataset in the site
    all_datasets = None

    logger.info("downloading list of dataset for site %s", options.site)
    datasets_metadata = get_list_site(options.site)

    queue = Queue.Queue()
    output_queue = Queue.Queue()
    # start the workers
    nworkers = options.workers
    if (options.random_workers):
        nworkers = random.gauss(options.workers, sqrt(options.workers))
        nworkers = int(max(options.workers / 2., nworkers))
    
    logger.info("starting %d workers", nworkers)
    workers = []
    for i in range(nworkers):
        w = Worker(queue, output_queue, options.site, use_dq2get_api)
        w.setDaemon(True)
        workers.append(w)
        w.start()
    
    logger.info("filling queue with %d datasets", len(datasets_metadata))
    start = time.time()
    # populate the queue
    if options.debug_small:
        datasets_metadata = datasets_metadata[:20]
    for i, dataset_metadata in enumerate(datasets_metadata):
        queue.put((i+1, dataset_metadata))

    # sit and wait
    queue.join()
    stop = time.time()
    logger.info("Elapsed Time: %s s (%s min). Time per dataset: %s", stop - start, (stop - start) / 60.,
                (stop-start) / len(datasets_metadata))

    # there is not better way to do it?
    datasets_fullmetadata = []
    while not output_queue.empty():
        datasets_fullmetadata.append(output_queue.get())
    
    logger.info("%d dataset(s) parsed", len(datasets_fullmetadata))
    non_working = 0
    for w in workers:
        level = logger.info if w.ndone > 0 else logger.warning
        level("worker %s: %d tasks processed", w.name, w.ndone)
        if w.ndone < (0.5 * len(datasets_fullmetadata) / len(workers)):
            non_working += 1
    if non_working > 0:
        logger.warning("%d workers are working less then half of the mean, "
                       "consider to reduce the number of workers", non_working)


    # grouping result with user id
    try:
        datasets_fullmetadata = sorted(datasets_fullmetadata, key=itemgetter("owner"))
    except KeyError:
        logging.info("unexpected KeyError, dataset_fullmetadata is %s", str(dataset_fullmetadata))
        raise

    groups = []
    users_name = []
    for k, g in groupby(datasets_fullmetadata, key=itemgetter("owner")):
        groups.append(list(g))
        users_name.append(k)

    output_data = []
    for user_name, userdata in zip(users_name, groups):
        row = {"owner": user_name,
               "nfiles": len(userdata),
               "size": sum((x["datasetsize"] for x in userdata if x["datasetsize"]))/1024./1024.,
               "filesinfo": userdata}
        output_data.append(row)

    logger.info("generate xml output")
    xml = generate_xml(output_data, options.site)

    output_filename = "usage_%s_%s.xml" % (options.site, datetime.today().date().isoformat())
    output_complete_filename = path.join(options.output_dir, output_filename)

    logger.info("writing xml output in %s", output_complete_filename)
    with open(output_complete_filename, "w") as f:
        f.write(xml)

    # this is the only output to stdout
    std_output = path.basename(output_complete_filename)
    print std_output
    logger.info("exiting with output %s", std_output)

