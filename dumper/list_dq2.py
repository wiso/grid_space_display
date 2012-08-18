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
logger = None


def generate_xml(data, sitename):
    impl = getDOMImplementation()
    newdoc = impl.createDocument(None, "DiskUsage", None)
    top_element = newdoc.documentElement
    top_element.setAttribute("version", "0.2")
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
            file_node.setAttribute('last_operation', ff['last_operation'].strip())
            file_node.setAttribute('size', ff['size'].strip())
            file_node.setAttribute('creationdate', str(ff['creationdate']).strip())
            file_node.appendChild(newdoc.createTextNode(ff['name']))
            userfiles_node.appendChild(file_node)
        owner_node.appendChild(userfiles_node)

    return newdoc.toxml()


def get_datetime(s):
    try:
        s = s.strip()
    except AttributeError:
        logger.error("attribute error on string %s" % s)
        return datetime.fromtimestamp(0)

    try:
        return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        logger.error("error converting string '%s' to datetime" % s)
        return datetime.fromtimestamp(0)
    except AttributeError:
        logger.error("Attribute error converting string '%s' to datetime"
                     " (python bug, see http://bugs.python.org/issue7980)" % s)
        return datetime.fromtimestamp(0)

class Worker(threading.Thread):
    def __init__(self, queue, out_queue, site):
        threading.Thread.__init__(self)
        logger.info("worker initializing")
        self.queue = queue
        self.out_queue = out_queue
        self.site = site
        self.ndone = 0

    def run(self):
        logger.info("worker starting main loop")
        while True:
            i, dataset_metadata = self.queue.get()
            if (self.ndone + 1) < 3:
                logger.info("starting event %d", self.ndone + 1)
#            with printing_lock:
#                print >> sys.stderr, "\rdataset %d\t(%s)  " % (i, self.name),
#                sys.stderr.flush()
            m = get_metadata(dataset_metadata["name"], self.site)
            m.update(dataset_metadata)
            m['creationdate'] = get_datetime(m['creationdate'])
            m['replica_creation'] = get_datetime(m['replica_creation'])
            self.ndone += 1
            if self.ndone % 100 == 0 or self.ndone < 3:
                logger.info("done %d task", self.ndone)
            self.out_queue.put(m)
            self.queue.task_done()

def download_list(site):
    cmd = "dq2-list-dataset-site2 -eH %s" % site
    logger.info("executing %s", cmd)
    popen = subprocess.Popen(cmd, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
    return list(popen.stdout)

def get_metadata(dataset, site):
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

def parse_output(data):
    """
    data = [first_line, second_line, ...]
    """
    result = []
    for line in data:
        l = line.split(",")
        if len(l) != 6:
            if l != ['']:
                logger.error("error parsing %s" % l)
            continue
        result.append({"name": l[0],
                       "replica": l[1],
                       "last_operation": l[2],
                       "creationdate": l[3],
                       "replica_creation": l[4],
                       "size": l[5]})
    return result

if __name__ == "__main__":
    usage = "usage: %prog [options] site"
    parser = OptionParser(description='Dump information about a space token using dq2 tools',
                          usage = usage)
    parser.add_option('--rerun', action='store_true', default=False, help='reuse the previous list of file')
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
    
    all_datasets_filename = "datasets_%s.txt" % options.site

    # downloading the list of all dataset in the site
    all_datasets = None
    need_to_download_list = not options.rerun
    if (options.rerun):
        try:
            f = open(all_datasets_filename)
            logger.info("reusing datasetlist %s", all_datasets_filename)
            all_datasets = f.read().split('\n')
            f.close()
        except IOError:
            logger.error("cannot find the list of file for site %s", options.site)
            need_to_download_list = True

    if (need_to_download_list):
        logger.info("downloading list of datasets")
        all_datasets = download_list(options.site)
        logger.info("found %s datasets", len(all_datasets))
        logger.info("saving list of dataset in %s", all_datasets_filename)
        f = open(all_datasets_filename, "w")
        for _ in all_datasets:
            f.write(_)

    # put them and the partial metadata in a dictionary
    logger.info("parse metadata into dictionary")
    datasets_metadata = parse_output(all_datasets)

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
        w = Worker(queue, output_queue, options.site)
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
               "size": int(sum( (int(x["size"].strip()) if x["size"].strip().isdigit() else 0
                                 for x in userdata) )/1024./1024.),
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

