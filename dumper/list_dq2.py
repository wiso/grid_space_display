import subprocess
from operator import itemgetter
from itertools import groupby
from datetime import datetime
import threading
import Queue
import sys
import time
import csv
from xml.dom.minidom import getDOMImplementation
from optparse import OptionParser
import logging
from os import path

printing_lock = threading.Lock()

def generate_xml(data, sitename):
    impl = getDOMImplementation()
    newdoc = impl.createDocument(None, "DiskUsage", None)
    top_element = newdoc.documentElement
    top_element.setAttribute("version", "0.1")
    top_element.setAttribute("creator", sys.argv[0])
    comment = newdoc.createComment("NO WARRANTY")
    top_element.appendChild(comment)
    metadata = newdoc.createElement("metadata")
    top_element.appendChild(metadata)
    d = newdoc.createElement("time")
    metadata.appendChild(d)
    dd = newdoc.createTextNode("%s" % datetime.today())
    d.appendChild(dd)
    sitename_node = newdoc.createElement("sitename")
    sitename_attribute = newdoc.createTextNode(sitename)
    sitename_node.appendChild(sitename_attribute)
    metadata.appendChild(sitename_node)
    data_node = newdoc.createElement("data")
    top_element.appendChild(data_node)
    
    for datum in data:
        owner_node = newdoc.createElement("owner")
        owner_node.setAttribute("name", datum["owner"])
        owner_node.setAttribute("files", datum["files"].__str__())
        owner_node.setAttribute("size", datum["size"].__str__())
        data_node.appendChild(owner_node)

    return newdoc.toxml()


def get_datetime(s):
    try:
        s = s.strip()
    except AttributeError:
        logging.error("attribute error on string %s" % s)
        return datetime.fromtimestamp(0)

    try:
        return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        logging.error("error converting string '%s' to datetime" % s)
        return datetime.fromtimestamp(0)
    except AttributeError:
        logging.error("Attribute error converting string '%s' to datetime (python bug, see http://bugs.python.org/issue7980)" % s)
        return datetime.fromtimestamp(0)

class Worker(threading.Thread):
    def __init__(self, queue, out_queue, site):
        threading.Thread.__init__(self)
        self.queue = queue
        self.out_queue = out_queue
        self.site = site

    def run(self):
        while True:
            i, dataset_metadata = self.queue.get()
            with printing_lock:
                print >> sys.stderr, "\rdataset %d\t(%s)  " % (i, self.name),
                sys.stderr.flush()
            m = get_metadata(dataset_metadata["name"], self.site)
            m.update(dataset_metadata)
            m['creationdate'] = get_datetime(m['creationdate'])
            m['replica_creation'] = get_datetime(m['replica_creation'])
            self.out_queue.put(m)
            self.queue.task_done()

def download_list(site):
    cmd = "dq2-list-dataset-site2 -eH %s" % site
    logging.info("executing", cmd)
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
            logging.error("cannot parse", keyvalue)
    return result

def parse_output(data):
    """
    data = [first_line, second_line, ...]
    """
    result = []
    for line in data:
        l = line.split(",")
        if len(l) != 6:
            if l!=['']:
                logging.error("error parsing %s" % l)
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
    parser.add_option('--debug_small', action='store_true', default=False, help='run only on a small subsample (only for debugging)')
    parser.add_option('--output-dir', default=".", help='output directory to store the xml file')
    (options, args) = parser.parse_args()

    if (len(args) == 1):
        options.site = args[0]
    else:
        raise ValueError("you need to specify the site (for example INFN-MILANO-ATLASC_LOCALGROUPDISK) as positional argument")
    
    all_datasets_filename = "datasets_%s.txt" % options.site

    datetime_today = datetime.today()

    # downloading the list of all dataset in the site
    all_datasets = None
    need_to_download_list = not options.rerun
    if (options.rerun):
        try:
            f = open(all_datasets_filename)
            logging.info("reusing datasetlist %s" % all_datasets_filename)
            all_datasets = f.read().split('\n')
            f.close()
        except IOError:
            logging.error("cannot find the list of file for site %s" % options.site)
            need_to_download_list = True

    if (need_to_download_list):
        logging.info("downloading list of datasets")
        all_datasets = download_list(options.site)
        f = open(all_datasets_filename, "w")
        for _ in all_datasets:
            f.write(_)

    # put them and the partial metadata in a dictionary
    datasets_metadata = parse_output(all_datasets)

    queue = Queue.Queue()
    output_queue = Queue.Queue()
    # start the workers
    for i in range(options.workers):
        w = Worker(queue, output_queue, options.site)
        w.name = "worker %d" % i
        w.setDaemon(True)
        w.start()
    
    logging.info("processing %d datasets" % len(datasets_metadata))
    start = time.time()
    # populate the queue
    if options.debug_small: datasets_metadata = datasets_metadata[:20]
    for i, dataset_metadata in enumerate(datasets_metadata):
        queue.put((i+1, dataset_metadata))

    # sit and wait
    queue.join()
    stop = time.time()

    # there is not better way to do it?
    datasets_fullmetadata = []
    while not output_queue.empty():
        datasets_fullmetadata.append(output_queue.get())

    logging.info("Elapsed Time: %s. Time per dataset: %s" % (stop - start, (stop-start) / len(datasets_fullmetadata)))
    logging.info("%d dataset parsed" % len(datasets_fullmetadata))

    # grouping result with user id
    datasets_fullmetadata = sorted(datasets_fullmetadata, key=itemgetter("owner"))

    groups = []
    users_name = []
    for k, g in groupby(datasets_fullmetadata, key=itemgetter("owner")):
        groups.append(list(g))
        users_name.append(k)


    for i, (datasets_user,u) in enumerate(zip(groups, users_name)):
        user_filename = "%s_filelist_user%d.html" % (options.site, i)
        fuser = open(path.join(options.output_dir, user_filename), "w")
        fuser.write("""
<html>
<head><title>{user} stastistics</title></head>
<body>
<h2>{user}</h2>
<p>time: {time}</p>
<table>
""".format(user= u, time=datetime_today)
                )
        # loop over datasets for a user
        byte_user = 0
        for dd in sorted(datasets_user, key=itemgetter("creationdate")):
            dataset_size = 0
            try:
                dataset_size = int(dd["size"])
                byte_user += dataset_size
            except ValueError:
                logging.error("size is not a number for user %s, dataset %s" % (u, dd))
                dataset_size = dd["size"]
            entry = "  <tr><td>%(name)s</td><td>%(size)s</td><td>%(creationdate)s</td></tr>\n" % {'name': dd["name"],
                                                                                                  'creationdate': dd["creationdate"],
                                                                                                  'size': dataset_size}
            fuser.write(entry)
        fuser.write("""
</table>
</body>
</html>
""")

    ###################################
    # remove the csv output
    fieldnames = ["owner", "files", "size"]
    fcvs = open("data.csv", "wb")
    fcvs.write(",".join(fieldnames) + '\n')
    cvsWriter = csv.DictWriter(fcvs, delimiter=',',
                              quotechar='|', quoting=csv.QUOTE_MINIMAL, fieldnames=fieldnames)
    #cvsWriter.writeheader() # new in 2.7
    output_data = []
    for user_name, userdata in zip(users_name, groups):
        row = {"owner": user_name,
               "files": len(userdata),
               "size": int(sum(map(lambda x: int(x["size"].strip()) if x["size"].strip().isdigit() else 0, userdata))/1024./1024.)}
        output_data.append(row)
        cvsWriter.writerow(row)
    ####################################


    xml = generate_xml(output_data, options.site)

    output_filename = "usage_%s_%s.xml" % (options.site, datetime.today().date().isoformat())
    output_complete_filename = path.join(options.output_dir, output_filename)

    with open(output_complete_filename, "w") as f:
        f.write(xml)

    print output_complete_filename # this is the only output to stdout
