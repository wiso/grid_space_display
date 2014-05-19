from stat import S_ISREG, ST_CTIME, ST_MODE
import os
import subprocess


def list_xmlfiles(dirpath):
    entries = (os.path.join(dirpath, fn) for fn in os.listdir(dirpath))
    # leave only regular xml files
    entries = (path for path in entries if (S_ISREG(os.stat(path)[ST_MODE]) and os.path.splitext(path)[1] == ".xml"))
    # sort in chronological order
    entries = sorted(entries, key=lambda path: os.stat(path)[ST_CTIME])
    return entries


def isslimmed(f):
    return False


def slim(filename):
    from os import path
    current_dir = path.dirname(path.realpath(__file__))
    arg_call = ["xsltproc", "-o", filename, path.join(current_dir, "slim.xslt"), filename]
    subprocess.call(arg_call)

if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-d", "--directory",
                      help="input directory with the xmls")
    parser.add_option("-n", "--number", type=int, default=1,
                      help="number of newest xml to don't slim")
    (options, args) = parser.parse_args()
    files_to_slim = list_xmlfiles(options.directory)[:-options.number]
    files_to_slim = (f for f in files_to_slim if not isslimmed(f))
    for f in files_to_slim:
        print "slimming", f
        slim(f)
