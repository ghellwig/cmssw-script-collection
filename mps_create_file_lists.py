#!/usr/bin/env python

import os
import re
import sys
import json
import math
import bisect
import random
import argparse
import subprocess


################################################################################
def main(argv = None):
    """
    Main routine. Not called, if this module is loaded via `import`.

    Arguments:
    - `argv`: Command line arguments passed to the script.
    """

    if argv == None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(description="Create file lists for alignment")
    parser.add_argument("-i", "--input", dest = "dataset", metavar = "DATASET",
                        required = True, help = "CMS dataset name")
    parser.add_argument("-j", "--json", dest = "json", metavar = "PATH",
                        help = "path to JSON file (optional)")
    parser.add_argument("-f", "--fraction", dest = "fraction", default = 0.5,
                        type = float,
                        help = "maximum fraction of files used for alignment")
    parser.add_argument("--iov", dest = "iovs", metavar = "RUN", type = int,
                        action = "append", default = [],
                        help = ("define IOV by specifying first run; for "
                                "multiple IOVs use this option multiple times"))
    parser.add_argument("-r", "--random", action = "store_true", default = False,
                        help = "select files randomly")
    parser.add_argument("-n", "--events-for-alignment", dest = "events",
                        type = int, metavar = "NUMBER",
                        help = ("number of events needed for alignment; all "
                                "events are used for validation if n<=0"))
    parser.add_argument("--tracks-for-alignment", dest = "tracks", type = int,
                        metavar = "NUMBER",
                        help = "number of tracks needed for alignment")
    parser.add_argument("--track-rate", dest = "rate", type = float,
                        metavar = "NUMBER",
                        help = "number of tracks per event")
    parser.add_argument("--minimum-events-in-iov", dest = "minimum_events_in_iov",
                        type = int, metavar = "NUMBER", default = 100000,
                        help = ("minimum number of events per IOV "
                                "(default: %(default)s)"))
    args = parser.parse_args(argv)

    if args.events:
        if args.tracks or args.rate:
            msg = ("-n/--events-for-alignment must not be used with "
                   "--tracks-for-alignment or --track-rate")
            parser.error(msg)
        print_msg("Requested {0:d} events for alignment.".format(args.events))
    else:
        if not (args.tracks and args.rate):
            msg = "--tracks-for-alignment and --track-rate must be used together"
            parser.error(msg)
        args.events = int(math.ceil(args.tracks / args.rate))
        print_msg("Requested {0:d} tracks with {1:.2f} tracks/event "
                  "-> {2:d} events for alignment."
                  .format(args.tracks, args.rate, args.events))

    args.iovs = sorted(set(args.iovs))
    if len(args.iovs) == 0: args.iovs.append(1)
    events_per_iov = dict((iov, 0) for iov in args.iovs)

    dataset_regex = re.compile(r"^/([^/]+)/([^/]+)/([^/]+)$")
    if not re.match(dataset_regex, args.dataset):
        print_msg("Dataset name '"+args.dataset+"' is not in CMS format.")
        sys.exit(1)

    formatted_dataset = re.sub(dataset_regex, r"\1.\2.\3", args.dataset)

    print_msg("Requesting information for dataset '{0:s}'.".format(args.dataset))
    events_in_dataset = das_client("dataset={0:s} | grep dataset.nevents"
                                   .format(args.dataset))
    events_in_dataset = find_key(find_key(events_in_dataset, "dataset"),
                                 "nevents")

    files = das_client("file dataset={0:s} | grep file.name, file.nevents > 0"
                       .format(args.dataset))
    files = [find_key(f["file"], "name") for f in files]

    if args.random: random.shuffle(files)

    files_alignment = []
    events_for_alignment = 0
    print_msg("Counting events in dataset files. This may take a while...")
    sys.stdout.flush()
    max_range = (0
                 if args.events <= 0
                 else int(math.ceil(len(files)*args.fraction)))
    for i in xrange(max_range):
        number_of_events = get_events_per_file(files[i])
        events_for_alignment += number_of_events

        iov = get_iov(guess_run(files[i]), args.iovs)
        if iov:
            events_per_iov[iov] += number_of_events
            files_alignment.append(files[i])
        if events_for_alignment > args.events:
            break
    files_validation = files[i+1:]

    not_enough_events = [iov for iov in args.iovs
                         if events_per_iov[iov] < args.minimum_events_in_iov]
    for iov in not_enough_events:
        for f in files_validation:
            if get_iov(guess_run(f), args.iovs) == iov:
                files_alignment.append(f)
                events_for_alignment += get_events_per_file(f)
                if events_per_iov[iov] < args.minimum_events_in_iov: break
    files_validation = [f for f in files_validation if f not in files_alignment]

    events_for_validation = events_in_dataset - events_for_alignment

    print_msg("Using {0:d} events for alignment ({1:.2f}%)."
              .format(events_for_alignment,
                      100.0*events_for_alignment/events_in_dataset))
    for iov in events_per_iov:
        print_msg("Events in IOV ({0:d}): {1:d}"
                  .format(iov, events_per_iov[iov]))
    print_msg("Using {0:d} events for validation ({1:.2f}%)."
              .format(events_for_validation,
                      100.0*events_for_validation/events_in_dataset))

    print_msg("Creating MillePede file list: "+formatted_dataset+".txt")
    with open(formatted_dataset+".txt", "w") as f:
        f.write("\n".join(files_alignment))

    create_dataset_file(formatted_dataset, files_validation, args.json)


################################################################################
def das_client(query):
    """
    Submit `query` to DAS client and retrieve output as list of lines.
    Further treatment of the output strings might be necessary.

    Arguments:
    - `query`: DAS query
    """

    # print query
    p = subprocess.Popen(["das_client",
                          "--limit", "0",
                          "--format", "json",
                          "--query", query],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    das_data, err = p.communicate()
    if p.returncode != 0:
        for line in err.split("\n"):
            if "WARNING" in line:
                print line
                sys.exit(1)
    das_data = json.loads(das_data)
    return das_data["data"]


def find_key(collection, key):
    """Searches for `key` in `collection` and returns first corresponding value.

    Arguments:
    - `collection`: list of dictionaries
    - `key`: key to be searched for
    """

    for item in collection:
        if item.has_key(key):
            return item[key]
    raise KeyError(key)


def print_msg(text):
    """Formatted printing of `text`.

    Arguments:
    - `text`: string to be printed
    """

    print "  >>>", text


def guess_run(file_name):
    """
    Try to guess the run number from `file_name`. If run could not be
    determined, 'sys.maxint' is returned.
    
    Arguments:
    - `file_name`: name of the considered file
    """
    try:
        return int("".join(file_name.split("/")[-4:-2]))
    except ValueError:
        return sys.maxint


def get_events_per_file(file_name):
    """Retrieve the number of a events in `file_name`.
    
    Arguments:
    - `file_name`: name of a dataset file
    """
    
    das_data = das_client("file={0:s} | grep file.nevents".format(file_name))
    return int(find_key(find_key(das_data, "file"), "nevents"))


def get_iov(run, iovs):
    """
    Return the IOV start for `run` and a given set of `iovs`. Returns 'None' if
    the run is before any defined IOV.
    
    Arguments:
    - `run`: run number
    - `iovs`: start runs of IOVs
    """
    
    iov_index = bisect.bisect(iovs, run)
    if iov_index > 0: return iovs[iov_index-1]
    else: return None


def create_dataset_file(name, files, json_file = None):
    """
    Create configuration fragment to define a dataset for validation. Fragment
    will be named 'Dataset_`name`_validation_cff.py

    Arguments:
    - `name`: dataset name
    - `files`: list of files
    - `json_file`: path to JSON file
    """
    file_list = ""
    for sub_list in get_chunks(files, 255):
        file_list += "readFiles.extend([\n'"+"',\n'".join(sub_list)+"'\n])\n"

    fragment = dataset_template.format(
        lumi_def = ("import FWCore.PythonUtilities.LumiList as LumiList\n\n"
                    "lumiSecs = cms.untracked.VLuminosityBlockRange()\n"
                    "goodLumiSecs = LumiList.LumiList(filename = "
                    "'{0:s}').getCMSSWString().split(',')".format(json_file)
                    if json_file else ""),
        lumi_arg = ("lumisToProcess = lumiSecs,\n                    "
                    if json_file else ""),
        lumi_extend = ("lumiSecs.extend(goodLumiSecs)"
                       if json_file else ""),
        files = file_list)

    fragment_name = "_".join(["Dataset", name, "cff.py"])
    print_msg("Creating validation dataset configuration fragment: "+
              fragment_name)
    with open(fragment_name, "w") as f: f.write(fragment)


def get_chunks(long_list, chunk_size):
    """
    Generates list of sub-lists of `long_list` with a maximum size of
    `chunk_size`.
    
    Arguments:
    - `long_list`: original list
    - `chunk_size`: maximum size of created sub-lists
    """
    
    for i in xrange(0, len(long_list), chunk_size):
        yield long_list[i:i+chunk_size]


dataset_template = """\
import FWCore.ParameterSet.Config as cms
{lumi_def:s}
readFiles = cms.untracked.vstring()
source = cms.Source("PoolSource",
                    {lumi_arg:s}fileNames = readFiles)
{files:s}{lumi_extend:s}
maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))
"""

################################################################################
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
