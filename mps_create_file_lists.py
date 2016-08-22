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

    file_list_creator = FileListCreator(argv)


################################################################################
class FileListCreator(object):
    """Create file lists for alignment and validation for a given dataset.
    """

    def __init__(self, argv):
        """Constructor taking the command line arguments.

        Arguments:
        - `args`: command line arguments
        """

        self._dataset_regex = re.compile(r"^/([^/]+)/([^/]+)/([^/]+)$")
        parser = self.define_parser()
        self._args = parser.parse_args(argv)
        self._formatted_dataset = re.sub(self._dataset_regex, r"\1.\2.\3",
                                   self._args.dataset)
        self.prepare_iov_datastructures()
        self.request_dataset_information()
        self.create_file_lists()
        self.print_eventcounts()
        self.write_file_lists()


    def define_parser(self):
        """Definition of command line argument parser."""

        parser = argparse.ArgumentParser(
            description = "Create file lists for alignment")
        parser.add_argument("-i", "--input", dest = "dataset",
                            metavar = "DATASET", required = True,
                            help = "CMS dataset name")
        parser.add_argument("-j", "--json", dest = "json", metavar = "PATH",
                            help = "path to JSON file (optional)")
        parser.add_argument("-f", "--fraction", dest = "fraction",
                            type = float, default = 0.5,
                            help = "max. fraction of files used for alignment")
        parser.add_argument("--iov", dest = "iovs", metavar = "RUN", type = int,
                            action = "append", default = [],
                            help = ("define IOV by specifying first run; for "
                                    "multiple IOVs use this option multiple "
                                    "times"))
        parser.add_argument("-r", "--random", action = "store_true",
                            default = False, help = "select files randomly")
        parser.add_argument("-n", "--events-for-alignment", dest = "events",
                            type = int, metavar = "NUMBER",
                            help = ("number of events needed for alignment; all"
                                    " events are used for validation if n<=0"))
        parser.add_argument("--tracks-for-alignment", dest = "tracks",
                            type = int, metavar = "NUMBER",
                            help = "number of tracks needed for alignment")
        parser.add_argument("--track-rate", dest = "rate", type = float,
                            metavar = "NUMBER",
                            help = "number of tracks per event")
        parser.add_argument("--minimum-events-in-iov",
                            dest = "minimum_events_in_iov", metavar = "NUMBER",
                            type = int, default = 100000,
                            help = ("minimum number of events per IOV "
                                    "(default: %(default)s)"))
        return parser


    def validate_input(self):
        """Validate command line arguments."""

        if self._args.events:
            if self._args.tracks or self._args.rate:
                msg = ("-n/--events-for-alignment must not be used with "
                       "--tracks-for-alignment or --track-rate")
                parser.error(msg)
            print_msg("Requested {0:d} events for alignment."
                      .format(self._args.events))
        else:
            if not (self._args.tracks and self._args.rate):
                msg = ("--tracks-for-alignment and --track-rate must be used "
                       "together")
                parser.error(msg)
            self._args.events = int(math.ceil(self._args.tracks /
                                              self._args.rate))
            print_msg("Requested {0:d} tracks with {1:.2f} tracks/event "
                      "-> {2:d} events for alignment."
                      .format(self._args.tracks, self._args.rate,
                              self._args.events))

        if not re.match(self._dataset_regex, self._args.dataset):
            print_msg("Dataset name '"+self._args.dataset+
                      "' is not in CMS format.")
            sys.exit(1)


    def prepare_iov_datastructures(self):
        """Create the needed objects for IOV handling."""

        self._iovs = sorted(set(self._args.iovs))
        if len(self._iovs) == 0: self._iovs.append(1)
        self._events_per_iov = dict((iov, 0) for iov in self._iovs)


    def request_dataset_information(self):
        """Retrieve general dataset information and create file list."""

        print_msg("Requesting information for dataset '{0:s}'."
                  .format(self._args.dataset))
        self._events_in_dataset = get_events_per_dataset(self._args.dataset)
        self._files = get_files(self._args.dataset)
        if self._args.random: random.shuffle(self._files)


    def create_file_lists(self):
        """Create file lists for alignment and validation."""

        # collect files for alignment until minimal requirements are fulfilled
        print_msg("Counting events in dataset files. This may take a while...")
        sys.stdout.flush()
        self._files_alignment = []
        self._events_for_alignment = 0
        max_range = (0
                     if args.events <= 0
                     else int(math.ceil(len(files)*args.fraction)))
        for i in xrange(max_range):
            number_of_events = get_events_per_file(files[i])
            self._events_for_alignment += number_of_events

            iov = get_iov(guess_run(files[i]), self._iovs)
            if iov:
                self._events_per_iov[iov] += number_of_events
                self._files_alignment.append(files[i])
            if self._events_for_alignment > self._args.events:
                break
        self._files_validation = self._files[i+1:]

        self.fulfill_iov_eventcount()


    def fulfill_iov_eventcount(self):
        """
        Try to fulfill the requirement on the minimum number of events per IOV
        in the alignment file list by picking files from the validation list.
        """

        not_enough_events = [
            iov for iov in self._iovs
            if self._events_per_iov[iov] < self._args.minimum_events_in_iov
            ]
        for iov in not_enough_events:
            for f in self._files_validation:
                if get_iov(guess_run(f), self._iovs) == iov:
                    self._files_alignment.append(f)
                    self._events_for_alignment += get_events_per_file(f)
                    if self._events_per_iov[iov] < args.minimum_events_in_iov:
                        break
        self._files_validation = [f for f in self._files_validation
                                  if f not in self._files_alignment]


    def print_eventcounts(self):
        """Print the event counts per file list and per IOV."""

        events_for_validation = events_in_dataset - events_for_alignment

        print_msg("Using {0:d} events for alignment ({1:.2f}%)."
                  .format(self._events_for_alignment,
                          100.0*
                          self._events_for_alignment/self._events_in_dataset))
        for iov in self._events_per_iov:
            print_msg("Events in IOV ({0:d}): {1:d}"
                      .format(iov, self._events_per_iov[iov]))
        print_msg("Using {0:d} events for validation ({1:.2f}%)."
                  .format(events_for_validation,
                          100.0*events_for_validation/self._events_in_dataset))


    def write_file_lists(self):
        """Write file lists to disk."""

        print_msg("Creating MillePede file list: "+
                  self._formatted_dataset+".txt")
        with open(self._formatted_dataset+".txt", "w") as f:
            f.write("\n".join(self._files_alignment))

        self.create_validation_dataset()


    def get_iov(self, run):
        """
        Return the IOV start for `run`. Returns 'None' if the run is before any
        defined IOV.

        Arguments:
        - `run`: run number
        """

        iov_index = bisect.bisect(self._iovs, run)
        if iov_index > 0: return self._iovs[iov_index-1]
        else: return None


    def create_validation_dataset(self):
        """
        Create configuration fragment to define a dataset for validation.
        """
        file_list = ""
        for sub_list in get_chunks(self._files_validation, 255):
            file_list += "readFiles.extend([\n'"+"',\n'".join(sub_list)+"'\n])\n"

        fragment = FileListCreator.dataset_template.format(
            lumi_def = ("import FWCore.PythonUtilities.LumiList as LumiList\n\n"
                        "lumiSecs = cms.untracked.VLuminosityBlockRange()\n"
                        "goodLumiSecs = LumiList.LumiList(filename = "
                        "'{0:s}').getCMSSWString().split(',')"
                        .format(self._args.json)
                        if self._args.json else ""),
            lumi_arg = ("lumisToProcess = lumiSecs,\n                    "
                        if self._args.json else ""),
            lumi_extend = ("lumiSecs.extend(goodLumiSecs)"
                           if self._args.json else ""),
            files = file_list)

        fragment_name = "_".join(["Dataset", self._formatted_dataset, "cff.py"])
        print_msg("Creating validation dataset configuration fragment: "+
                  fragment_name)
        with open(fragment_name, "w") as f: f.write(fragment)


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


def get_files(dataset_name):
    """Retrieve list of files in `dataset_name`.

    Arguments:
    - `dataset_name`: name of the dataset
    """

    data = das_client("file dataset={0:s} | grep file.name, file.nevents > 0"
                      .format(dataset_name))
    return [find_key(f["file"], "name") for f in data]


def get_events_per_dataset(dataset_name):
    """Retrieve the number of a events in `dataset_name`.

    Arguments:
    - `dataset_name`: name of a dataset
    """

    return _get_events("dataset", dataset_name)


def get_events_per_file(file_name):
    """Retrieve the number of a events in `file_name`.

    Arguments:
    - `file_name`: name of a dataset file
    """

    return _get_events("file", file_name)


def _get_events(entity, name):
    """Retrieve the number of events from `entity` called `name`.

    Arguments:
    - `entity`: type of entity
    - `name`: name of entity
    """

    data = das_client("{0:s}={1:s} | grep file.nevents".format(entity, name))
    return int(find_key(find_key(data, entity), "nevents"))


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


################################################################################
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
