#!/usr/bin/env python

import os
import sys

if not os.environ.has_key("CMSSW_BASE"):
    print "You need to source the CMSSW environment first."
    sys.exit(1)

required_version = (2,7)
if sys.version_info < required_version:
    print "Your Python interpreter is too old. Need version 2.7 or higher."
    sys.exit(1)

import argparse
import FWCore.ParameterSet.Config as cms
import FWCore.PythonUtilities.LumiList as LumiList



def main(argv=None):
    """
    Main routine. Not called, if this module is loaded via `import`.

    Arguments:
    - `argv`: Command line arguments passed to the script.
    """

    if argv == None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(description="Convert runs into json file")
    required = parser.add_argument_group('required arguments')
    required.add_argument("-b", "--begin", type = int, metavar = "RUN",
                          required = True, help = "first run to be included")
    required.add_argument("-e", "--end", type = int, metavar = "RUN",
                          required=True, help = "last run to be included")
    required.add_argument("-o", "--output", metavar = "FILE", required = True,
                          help = "output file name")
    parser.add_argument("-x", "--exclude", metavar = "RUNS", default = "",
                        help = "comma separated list of runs to exclude")
    args = parser.parse_args(argv)

    run_list = range(args.begin, args.end + 1)
    excludes = [int(x) for x in args.exclude.split(',') if x != ""]
    run_list = [run for run in run_list if run not in excludes]

    lumi_list = LumiList.LumiList(runs = run_list)
    lumi_list.writeJSON(fileName = args.output)
    with open(args.output+".args", "w") as f: f.write(" ".join(argv)+"\n")



if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
