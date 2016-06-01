#!/usr/bin/env python

import sys
import argparse
import datetime
import itertools
import matplotlib.pyplot as plt

################################################################################
def main(argv = None):
    """
    Main routine which is not called, if this module is loaded via `import`.

    Arguments:
    - `argv`: command line arguments passed to the script
    """

    if argv == None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(description = "Memory usage plotter")
    parser.add_argument("files", nargs = "+", metavar = "INPUT",
                        help="paths to the input data files")
    args = parser.parse_args(argv)

    data = [extract_data(f) for f in args.files]
    data = [(title, rebase_to_zero(times), kb_to_mb(rss))
            for title, times, rss in data]

    marker = itertools.cycle(("o", "^", "v", "s", "8"))
    color  = itertools.cycle(("red", "blue", "black", "green"))

    plt.figure(figsize = (12, 8), dpi = 200)

    for title, times, rss in data:
        plt.plot(times, rss,
                 marker = marker.next(),
                 color = color.next(),
                 linestyle = "",
                 label = title)

    plt.legend(loc = 2, numpoints = 1)

    ax = plt.gca()
    ax.set_xlabel("time [seconds]")
    ax.set_ylabel("rss [MB]")

    plt.savefig("pede_memory_usage.png", format = "png")
    plt.semilogx()
    plt.savefig("pede_memory_usage_log.png", format = "png")



################################################################################
def extract_data(inputs):
    """Extracts title times and rss values from `inputs`.

    Arguments:
    - `inputs`: path to file containing the input data
                (optionally with comma-separated title)
    """

    time_format = r"%a %b %d %H:%M:%S %Z %Y"
    checked_start = "checking 'cmsRun': "
    checked_split = " >> rss: "

    inputs = inputs.split(",")
    file_name = inputs[0]
    try:
        title = inputs[1]
    except IndexError:
        title = inputs[0]

    times = []
    rss   = []
    with open(file_name, "r") as f:
        for line in f:
            if not line.startswith(checked_start): continue
            line = line.replace(checked_start, "")
            splitted = line.split(checked_split)
            times.append(datetime.datetime.strptime(splitted[0], time_format))
            rss.append(int(splitted[1]))

    return title, times, rss


def rebase_to_zero(times):
    """Rebases time series to seconds starting from zero.

    Arguments:
    - `times`: list of datetime objects
    """

    if len(times) == 0: return times

    rebased = [0]
    for t in times[1:]: rebased.append((t-times[0]).seconds)
    return rebased


def kb_to_mb(rss):
    """Converts `rss` in KB to MB

    Arguments:
    - `rss`: list of rss values
    """

    if len(rss) == 0: return rss
    return [mem/1024 for mem in rss]


################################################################################
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
