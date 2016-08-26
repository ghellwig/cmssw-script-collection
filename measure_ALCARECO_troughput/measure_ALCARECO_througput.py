#!/usr/bin/env python

import os
import sys
import time
import shutil
import signal
import argparse
import functools
import subprocess
import multiprocessing

import Configuration.PyReleaseValidation.MatrixReader as MatrixReader
import Configuration.PyReleaseValidation.MatrixRunner as MatrixRunner
import Configuration.PyReleaseValidation.relval_steps as relval_steps
import Configuration.PyReleaseValidation.relval_production as relval_production

################################################################################
def main(argv = None):
    """
    Main routine. Not called, if this module is loaded via `import`.

    Arguments:
    - `argv`: Command line arguments passed to the script.
    """

    if argv == None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(description = "Measure ALCARECO throughput")
    parser.add_argument("-i", "--input", dest = "inputs", metavar = "STEP",
                        action = "append", default = [], required = True,
                        help = ("input step as defined in"
                                "'Configuration/PyReleaseValidation/python/"
                                "relval_steps.py'; for multiple inputs use this"
                                " option multiple times"))
    parser.add_argument("--list-input-choices", dest = "list_input_choices",
                        action = "store_true", default = False,
                        help = "list all possible inputs and exit")
    parser.add_argument("-g", "--global-tag", dest = "global_tag",
                        metavar = "TAG", default = "auto:run2_data",
                        help = ("global tag to be used for the comparison "
                                "(default: '%(default)s')"))
    parser.add_argument("-n", "--number-of-events", dest = "number_of_events",
                        metavar = "NUMBER", type = int, default = 100,
                        help = ("number of events to perform the measurement "
                                "(default: '%(default)s')"))
    parser.add_argument("-N", "--job-name", dest = "job_name", metavar = "ID",
                        help = "name for the measurment job")
    parser.add_argument("--number-of-processes", dest = "number_of_processes",
                        metavar = "NUMBER", type = int, default = 4,
                        help = ("number of processes to be used "
                                "(default: '%(default)s')"))
    parser.add_argument("--dry-run", dest = "dryRun", action = "store_true",
                        default = False,
                        help = "create only scripts without submission")
    parser.add_argument("-p", "--pull-request", dest = "pull_request",
                        type = int, metavar = "ID",
                        help = "measure changes introduced by this pull request")
    parser.add_argument("-b", "--batch", dest = "batch", metavar = "QUEUE",
                        help = ("submit job to QUEUE (NOTE: will be slower "
                                "because multiprocessing does not work well on "
                                "worker nodes)"))

    args = parser.parse_args(argv)

    if args.list_input_choices:
        list_input_choices()
        sys.exit()

    if not args.job_name:
        args.job_name = get_unique_output_name()
        sys.argv.extend(["-N", args.job_name])

    if args.batch:
        submit_to_batch(args.job_name)

    args = add_required_defaults(args)

    os.makedirs(args.job_name)
    os.chdir(args.job_name)



    # workaround to deal with KeyboardInterrupts in the worker processes:
    # - ignore interrupt signals in workers (see initializer)
    # - use a timeout of size sys.maxint to avoid a bug in multiprocessing
    pool = multiprocessing.Pool(
        processes = args.number_of_processes,
        initializer = lambda: signal.signal(signal.SIGINT, signal.SIG_IGN))
    run_workflows_with_args = functools.partial(run_workflows, args = args)
    result = pool.map_async(run_workflows_with_args, args.inputs).get(sys.maxint)
    print result

################################################################################
def matrix_reader(args):
    """Create MatrixReader object.

    Arguments:
    - `args`: command line args
    """

    args = add_required_defaults(args)
    return MatrixReader.MatrixReader(args)


def matrix_runner(reader, args):
    """Create MatrixRunner object.

    Arguments:
    - `reader`: MatrixReader object
    - `args`: command line args
    """

    args = add_required_defaults(args)
    return MatrixRunner.MatrixRunner(reader.workFlows,
                                     args.nProcs,
                                     args.nThreads)


def add_required_defaults(args):
    """Add default values for required arguments.

    Arguments:
    - `args`: command line arguments
    """

    defaults = {"what": "production",
                "wmcontrol": None,
                "revertDqmio": "no",
                "command": None,
                "apply": None,
                "workflow": None,
                "overWrite": None,
                "noRun": False,
                "cafVeto": True,
                "dasOptions": "--limit 0",
                "jobReports": False,
                "nProcs": 1,
                "nThreads": 1,
                }

    for key in defaults:
        if not hasattr(args, key):
            setattr(args, key, defaults[key])
    return args


def list_input_choices():
    """List all input choices."""

    longest_name = ""
    input_choices = []
    for name in sorted(relval_steps.steps):
        if "INPUT" not in relval_steps.steps[name]: continue
        if len(name) > len(longest_name): longest_name = name
        input_choices.append((name, relval_steps.steps[name]["INPUT"].dataSet))

    print "Available inputs:"
    print "================="
    for name, dataset in input_choices:
            print name.ljust(len(longest_name)),"->", dataset


def override_in_all_steps(key, value):
    """Apply `value` in all relval steps in which `key` exists.

    Arguments:
    - `key`: key in step configuration
    - `value`: value to be associated to `key`
    """

    for _, step in relval_steps.steps.iteritems():
        if key in step: step[key] = value


def prepare_test_workflow(input_step, args):
    """Performs the required modifications on workflow 1000.

    Arguments:
    - `args`: command line arguments
    """

    workflow = 1000

    args.command = "-n {0:d}".format(args.number_of_events)
    args.testList = [workflow]

    override_in_all_steps("--conditions", args.global_tag)
    if "run2" in args.global_tag.lower():
        relval_steps.steps["TIER0"]["--customise"] = \
            "Configuration/DataProcessing/RecoTLR.customisePostEra_Run2_2016"
        relval_steps.steps["TIER0"]["--era"] = "Run2_2016"

    relval_production.workflows[workflow][1][0] = input_step
    remove_steps = ("SKIMD", "HARVESTDfst2") # these steps are not needed
    relval_production.workflows[workflow][1] = \
        filter(lambda x: x not in remove_steps,
               relval_production.workflows[workflow][1])

    return ("{0:.1f}_".format(workflow)+
            "+".join(relval_production.workflows[workflow][1][0:1]+
                     relval_production.workflows[workflow][1]))


def run_workflows(input_step, args):
    """Prepare and run the matrix test for a given `input_step`.

    Arguments:
    - `input_step`: input step defining the data to be processed
    - `args`: command line arguments
    """

    name = prepare_test_workflow(input_step, args)

    mrd = matrix_reader(args)
    mrd.prepare(useInput = None, refRel = None, fromScratch = None)

    mrunner = matrix_runner(mrd, args)
    if args.dryRun:
        mrd.show(selected = args.testList,
                 extended = True,
                 cafVeto = args.cafVeto)
    else:
        mrunner.runTests(args)

    return name


def submit_to_batch(name):
    """Submit the script to the batch.

    Arguments:
    - `name`: name of the batch job
    """

    # check first if proxy is set
    try:
        subprocess.check_call(["voms-proxy-info", "--exists"])
    except subprocess.CalledProcessError:
        print "Please initialize your proxy before submitting."
        sys.exit(1)

    local_proxy = subprocess.check_output(["voms-proxy-info", "--path"]).strip()

    try:
        index = sys.argv.index("-b")
    except ValueError:
        index = sys.argv.index("--batch")
    queue = sys.argv[index+1]
    del sys.argv[index:index+2]
    sys.argv[0] = os.path.realpath(sys.argv[0])

    os.makedirs(name)

    submit_proxy = os.path.join(name, os.path.basename(local_proxy))
    shutil.copyfile(local_proxy, submit_proxy)
    script = script_template.format(command = " ".join(sys.argv),
                                    user_proxy = os.path.realpath(submit_proxy),
                                    job_name = name,
                                    submit_dir = os.path.realpath(name),
                                    **os.environ)
    script_name = os.path.join(name, "submit.sh")
    with open(script_name, "w") as f: f.write(script)
    os.chmod(script_name, 0755)

    subprocess.call(["bsub",
                     "-q", queue,
                     "-J", name,
                     "-o", os.path.realpath(os.path.join(name, "submit.stdout")),
                     "-e", os.path.realpath(os.path.join(name, "submit.stderr")),
                     os.path.realpath(script_name)])
    sys.exit()

script_template="""\
#!/bin/sh

export X509_USER_PROXY={user_proxy}
cwd=$(pwd)
cd {CMSSW_BASE:s}/src
eval `scramv1 runtime -sh`
cd ${{cwd}}
{command:s}
cp -r {job_name}/* {submit_dir}/
"""


def get_unique_output_name():
    """Returns unique output name."""

    return "_".join(["ALCARECO", "throughput"]+time.ctime().split())

################################################################################
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
