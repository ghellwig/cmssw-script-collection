#!/usr/bin/env python

import os
import os.path as op
import shutil
import sys
import re
import argparse
import subprocess

if not os.environ.has_key("CMSSW_BASE"):
    print "Please source CMSSW environment first."
    sys.exit(1)


################################################################################
def main(argv=None):
    """
    Main routine. Not called, if this module is loaded via `import`.

    Arguments:
    - `argv`: Command line arguments passed to the script.
    """

    eos = "/afs/cern.ch/project/eos/installation/cms/bin/eos.select"
    conf_script_dir = op.join(os.environ["CMSSW_RELEASE_BASE"], "src",
                              "Configuration", "DataProcessing", "test")
    skimmer   = op.join(conf_script_dir, "RunAlcaSkimming.py")
    harvester = op.join(conf_script_dir, "RunAlcaHarvesting.py")
    mem_usage = op.abspath(op.join(op.dirname(__file__), "check_memory_usage.sh"))


    if argv == None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(description="Submit large scale PCL check.")
    parser.add_argument("--global-tag", dest = "global_tag", metavar = "TAG",
                        default = "80X_dataRun2_Express_v7",
                        help = ("global tag to be used for the check "
                                "(default: %(default)s)"))
    parser.add_argument("--skim", default = "PromptCalibProdSiPixelAli",
                        help = ("name of the skim to be produced "
                                "(default: %(default)s)"))
    parser.add_argument("--era", default = "ppEra_Run2_2016",
                        help = "scenario to be used (default: %(default)s)")
    parser.add_argument("name", nargs=1, help = "name of the check")
    parser.add_argument("file_list", nargs=1,
                        help = "text file with list of input files")
    parser.add_argument("-f", "--force", action = "store_true", default = False,
                        help = "force overwrite of previous results")
    args = parser.parse_args(argv)

    name = args.name[0]

    cms_dir = op.join(op.expandvars("/store/caf/user/${USER}/CHECK_PCL"), name)
    eos_dir = "/eos/cms" + cms_dir
    sub_dir = op.join(os.getcwd(), name)

    d = {"skimmer": skimmer,
         "harvester": harvester,
         "era": args.era,
         "skim": args.skim,
         "global_tag": args.global_tag,
         "sub_dir": sub_dir,
         "eos_dir": eos_dir,
         "eos": eos,
         "mem_usage": mem_usage,
         "CMSSW_BASE": os.environ["CMSSW_BASE"],
         }

    try:
        os.makedirs(name)
    except OSError as e:
        if e.args == (17, 'File exists'):
            if args.force:
                shutil.rmtree(name)
                os.makedirs(name)
            else:
                print "Already performed a check with name '"+name+"'."
                sys.exit(1)
        else:
            raise

    skim_files = []
    skim_scripts = []
    with open(args.file_list[0], "r") as input_files:
        for i, input_file in enumerate(input_files):
            script_name = op.join(sub_dir, "RunAlcaSkimmingCfg_"+str(i)+".sh")
            with open(script_name, "w") as script:
                script.write(skim_template.format(file = input_file,
                                                  count = i,
                                                  **d))
            os.chmod(script_name, 0755)
            skim_scripts.append(script_name)
            skim_files.append(op.join(cms_dir, args.skim+"_"+str(i)+".root"))

    script_name = op.join(sub_dir, "RunAlcaHarvestingCfg.sh")
    with open(script_name, "w") as script:
        script.write(harvest_template.format(**d))
    os.chmod(script_name, 0755)

    subprocess.call(["python", harvester,
                     "--scenario", args.era,
                     "--global-tag", args.global_tag,
                     "--workflows", "SiPixelAli",
                     "--dataset", "/Just/A/dummyDQM",
                     "--lfn", "file:dummy.root"], cwd = sub_dir)

    sys.path.append(sub_dir) # dirty hack to make RunAlcaHarvestingCfg visible
    import RunAlcaHarvestingCfg
    del RunAlcaHarvestingCfg.process.source.fileNames[:] # remove dummy file
    for skim_file in skim_files:
        RunAlcaHarvestingCfg.process.source.fileNames.append(skim_file)
    # overwrite original config:
    with open(op.join(sub_dir, "RunAlcaHarvestingCfg.py"),"w") as f:
        f.write(RunAlcaHarvestingCfg.process.dumpPython())


    # submit all scripts
    subprocess.call([eos, "mkdir", "-p", eos_dir])
    regex = re.compile(r".+<([0-9]{9})>.+")
    job_ids = []
    for script in skim_scripts:
        job_name = op.split(script)[-1]
        cmd = ["bsub",
               "-J", op.splitext(job_name)[0] + "_" + name,
               "-o", op.splitext(job_name)[0] + ".stdout",
               "-e", op.splitext(job_name)[0] + ".stderr",
               "-q", "cmscaf1nh",
               job_name]
        print " ".join(cmd)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, cwd = sub_dir)
        out,_ = p.communicate()
        print out,
        result = regex.search(out)
        job_ids.append(result.group(1))

    job_name = "RunAlcaHarvestingCfg.sh"
    cmd = ["bsub",
           "-J", op.splitext(job_name)[0] + "_" + name,
           "-o", op.splitext(job_name)[0] + ".stdout",
           "-e", op.splitext(job_name)[0] + ".stderr",
           "-q", "cmscaf1nw",
           "-w", " && ".join(["ended("+i+")" for i in job_ids]),
           job_name]
    print " ".join(cmd)
    subprocess.call(cmd, cwd = sub_dir)



################################################################################
skim_template=r"""
#!/bin/zsh
cwd=$(pwd)
cd {CMSSW_BASE:s}/src
eval `scramv1 runtime -sh`
cd ${{cwd}}
python {skimmer:s} --scenario {era:s} --skims {skim:s} --global-tag {global_tag:s} --lfn {file:s}
sed -i "s/\({skim:s}\)\(\.root\)/\1_{count:d}\2/" RunAlcaSkimmingCfg.py
sed -i "s/\(input = cms.untracked.int32(\)10\()\)/\1 -1 \2/" RunAlcaSkimmingCfg.py
sed -i "s/\(process = cms.Process(\"\)ALCA\(\")\)/\1ALCATEST\2/" RunAlcaSkimmingCfg.py
mv RunAlcaSkimmingCfg.py {sub_dir:s}/RunAlcaSkimmingCfg_{count:d}.py
cmsRun {sub_dir:s}/RunAlcaSkimmingCfg_{count:d}.py
{eos:s} cp {skim:s}_{count:d}.root {eos_dir:s}/
ls -l
"""

harvest_template=r"""
#!/bin/zsh
cwd=$(pwd)
cd {CMSSW_BASE:s}/src
eval `scramv1 runtime -sh`
cd ${{cwd}}
cmsRun {sub_dir:s}/RunAlcaHarvestingCfg.py &
{mem_usage:s} 5 cmsRun {sub_dir:s}/pede_memory_report.txt
ls -l
gzip *.dat *.txt *.db
for f in $(ls --color=never *.gz)
do
    {eos:s} cp ${{f}} {eos_dir:s}/
done
ls -l
"""


################################################################################
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
