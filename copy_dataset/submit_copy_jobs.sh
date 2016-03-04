#!/bin/bash

dataset="/TKCosmics_38T/Cosmic70DR-TkAlCosmics0T-Peak_Opti_COSM70_PEA_V2-v1/ALCARECO"
queue="cmsexpress"

################################################################################
if [ x${CMSSW_BASE} = 'x' ]
then
    echo "Please source a CMSSW environment."
    exit 1
fi

################################################################################
eos=/afs/cern.ch/project/eos/installation/cms/bin/eos.select

out_name=$(echo ${dataset} | sed 's|^/||;s|/ALCARECO||;s|/|_|g;')

submit_dir="submit"
current_dir=$(pwd -P)
eos_dir=/eos/cms/store/caf/user/${USER}/Datasets/${out_name}/

rm -rf ${submit_dir}
mkdir ${submit_dir}
${eos} mkdir -p ${eos_dir}

input_files=$(das_client --limit=0 --query="file dataset=${dataset}")

cd ${submit_dir}
count=1
for input in ${input_files}
do
    formatted_count=$(printf %04d ${count})
    output=${out_name}_${formatted_count}.root
    script_name=copy_dataset_${formatted_count}.sh
    echo '#!/bin/bash' > ${script_name}
    echo "eos=${eos}" >> ${script_name}
    echo 'CWD=$(pwd -P)' >> ${script_name}
    echo "cd ${CMSSW_BASE}/src" >> ${script_name}
    echo 'eval `scramv1 ru -sh`' >> ${script_name}
    echo 'cd ${CWD}' >> ${script_name}
    echo 'echo ${CWD}' >> ${script_name}
    echo "cp $(readlink -e ../copy_dataset.py) ." >> ${script_name}
    echo "cmsRun copy_dataset.py inputFiles=${input} outputFile=${output}" >> ${script_name}
    echo "${eos} cp ${output} ${eos_dir}" >> ${script_name}
    chmod +x ${script_name}
    bsub -q ${queue} ${script_name}
    count=$((count + 1))
done
cd ${current_dir}
