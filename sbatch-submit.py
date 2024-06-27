#### 使用前，请修改第37行的sbatch日志存储位置。
"""
Version : v1
Time    : 2022.10.31
Author  : gaorongsheng
editor  : xieyulong
"""

import argparse
import os
import re
import time

parser = argparse.ArgumentParser(description='''\
This program submit the jobs and control them running on slurm system.

Example:
python sbatch-submit.py -l 1 --mem_per_cpu 5G --job_name array_task -w NODE20,NODE23 your_script

''', formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument("filename", type=str, help="script file")
parser.add_argument('-l','--lines', type=int, default=200, help="split the script into n lines one")
parser.add_argument('-n','--ntasks', type=int, default=1, help="number of tasks to run")
parser.add_argument('-N','--nodes', type=int, default=1, help="number of nodes on which to run (N = min[-max])")
parser.add_argument('-c','--cpus_per_task', type=int, default=1, help="number of cpus required per task")
parser.add_argument('--mem_per_cpu', metavar='MEM_PER_CPU[M|G]', type=str, default="2.5G", help="maximum amount of real memory per allocated\ncpu required by the job.\n--mem >= --mem-per-cpu if --mem is specified.")
parser.add_argument('--job_name', type=str, default="mega_array", help="job name")
parser.add_argument('--prefix', type=str, default="task", help="split script file name prefix")
parser.add_argument('-w','--nodelist', type=str, help="request a specific list of hosts; e.g. --nodelist=NODE1,NODE3")
parser.add_argument('--mem', metavar='MEM[M|G]', type=str, help="minimum amount of real memory")
parser.add_argument('--mail_type', type=str, choices=["NONE", "BEGIN", "END", "FAIL", "ALL"],help="notify on state change: BEGIN, END, FAIL or ALL")
parser.add_argument('--mail_user', type=str, help="who to send email notification for job state changes")
parser.add_argument('-p','--partition', type=str,default='cpu64',help="cpu or a10 or a100 or fat")

args = vars(parser.parse_args())

SUBMIT_PATH = '/share/home/zhanglab/user/xieyulong/00.data/02.discordance/cds/submits/'
pid = os.getpid()
submit_dir = os.path.join(SUBMIT_PATH,f"{args['filename'].split('/')[-1]}.{pid}.submit")
if not os.path.exists(submit_dir):
    os.makedirs(submit_dir)

sbatch_file = os.path.join(submit_dir,f"{pid}.sbatch")
sbatch_log = os.path.join(submit_dir,f"{pid}.sbatch.log")

with open(sbatch_log,'a') as f:
    f.write("start to make the submit shell files...\n")

n_script = 1
lines_count = 0
script_tmp = ""
script_file_list = []
with open(args['filename'],'r') as f:
    for line in f.readlines():
        line = line.strip()
        script_tmp += f"{line.strip(';')} && echo This-task-completed!\n"
        lines_count += 1
        if lines_count==args['lines']:
            script_file_name = os.path.join(submit_dir,f"{args['prefix']}.{n_script}.sh")
            with open(script_file_name,'w') as wf:
                wf.write(script_tmp)
            lines_count = 0
            n_script += 1
            script_tmp = ""
    if script_tmp:
        script_file_name = os.path.join(submit_dir,f"{args['prefix']}.{n_script}.sh")
        with open(script_file_name,'w') as wf:
            wf.write(script_tmp)
    else:
        n_script -= 1
with open(sbatch_log,'a') as f:
    f.write("make the submit shell files done!\n")
    f.write("start to make the sbatch file...\n")

sbatch_list = f"{pid}.sbatch.list"
sbatch_log = f"{pid}.sbatch.log"

with open(sbatch_file,'w') as f:
    f.write(f"#!/bin/sh\n")
    f.write(f"#SBATCH --job-name={args['job_name']}\n")
    f.write(f"#SBATCH --nodes={args['nodes']}\n")
    f.write(f"#SBATCH --ntasks={args['ntasks']}\n")
    f.write(f"#SBATCH --cpus-per-task={args['cpus_per_task']}\n")
    if re.search(r'(0.\d+)[gG]$',args['mem_per_cpu']):
        mem = float(re.search(r'(0.\d+)[gG]$',args['mem_per_cpu']).group(1))
        mem *=1000
        args['mem_per_cpu'] = f'{int(mem)}M'
    f.write(f"#SBATCH --mem-per-cpu={args['mem_per_cpu']}\n")
    f.write(f"#SBATCH --array=1-{n_script}\n")
    f.write(f"#SBATCH --output={args['prefix']}.%a_%A.out\n")
    f.write(f"#SBATCH --error={args['prefix']}.%a_%A.error\n")
    f.write(f"#SBATCH --partition={args['partition']}\n")
    if args['nodelist']:
        f.write(f"#SBATCH --nodelist={args['nodelist']}\n")
    if args['mem']:
        f.write(f"#SBATCH --mem={args['mem']}\n")
    if args['mail_type']:
        f.write(f"#SBATCH --mail-type={args['mail_type']}\n")
    if args['mail_user']:
        f.write(f"#SBATCH --mail-user={args['mail_user']}\n")
    f.write(f"\n#run\n")
    f.write(f"date\n")
    f.write(f"echo $SLURM_JOB_ID\t$SLURM_ARRAY_JOB_ID\t$SLURM_ARRAY_TASK_ID >> {sbatch_list}\n")
    f.write(f"sh {args['prefix']}.$SLURM_ARRAY_TASK_ID.sh\n")
    f.write(f"date")

os.chdir(submit_dir)
with open(sbatch_log,'a') as f:
    f.write("make the sbatch file done!\n")
    f.write("start to run the sbatch file...\n")

command = f"sbatch {pid}.sbatch"
with os.popen(command, "r") as p:
    re = p.read()
    with open(sbatch_log,'a') as f:
        f.write(f"{re}\n")
#print(command)

# check array task
def sacct_job(job_id):
    command = f"sacct -o State -j {job_id} | tail -n 1"
    with os.popen(command, "r") as p:
        state = p.read()
        state = state.strip()
    return state

job_id_dict = {}
get_id_loop = 0
time.sleep(10)
while(len(job_id_dict)!=n_script and get_id_loop<60):
    get_id_loop += 1
    time.sleep(10)
    if os.path.exists(sbatch_list):
        with open (sbatch_list,'r') as f:
            for line in f.readlines():
                line_split = line.strip().split()
                job_id_dict[line_split[0]]=f"{line_split[1]}_{line_split[2]}"

if get_id_loop >= 60:
    with open(sbatch_log,'a') as f:
        f.write("get job id error, cannot monitor the array task\n")
else:
    undone_id_list = list(job_id_dict.keys())
    cant_get_id_dict = {}
    time.sleep(10)
    while(undone_id_list):
        for job_id in undone_id_list:
            try:
                job_state = sacct_job(job_id)
            except:
                if job_id not in cant_get_id_dict:
                    cant_get_id_dict[job_id] = 1
                else:
                    cant_get_id_dict[job_id] +=1
                if cant_get_id_dict[job_id] > 4:
                    with open(sbatch_log,'a') as f:
                        f.write(f"error:can't get state of job_id\t{job_id}\tarray_id\t{job_id_dict[job_id]}\n")
                        undone_id_list.remove(job_id)
            else:
                if job_state == 'COMPLETED':
                    with open(sbatch_log,'a') as f:
                        f.write(f"job_id\t{job_id}\tarray_id\t{job_id_dict[job_id]}\tCOMPLETED!\n")
                    undone_id_list.remove(job_id)
                elif job_state in ['PENDING','RUNNING','CONFIGURING','COMPLETING']:
                    continue
                elif job_state in ['CANCELLED','FAILED','TIMEOUT','NODE FAILURE','SPECIAL EXIT STATE']:
                    with open(sbatch_log,'a') as f:
                        f.write(f"error:job_id\t{job_id}\tarray_id\t{job_id_dict[job_id]}\t{job_state}!\n")
                    undone_id_list.remove(job_id)
                else:
                    with open(sbatch_log,'a') as f:
                        f.write(f"error:job_id\t{job_id}\tarray_id\t{job_id_dict[job_id]}\t{job_state}!\n")
                    undone_id_list.remove(job_id)
        if undone_id_list:
            time.sleep(10)
        else:
            break
    with open(sbatch_log,'a') as f:      
        f.write("all task done!\n")
    
