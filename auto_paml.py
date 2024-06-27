######  该脚本用于将你已经提取好的基因群批量进行paml的codeml计算， 请配合LRT_test.py和sbatch-submit.py使用。  ######
#### 使用前，请检查三个脚本的文件存储位置是否正确。需要提供codeml模版的绝对路径。 ####
#### 如果有任何问题，请联系 xieyulong@zju.edu.cn ####
import os
import subprocess
import multiprocessing
#### 因为multiprocessing会复制整个进程，会造成额外的内存浪费，因此不建议使用该包创建太多进程，实测10个可用，限制在5个以内最好 ####
import psutil
from LRT_test import calculate_p_value
import argparse
import pdb

def until_files(path,list):
    dir_tl = os.listdir(path)
    for item in dir_tl:
        f_item = os.path.join(path,item)
        if os.path.isfile(f_item):
            list.append(f_item)
        else:
            list = until_files(f_item,list)
    return list

def read_ctl(file):
    dic = {}
    for line in file:
        line = line.strip()
        if not line: 
            continue
        if line.endswith(':'): #三要素：gene,namelist,tree
            current_key = line[:-1]  # 移除冒号
            dic[current_key] = []  # 初始化列表用于存储该关键词的内容
        else:
            # 如果当前有有效的关键词，则将内容添加到对应的列表中
            if current_key is not None:
                dic[current_key].append(line)
    return dic

def del_files(dir_path):
    for root, dirs, files in os.walk(dir_path, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))

def muilt_sub(lst):
    pro = subprocess.Popen(lst)
    pro.communicate()
            
def modify_ctl(ctl_template,seqfile,treefile,wz,node):
    def modifer(keyword,seq,lst):
        for i, entry in enumerate(lst):
            index = entry.find(keyword)
            if index != -1:
                lst[i] = f"{keyword} {seq}"  # 替换为新值
                return lst
    opath = os.path.join(wz,'out',node)
    gene = os.path.basename(seqfile).split('_')[1].split('.')[0]
    modified_ctl = modifer("treefile = ",treefile[0],ctl_template)
    modified_ctl = modifer("seqfile = ",seqfile,ctl_template)
    return_ctl0 = modifer("model = ",'model = 0',modified_ctl)
    return_ctl0 = modifer("outfile = ",os.path.join(opath,f"{gene}.{node}.0.out"),return_ctl0)
    ctl0 = '\n'.join(return_ctl0)
    return_ctl2 = modifer("model = ",'model = 2',modified_ctl)
    return_ctl2 = modifer("outfile = ",os.path.join(opath,f"{gene}.{node}.2.out"),return_ctl2)
    ctl2 = '\n'.join(return_ctl2)
    return (ctl0,ctl2)

def write_paml_shell(para_dic,ctl_template,wz): #传入参数字典和ctl文件模版以及工作区地址
    shell_l = []
    for node, paras in para_dic.items(): #假设前期已经完成了基因筛选工作，那么我们只需要调整ctl文件并检查输入是否完整
        # 写入paml ctl文件
        for gene_p in paras[0]:
            if not os.path.exists(gene_p):
                continue
            gene = os.path.basename(gene_p).split('_')[1].split('.')[0]
            ctl0,ctl2 = modify_ctl(ctl_template,gene_p,paras[2],wz,node)
            # pdb.set_trace()
            op_dir = os.path.join(wz,'ctls',node)
            if not os.path.exists(op_dir):
                os.makedirs(op_dir,mode=0o755, exist_ok=True)
            op_path = os.path.join(op_dir,f"{gene}.0.ctl")
            with open(op_path,'w')as fo:
                fo.write(ctl0)
            shell_l.append(f"/share/home/zhanglab/user/xieyulong/software/paml-4.10.7/bin/codeml {op_path}")
            op_path = os.path.join(op_dir,f"{gene}.2.ctl")
            with open(op_path,'w')as fo:
                fo.write(ctl2)
            shell_l.append(f"/share/home/zhanglab/user/xieyulong/software/paml-4.10.7/bin/codeml {op_path}")
        # pdb.set_trace()
        with open(os.path.join(wz,"shells",f"{node}.sh"),'w')as fo:
            fo.write('\n'.join(shell_l))
        sba_l.append(os.path.join(wz,"shells",f"{node}.sh"))

def run_LRT(out_dir,op_file,node):
    outs = [file for file in os.listdir(out_dir)]
    result_l = ["gene\tlog_likelihood_H0\tdf_H0\tlog_likelihood_H1\tdf_H1\tLRT\tAcptHypo"]
    gene_l = []
    for file in outs:
        gene = file.split('.')[0]
        if gene not in gene_l:
            gene_l.append(gene)
        else:
            continue
        shell_command = f"grep lnL {os.path.join(out_dir,node,gene)}.*|awk -F':|)|"+"\\0' '{print $4,$6}' |awk '{print $1,$2}'" 
        op_lines = subprocess.run(shell_command, shell=True, check=True, stdout=subprocess.PIPE, text=True)
        data = op_lines.stdout.strip().split("\n")
        if len(data) >= 2:
            df1,log_likelihood_1 = data[0].split(' ')[0],data[0].split(' ')[1]
            df2,log_likelihood_2 = data[1].split(' ')[0],data[1].split(' ')[1]
        else:
            raise ValueError("Insufficient data returned from the command.")
        result = calculate_p_value(float(log_likelihood_1), float(log_likelihood_2), float(df1), float(df2))
        if result>0.05:
            AcptHypo = 'H1'
        else:
            AcptHypo = 'H0'
        result_l.append('\t'.join([gene,log_likelihood_1, df1, log_likelihood_2, df2, str(result),AcptHypo]))
    return_data = '\n'.join(result_l)
    with open(op_file,'w')as fo:
        fo.write(return_data)


parser = argparse.ArgumentParser(description='''\
This program is used to auto run PAML codeml for neutual selection.

Example:
python3 auto_paml.py -i fasta_file -b bed_file -o op_dir 

''', formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-i', '--ctlDir', type=str,help=f"Your ctlDir, including wanted species,gene_list,example:...")
parser.add_argument('-w', '--workingZone', type=str,help="where to work and save temporary files.")
# parser.add_argument('-o', '--OutputPath', type=str,help="Which dir to save your splited output files.")

# 如果批量运行，需要节点的名单，每个节点的物种名单，节点的基因名单；
# 提取需要物种的基因组，需要fasta地址，物种名单（与节点对应）；

args = vars(parser.parse_args())
ctl_dir = args['ctlDir']
wz = args["workingZone"]

print("Start initiating...")
ctl_l = [os.path.join(ctl_dir,file) for file in os.listdir(ctl_dir)]
if not os.path.exists(wz):
    os.mkdir(wz)
os.chdir(wz)
if not os.path.exists(f"{wz}/ctls"):
    os.mkdir("ctls")
if not os.path.exists(f"{wz}/shells"):
    os.mkdir("shells")
out_dir = os.path.join(wz,'out')
if not os.path.exists(out_dir):
    os.mkdir(out_dir)
if os.path.exists("/share/home/zhanglab/user/xieyulong/00.data/02.discordance/cds/submits/"):
    del_files("/share/home/zhanglab/user/xieyulong/00.data/02.discordance/cds/submits/")
# 把基因表和物种名单存入字典
para_dic = {}
for file in ctl_l:
    base_unit = os.path.basename(file).split('.')[0]
    with open(file,'r')as fi:
        single_dic = read_ctl(fi)
        para_dic[base_unit] = (single_dic['gene'],single_dic['namelist'],single_dic['tree'])
    fi.close()
    if not os.path.exists(os.path.join(out_dir,base_unit)):
        os.mkdir(os.path.join(out_dir,base_unit))

basic_ctlf = '/share/home/zhanglab/user/xieyulong/00.data/02.discordance/cds/paml_ctls/codeml.ctl'
ctl_template = open(basic_ctlf,'r').read().splitlines()
print(f"totally {len(para_dic)} nodes. Start writing shells for nodes...")
## 开始对节点进行处理
sba_l = []
write_paml_shell(para_dic,ctl_template,wz)
# 将shell文件汇总并提交
l_popen_l = []
print(u'multiprocessing前进程的内存使用：%.4f GB' % (psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024 / 1024) )
# multiprocessing.set_start_method('spawn')
po = multiprocessing.Pool(4)
for sba_f in sba_l:
    popen_l = ['python3','/share/home/zhanglab/user/xieyulong/00.data/02.discordance/cds/99.code/sbatch-submit.py'
               ,'-l','10','--mem_per_cpu','400M','--job_name',f'{os.path.basename(sba_f).split(".")[0]}',f'{sba_f}']
    l_popen_l.append(popen_l)
po.map(muilt_sub,l_popen_l)
po.close()
po.join()
# 运行完以后，执行LR检验，存入csv输出表

if not os.path.exists(os.path.join(wz,'LRTresult')):
    os.mkdir(os.path.join(wz,'LRTresult'))
for node in os.listdir(out_dir):
    out_dirp = os.path.join(out_dir,node)
    csv_path = os.path.join(os.path.join(wz,'LRTresult',f'{node}.csv'))
    run_LRT(out_dirp,csv_path,node)
