import os,pdb
import argparse
class orthologs:
    def __init__(self,Chr,Pos,Len,Alns):
        self.chr = Chr
        self.pos = Pos
        self.len = Len
        self.alns = Alns
      
def record_maf_optimized(fi, opath):
    last_orth = None
    for block in read_maf_block(fi):
        current_orth = process_single_block(block)
        # 判断是否与上一个orthologs连续
        if last_orth and last_orth.chr == current_orth.chr and last_orth.pos + last_orth.len == current_orth.pos:
            # 数据块连续，合并current_orth到last_orth
            for key, seq in current_orth.alns.items():
                if key in last_orth.alns:
                    last_orth.alns[key] += seq
                else:
                    fill_length = last_orth.len
                    last_orth.alns[key] = '-' * fill_length + seq
            last_orth.len += current_orth.len
            
             # 然后检查last_orth中是否存在current_orth没有的键，并进行填充
            for key in last_orth.alns.keys():
                if key not in current_orth.alns:
                    fill_length = current_orth.len
                    last_orth.alns[key] += '-' * fill_length
        else:
            # 数据块不连续，输出last_orth（如果存在），然后将current_orth设为新的last_orth
            if last_orth:
                write_orth(opath, last_orth)
            last_orth = current_orth

    # 处理完所有数据块后，检查是否还有未输出的last_orth
    if last_orth:
        write_orth(opath, last_orth)
      
def read_maf_block(fi):
    # 生成器,逐块读取maf文件,忽略#注释行，每块从'a'行开始，到下一个'a'行或文件结束
    block_lines = []
    for line in fi:
        stripped_line = line.strip()
        # 跳过空行和注释行
        if not stripped_line or stripped_line.startswith('#'):
            continue
        if stripped_line.startswith('a'):
            if block_lines:  # 如果已有数据块，则先处理并清空
                yield block_lines
                block_lines = []
        block_lines.append(stripped_line)
    if block_lines:  # 处理
        yield block_lines
def process_single_block(block_data):
    # 处理单个数据块,生成并写入orthologs对象
    chr, pos, lens, dic = '', 0, 0, {}
    if block_data and block_data[0].startswith('a'):
        block_data.pop(0)  # 移除首个'a'行
        if block_data:  # 确保还有数据才继续
            first_s_line = block_data.pop(0)  # 获取首个's'行
            parts = first_s_line.split('\t')
            chr = parts[1].split('.')[1]
            pos = int(parts[2])
            lens = int(parts[3])
            key = parts[1].split('.')[0]
            seq = parts[6].strip('\n')
            dic[key] = seq  # 初始化dic
    for line in block_data:
        if line.startswith('s'):
            parts = line.split('\t')
            key = parts[1].split('.')[0]
            seq = parts[6].strip('\n')
            dic[key] = seq
    
    orth = orthologs(chr, pos, lens, dic)
    return orth if chr else None
    # 处理完一个块后，检查是否有未写入的orthologs对象
def write_orth(path,orth):
    filename = f"{orth.chr}.{str(orth.pos)}_{str(orth.pos+orth.len-1)}.fasta"
    opath = os.path.join(path,filename)
    with open(opath,'w')as fo:
        for key,value in orth.alns.items():
            fo.write(f">{key}\n")
            fo.write(f"{value}\n")
            
parser = argparse.ArgumentParser(description='''\
This program is used to convert MAF file to Fasta file.
If multiple blocks recorded in MAF file, the output will be splited.
Example:
python3 maf2fa.py -i maf_file -o fasta_dir
-------------------------------------------------
Written by Yulong Xie, 2024/06/13 contact at xieyulong@zju.edu.cn

''', formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-i', '--InputMAFFile', type=str,help="Your MAF file.")
parser.add_argument('-o', '--OutputPath', type=str,help="Which dir to save your splited output files.")

args = vars(parser.parse_args())
maf_file = args['InputMAFFile']
op_dir = args['OutputPath']
if not os.path.exists(op_dir):
    os.makedirs(op_dir,0o755)

with open(maf_file,'r')as fi:
    record_maf_optimized(fi,op_dir)
