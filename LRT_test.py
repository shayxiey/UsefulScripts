#### 用于进行似然比检验，辅助paml的codeml工作。也可直接使用检验单个脚本，只需去掉底部行的注释即可。

from scipy.stats import chi2  
import sys,os
import subprocess

def calculate_p_value(log_likelihood_1, log_likelihood_2, df1, df2):  
    likelihood_ratio = -2 * (log_likelihood_1 - log_likelihood_2)  
    p_value = chi2.sf(likelihood_ratio, df2 - df1)  
    return p_value  

def run_LRT(out_dir,op_file):
    outs = [file for file in os.listdir(out_dir)]
    result_l = ["gene\tlog_likelihood_H0\tdf_H0\tlog_likelihood_H1\tdf_H1\tLRT"]
    gene_l = []
    for file in outs:
        gene = file.split('.')[0]
        if gene not in gene_l:
            gene_l.append(gene)
        else:
            continue
        shell_command = f"grep lnL {os.path.join(out_dir,gene)}.*|awk -F':|)|"+"\\0' '{print $4,$6}' |awk '{print $1,$2}'" 
        op_lines = subprocess.run(shell_command, shell=True, check=True, stdout=subprocess.PIPE, text=True)
        data = op_lines.stdout.strip().split("\n")
        if len(data) >= 2:
            df1,log_likelihood_1 = data[0].split(' ')[0],data[0].split(' ')[1]
            df2,log_likelihood_2 = data[1].split(' ')[0],data[1].split(' ')[1]
        else:
            raise ValueError("Insufficient data returned from the command.")
        result = calculate_p_value(float(log_likelihood_1), float(log_likelihood_2), float(df1), float(df2))
        result_l.append('\t'.join([gene,log_likelihood_1, df1, log_likelihood_2, df2, str(result)]))
    return_data = '\n'.join(result_l)
    with open(op_file,'w')as fo:
        fo.write(return_data)
        
# log_likelihood_1 = float(sys.argv[1])
# df1 = float(sys.argv[2])
# log_likelihood_2 = float(sys.argv[3])
# df2 = float(sys.argv[4])
# test_dir = sys.argv[1]
# op_file = sys.argv[2]
# print(run_LRT(test_dir,op_file))
