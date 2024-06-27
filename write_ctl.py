import os,sys
print("Remember put in which dir to save output.")
# 定义基础目录
base_dir = '/share/home/zhanglab/user/xieyulong/00.data/02.discordance/cds/alns/'
spls_dir = '/share/home/zhanglab/user/xieyulong/00.data/02.discordance/cds/sp_lsts/'
trees_dir = '/share/home/zhanglab/user/xieyulong/00.data/02.discordance/cds/trees/'
op_path = sys.argv[1]
# 遍历子文件夹
for subdir in os.listdir(base_dir):
    if os.path.isdir(os.path.join(base_dir, subdir)):
        ctl_file_path = os.path.join(op_path, f"{subdir}.ctl")
        with open(ctl_file_path, 'w') as ctl_file:
          
            # gene部分
            ctl_file.write("gene:\n")
            for filename in os.listdir(os.path.join(base_dir, subdir)):
                if os.path.isfile(os.path.join(base_dir, subdir, filename)):
                    ctl_file.write(f"  {os.path.join(base_dir, subdir, filename)}\n")
            
            # namelist部分
            ctl_file.write("\nnamelist:\n")
            namelist_file = os.path.join(spls_dir, f"{subdir}.splst")
            ctl_file.write(f"  {namelist_file}\n")
            
            # tree部分
            ctl_file.write("\ntree:\n")
            tree_file = os.path.join(trees_dir, f"{subdir}.mapping.new")
            ctl_file.write(f"  {tree_file}\n")

print("CTL files have been generated.")
