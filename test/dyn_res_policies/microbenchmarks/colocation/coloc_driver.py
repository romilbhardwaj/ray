# this script runs the co-location script with variable configurations to get the data for the plots
import subprocess

sizes_to_try = [100000, 1000000, 10000000]#, 100000000]   # 0.1 MiB, 1 MiB, 10 MiB, 100 MiB
flags_to_try = ["", "--locality-aware"]  # first is random placement

for size in sizes_to_try:
    for flag in flags_to_try:
        command = "python coloc_benchmark.py --bytes-big {} {}".format(size, flag)
        print("Running cmd: {}".format(command))
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        process.wait()
        print(process.returncode)
        # process = subprocess.Popen("rm -rf /tmp/*", shell=True, stdout=subprocess.PIPE)
        # process.wait()