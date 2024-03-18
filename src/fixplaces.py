#!/usr/bin/env python
import os
import subprocess
HOME=os.environ["HOME"]
WD=os.path.join(HOME,"repos/dialectic-twitter-maps-generator") 
DATA_DIR=os.path.join(WD,'data')
DATA_DIR_SUBDIRS=os.listdir(DATA_DIR)
FIX_PLACES_SCRIPT=os.path.join(WD,"src/cl-fixfile.py")
def execute_fixfile_script(file_path):
args=["python3", FIX_PLACES_SCRIPT, file_path]
try:
    with subprocess.Popen(args, stdout=subprocess.PIPE, shell=False) as process:
        outs, errs = process.communicate()
        ret_code=process.returncode
        out_str=outs.decode("utf-8")
        output_lines = out_str.split('\n')
        if ret_code == 0:
            print("cl-fixfile script loaded...")
            for out in output_lines:
                print(out)
        elif ret_code == 1:
            print("File already fixed, skiping...")
            for out in output_lines:
                print(out)
        elif ret_code ==2:
            for out in output_lines:
                print(out)
except OSError as e:
    print(f"Execution failed: {e}")

for dir in DATA_DIR_SUBDIRS:
if os.path.basename(dir) != "Extra":
    sub_dir=os.path.join(DATA_DIR,dir)
    files=os.listdir(sub_dir)
    print(f"Processing contents in {dir}...")
    for file in files:
        print(f"Fixing {file} data...")
        file_path=os.path.join(sub_dir,file)
            execute_fixfile_script(file_path)
