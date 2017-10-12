#!/usr/bin/python34
#
# http://docs.aws.amazon.com/emr/latest/ReleaseGuide/UseCase_Streaming.html

import sys
if sys.version < "3.4":
    raise RuntimeError("q2_python.py requires Pyton 3.4 or above")

import os,os.path
import time
import datetime
import validator
import subprocess
import platform

def plot(results):
    png_fname = results.replace(".txt","plot.png")
    pdf_fname = results.replace(".txt","plot.pdf")
    print("*** Simson needs to finish writing this code! ***")
    exit(1)


def hadoop_nodes():
    """Return how many hadoop nodes we have"""
    import re
    (out,err) = subprocess.Popen(["yarn","node","-list"],stdout=subprocess.PIPE,stderr=subprocess.PIPE).communicate()
    m = re.search("Total Nodes:([0-9]+)",out.decode('utf-8'))
    if m:
        return int(m.group(1))
    return None
    

if __name__=="__main__":
    if "/" in __file__ or "\\" in __file__:
        print("Please run the python script from the A2 directory")
        exit(1)

    results = "results.py"
    if "python.py" in __file__:
        results   = __file__.replace("python.py","results.txt")

    # Generate an output that will probably be unique
    # Note that we can't have a ":" in a filename, so the ":" is changed to a "-"
    default_output="output-"+datetime.datetime.fromtimestamp(int(time.time())).isoformat().replace(":","-")

    import argparse
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--plot',action='store_true',help='Generate plot')
    parser.add_argument('--output',help='output location',default=default_output)
    parser.add_argument('--input',help='input location',default='s3://gu-anly502/A2/sonnet18.txt')
    parser.add_argument('--mapper',help='mapper',default='q2_mapper.py')
    parser.add_argument('--reducer',help='reducer',default='q2_reducer.py')
    parser.add_argument('--streaming_jar',help='Specifies name of Hadoop streaming jar',default='/usr/lib/hadoop/hadoop-streaming-2.7.3-amzn-1.jar')
    
    args = parser.parse_args()
    if args.plot:
        try:
            import matplotlib
        except ImportError as e:
            print("Plotting requires matplotlib.")
            exit(1)
        plot(results)

    # Validate the mapper and the reducer
    for fn in [args.mapper,args.reducer]:
        if not os.path.exists(fn):
            print("{} does not exist!".format(fn))
            exit(1)
        if not validator.validate_pyfile(fn):
            print("{} does not compile".format(fn))
            exit(1)
            
              
    # Make sure we have a jar file!
    if not os.path.exists(args.streaming_jar):
        print("{} does not exist".format(args.streaming_jar))
        exit(1)


    print("Running with fname")
    t0 = time.time()
    files = [args.mapper,args.reducer]
    cmd = ['hadoop',
           'jar',args.streaming_jar,
           '-files',",".join(files), # generic argument, must come before -mapper and -reducer
           '-input',args.input,
           '-output',args.output,
           '-mapper',args.mapper,
           '-reducer',args.reducer]
    print(" ".join(cmd))
    subprocess.call(cmd)
    t1 = time.time()
    print("Execution clock time: {}".format(t1-t0))
    # Write the results to the output file
    
    with open(results,"a") as f:
        import json
        f.write(json.dumps({"seconds":t1-t0,
                            "nodes":hadoop_nodes(),
                            "date":time.time(),
                            "node":platform.uname().node,
                            "input:":args.input,
                            "output":args.output
                        }))
        f.write("\n")
    
    print("To see the results, use this command:")
    print("hdfs dfs -cat {}/*".format(args.output))
    
