#!/usr/bin/python3.5
#
# generate the streaming time report
import subprocess
import datetime

#command line execute
def exe_command(command):
    result = None
    try:
        result = subprocess.check_output(command, shell=True)
    except:
        pass
    return result

if __name__=="__main__":
    import argparse,os

    parser = argparse.ArgumentParser()
    parser.add_argument("s3",help="S3 URL")
    parser.add_argument("output",help="Output filename")
    
    args = parser.parse_args()

    #instance type
    inst_type = exe_command("ec2-metadata -t")
    #output ex. instance type:  t2.nano 
    #we have to delete the text before instance type
    inst_type = inst_type.decode("utf-8") #decode from unitext
    inst_type = inst_type[inst_type.index(":") + 2 : 100 ] #keep the instance type
    
    start = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=-5)))
    mal_count = exe_command("aws s3 cp {} - | grep 'fnard:-1 fnok:-1 cark:-1 gnuck:-1' -c".format(args.s3))
    end = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=-5)))
    time_consume = (end - start).seconds

    if os.path.exists(args.output):
        raise RuntimeError("{} exists".format(args.output))
    
    # Create the output file
    with open(args.output,"w") as f:
        f.write("# streaming to a {}".format(inst_type))
        f.write("date: {}\n".format(start.strftime("%Y-%m-%dT%H:%M:%S")))
        f.write("model: streaming\n")
        f.write("streaming: {}\n".format(time_consume))
        f.write("malfunctions: {}".format(mal_count.decode("utf-8")))

    
