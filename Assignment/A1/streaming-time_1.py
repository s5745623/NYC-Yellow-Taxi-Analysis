#!/usr/bin/python3.5
#
# generate the streaming time report
import subprocess
import datetime

def execute_command(command):
    result = None
    try:
        result = subprocess.check_output(command, shell=True)
    except:
        pass
    return result

def get_current_timezone_datetime():
    return datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=-5)))

if __name__=="__main__":
    import argparse,os

    parser = argparse.ArgumentParser()
    parser.add_argument("s3",help="S3 URL")
    parser.add_argument("output",help="Output filename")
    
    args = parser.parse_args()

    type = execute_command("ec2-metadata -t")
    if type != None:
        type = type.decode("utf-8")
        type = type[type.index(":") + 2 : type.index("\n")]
    start = get_current_timezone_datetime()

    count = execute_command("aws s3 cp {} - | grep 'fnard:-1 fnok:-1 cark:-1 gnuck:-1' -c".format(args.s3))
    end = get_current_timezone_datetime()
    timecost = (end - start).seconds

    if os.path.exists(args.output):
        raise RuntimeError("{} exists".format(args.output))
    
    # Create the output file
    with open(args.output,"w") as f:
        f.write("# streaming to a {}\n".format(type))
        f.write("date: {}\n".format(start.strftime("%Y-%m-%dT%H:%M:%S")))
        f.write("model: streaming\n")
        f.write("streaming: {}\n".format(timecost))
        f.write("malfunctions: {}".format(count.decode("utf-8")))

    
