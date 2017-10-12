## ANLY 502 Assignment 2: Learning MapReduce (V1.3)

***RELEASED***

## Required skills and technologies

* YARN
* Hadoop
* MapReduce

In this assignment you will learn the basics of MapReduce. You will do this with four exercises: 

1. We will rework the filtering exercise of [Assignment
1](../A1/Readme.md) on Hadoop MapReduce using Hadoop's "Streaming"
API. We will do this with a _mapper_ that filters out the lines that
are not wanted, and a _reducer_ that simply copies from input to output.

2. We will then perform the classic "word count" exercise, which creates a histogram of all the words in a text document. The mapper will map single lines to individual words, and the reducer will count the number of words.

3. We will perform a logfile analysis, using web logs from from the website
[https://forensicswiki.org/](https://forensicswiki.org/) between TKDATE and TKDATE. We will generate a report of the number of web "hits" for each month in the period under analysis. 

4. We will then introduce the concept of the _combiner_, which is a
reducer that runs on each mapper before the keys are combined
globally. We will use the combiner to implement an efficient "top-10" pattern that computes the top-10 on each node, minimizing the amount of data that is transfered. 

## Part 1: Creating your first EMR cluster.

1. If you VMs from Assignment #1 are still running, log into them and
make sure that the git repositories are committed and pushed back to
your git server.  (Be sure to use `git status` to see if there are any
files that need to be added to the git repository with `git add`.)

2. Log into the [Amazon Web Services console](https://aws.amazon.com/) and make sure that your prevous VMs are shut down. 

3. Log into the [Elasitic Map Reduce console](https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#) and create a cluster. Below we walk you through the "Quick options."

* Log to S3

* Create a cluster for Core Hadoop.

* Use m3.xlarge with 3 instances (1 master and 2 core nodes)

* Use your existing EC2 key pair (hopefully you still have access to the private key!)

4. Click on `AWS CLI export` and copy the command line into the file [q1.txt](q1.txt) in this directory.  Answer the questions in the file.

5. Log into the head end node ssh. Note: *git* will not be installed on the EMR cluster, but *python 3.4* is installed by default!  (We just discovered this, and we're thrilled!)

6. Run the command `df -h` to see the drives attached to your virtual machine. 

7. Review the [YARN Commands](https://hadoop.apache.org/docs/r2.7.3/hadoop-yarn/hadoop-yarn-site/YarnCommands.html) on the Apache Hadoop website.  Run the `yarn version` command to see which version of YARN you are using. Run the `yarn node -list` comkmand to see the nodes that are installed on your cluster. 

*Yea! You are now ready to use MapReduce!*

## Part 2: Basic Filtering with Map Reduce

In this section we will replicate the filtering project of Assignment #1, but we'll do it with [Hadoop Streaming](https://hadoop.apache.org/docs/r2.7.3/hadoop-streaming/HadoopStreaming.html).  (To see how Hadoop Streaming has been modified for Amazon Map Reducer, please review the [Amazon EMR documentation](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/UseCase_Streaming.html)) Because of the minimal amount of computation done, these tasks are entirely I/O-bound. 

We have given you a program called [run.py](run.py). This Python program is specially created for this course. It both runs the Hadoop
Streaming job and writes time results into a data file. There is also a symbolic link to `run.py` called [p2_python.py](p2_python.py). When you run the command `p2_python.py`, it will default to using the program `q2_mapper.py` as the mapper and `q2_reducer.py` as the reducer, but you can change these with the `--mapper` and `--reducer` arguments. The program uses `--input` to specify the input S3 file/prefix, or HDFS file/directory.  The `--output` option specifies where the output is stored.  You can specify other things as well; use `python34 run.py --help` to get a list of the options.

We have also give you two starting Python programs: [q2_mapper.py](q2_mapper.py) and [q2_reducer.py](q2_reducer.py). These programs implement _wordcount_. You need to change them to implement a filtered counting!

1. Run a Hadoop Streaming job using a modified version of the provided [q2_mapper.py](q2_mapper.py) and [q2_reducer.py](q2_reducer.py) that  outputs the number of lines that have the `fnard:-1 fnok:-1 cark:-1 gnuck:-1` pattern. The input to the Hadoop Streaming job is an S3 filename. 

You can run the Hadoop Streaming job two ways:

* Using the provided [run.py](run.py) Python script as explained above, or,

* Manually running the `hadoop` command as explained in class using the following as an example: 

```
hadoop jar /usr/lib/hadoop/hadoop-streaming-2.7.3-amzn-1.jar \
-files q2_mapper.py,q2_reducer.py \
-input [[input-file]] -output [[output-location]] \
-mapper q2_mapper.py \
-reducer q2_reducer.py
```

2. Run the program on the file `s3://gu-anly502/A1/quazyilx1.txt` and verify that you get the same result that you got in Assignment 1. 

3. Run the program on the file `s3://gu-anly502/A1/quazyilx2.txt`, a 19.4GB file, and note the answer.

4. Run the program on the file `s3://gu-anly502/A1/quazyilx3.txt`, a 39.5GB file, and note the answer.

5. You did the above problems with your cluster with 1 master node and 2 core nodes. The master node controls your cluster, hold HDFS files, and can run jobs. The core nodes holds HDFS files and run jobs. Amazon's EMR allows you to dynamically add (or remove) core and task instances. As you add more core and task instances, your jobs will run faster, up to a particular point.  Add a core node and recompute the amount of time it takes to run the job on `quazyilx1.txt`, `quazyilx2.txt` and `quazyilx3.txt`.

       

6. Repeat the experiment with 2 and 4 Task nodes. The prototype
[run.py](run.py) program that we have given you computes the
clock time that the job took and records the number of nodes in an output file. We have also created a symbolic link called [q2_python.py](q2_python.py) that points to [run.py](run.py). When the [q2_python.py](q2_python.py) link is given to the Python language, the [run.py](run.py) sees the name that it has been called with and stores the results in a
file called [q2_results.txt](q2_results.txt). The `--plot` option of the program should read this file and plots it using [matplotlib](http://matplotlib.org/). However, currently it doesn't. Instead, there is a program called [grapher.py](grapher.py) that generates a plot with fake data that is hard-coded into the file. We will modify the program to do the plotting by the end of the first weekend of the problem set, but you can do it yourself if you wish the experience! 

To use this program, you will need to either install matplotlib on your head-end, or else you will need to commit the results to your private git repository, pull the results to a system that has matplotlib installed, and generate the plot there.

Turn in the files `q2_results.txt`, `q2_plot.png` and `q2_plot.pdf`, showing how the speed of this system responds to increases in the number of nodes.

## Part 3: HDFS

As in Part 2, these steps are entirely I/O bound. However this time you are able to control the I/O performance of your system.

1. Resize your cluster and remove all of the Task nodes.  You now have 3 nodes: 1 master and 2 core.

2. Copy the file s3://gu-anly502/A1/quazyilx3.txt to your local HDFS system.

3. The program [q3_python.py](q3_python.py) is another link to the program (run.py)[run.py] but stores its results in the file [q3_results.txt](q3_results.txt). Run the program and compute how long it takes to run.

4. When you copied the file `quazyilx3.txt` to your HDFS system, it was split into blocks and stored on three different systems. Delete the file in HDFS and resize your cluster so that it has 5 Core nodes.  Copy the file `quazyilx3.txt` back to your HDFS system; now it will be stored on 6 nodes (the 1 master and the 5 core nodes).  Re-run (q3_python.py)[q3_python.py] and see how the time that the program takes changes.

5. Delete the file `quazyilx3.txt` again, resize your cluster to have 10 Core nodes, copy the file `quazyilx3.txt` back to your cluster, and re-run (q3_python.py)[q3_python.py]. This determines how long the process takes with the file stored on 11 HDFS nodes.

Commit your results to your git repository and push the repository to your git server. Shut down your cluster when you are done.

## Part 4: Logfile analysis

The file s3://gu-anly502/logs/forensicswiki.2012.txt is a year's worth of Apache logs for the forensicswiki website. Each line of the log file correspondents to a single `HTTP GET` command sent to the web server. The log file is in the [Combined Log Format](https://httpd.apache.org/docs/1.3/logs.html#combined).

If you look at the first few lines of the log file, you should be able to figure out the format. You can view the first 10 lines of the file with the command:

    aws s3 cp s3://gu-anly502/logs/forensicswiki.2012.txt - | head -10

At this point, you should understand why this command works.

Our goal in this part is to write a map/reduce programs using Python3.4 and Hadoop Streaming that will report the number of hits for each month. For example,
if there were 10,000 hits in the month January 2010 and 20,000 hits in
the month February 2010, your output should look like this:

<pre>
    2010-01 10000
    2010-02 20000
    ...
</pre>

Where `10000` and `20000` are replaced by the actual number of hits in each month.

Here are some hints to solve the problem:

* Your mapper should read each line of the input file and output a key/value pair in the form `YYYY-MM\t1` where `YYYY-MM` is the year and the month of the log file, `\t` is the tab character, and `1` is the number one.

* Your reducer should tally up the number of hits for each key and output the results.

* Once the results are stored in in HDFS, you can output the results the `hdfs dfs -cat` command piped into a Unix sort.

1. Store the output in the file `q4_monthly.txt` . 

Turn in `q4_mapper.py`, `q4_reducer.py`, and `q4_run.py` in addition to `q4_monthly.txt`.

## Part 5: EXTRA CREDIT Finding the top-10 hits

1. Extract the URL and generate a histogram of the total number of HTTP transactions ("hits") for each URL.

2. Now modify this program so that it only records the top 10 hits. Do this in two steps:

2a. Write a top-10 combiner that performs a data reduction at each node, keeping track of the top-10, and outputing the most popular 10 with a single key to the reducer.

2b. Write a top-10 reducer that reads the top-10 from each of the combiners, determines the top-10, and stores it in a single file in HDFS or S3.

Provide an output file that has the top-10. This file should be called q5_top_10.txt and have the following format:

        COUNT <tab> URL

The file should be sorted by COUNT, with the largest first.

Turn in `q5_mapper.py`, `q5_reducer.py`, `q5_combiner.py` and `q5_run.py` in addition to `q5_top_10.txt`.

## Make submit and submit!

As before, you should submit a single ZIP file. Please use the `Makefile` and the `validator.py` to make the Makefile. Do this by typing `make submit`. Be sure to edit `../user.cfg` to put in your personal information.

