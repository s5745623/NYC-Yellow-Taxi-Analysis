# ANLY 502 Assignment 5 version 1.2
# Due Sunday, April 2nd, 11:59pm

# SparkSQL and NoSQL
In this problem set we will work with SparkSQL and DynamoDB. In the first part of this problem set we will re-evaluate some of the problems from earlier in the course and see how they are easier to do with SparkSQL. In the second part we will explore how a document-based JSON database can be used to solve some problems with more flexibility than a SQL database.

## Question 1: Quazyilx again!
Remember that crazy dataset with the lines that look like this:

    2015-12-10T08:40:10Z fnard:-1 fnok:-1 cark:-1 gnuck:-1

Yes, you remember it. It's still stored in Amazon S3:

    $ aws s3 ls s3://gu-anly502/A1/
    2017-01-07 15:15:41  5244417004 quazyilx1.txt
    2017-01-07 13:19:28 19397230888 quazyilx2.txt
    2017-01-08 19:16:19 39489364082 quazyilx3.txt
    $ 

Your job is to do a better job analyzing one of these files than before.

It turns out that `quazyilx1.txt`  has 100,000,000 lines. On the Amazon m4.2xlarge computer it takes nearly 20 minutes to analyze the file, even when it is stored in HDFS. So we contacted Amazon tech support and were told that we were building our clusters the wrong way for this problem. Amazon recommended the following configuration:

* 1 Master m3.xlarge
* 2 Core m3.xlarge
* 4 Task m3.xlarge

Using this configuration, we were able to read the entire `quazyilx1.txt` file into an RDD in 59 seconds. This demonstrates how important it is to tune your cluster to your problem!

We recommend that you use the `.cache()` method as necessary.

1. Create a class called Quazyilx in the file `quazyilx.py` that processes a line and returns it as a slotted structure, with attributes for the `.time`, `.fnard`, `.fnok` and `.cark`. A test program called `quazyilx_test.py` has been provided for you.


2. You will need to turn the Quazyilx object into a `Row()` object. You can do that with a lambda, like this:

    `lambda q:Row(datetime=q.datetime.isoformat(),fnard=q.fnard,fnok=q.fnok,cark=q.cark,gnuck=q.gnuck))`

   Alternatively, you can add a new method to the Quazyilx class called `.Row()` that returns a Row. All of these ways are more or less equivalent. You just need to pick one of them.  You may find it useful to look at [this documentation](http://spark.apache.org/docs/latest/sql-programming-guide.html#inferring-the-schema-using-reflection).

3. Modify the file `quazyilx_run.py` to read the file [s3://gu-anly502/A1/quazyilx0.txt](s3://gu-anly502/A1/quazyilx0.txt) into an RDD of Row() objects (created above). Convert that RDD into a dataframe using the `spark.createDataFrame` method, register it as the SQL table `quazyilx` with the method `.createOrReplaceTempView`.

4. You should probably be doing this all in iPython (or, specifically, `ipyspark --files quazyilx.py`). You can now evaluate SQL commands at the command line using the `spark.sql` object. For example, if you want to know how many rows are in the `quazyilx` table, you can type:

```
In [3]: spark.sql("select count(*) from quazyilx").collect()
Out[3]: [Row(count(1)=1000000)]

In [4]:
```

   Experiment with the SQL and figure out how to generate:
   
   * The number of rows in the dataset
   * The number of lines that has -1 for `fnard`, `fnok`, `cark` and `gnuck`.
   * The number of lines that have -1 for `fnard` but have `fnok>5` and `cark>5`
   * The first datetime in the dataset, and the first datetime that has -1 for all of the values.
   * The last datetime in the dataset, and the last datetime that has a -1 for all of the values.

Each of these should be calculated with an SQL query. We've given you the prototype of a program in `quazyilx_run.py` to help you get going.  Get each SQL query working within `ipython`. Then paste the query into the program `quazyilx_run.py`. When you are finished, you can run the program with `spark-submit` and the output will be generated.
   
5. Run this program and save the output in the file `quazyilx_run.txt`. In fact, you should be able to type `make quazyilx_run.txt` on your Master node and have everything "just work." (Look at the [Makefile](Makefile) to see how the magic happens.)

**Note:** in our testing, processing `quazyilx1.txt` required 79 partitions, with each partition requiring approximately 210 seconds. Not all of the partitions can run at the same time, of course. The total time to compute `count(*)` of the Quazyilx objects was just under 15 minutes. Once that RDD was in memory, the other SQL statements ran in seconds. We've asked you to work with this whole dataset so that you can experience  the difference. 
   
## Question 2: Forensicswiki again!

Recall that the file s3://gu-anly502/logs/forensicswiki.2012.txt is a year's worth of Apache logs for the forensicswiki website. Each line of the log file correspondents to a single `HTTP GET` command sent to the web server. The log file is in the [Combined Log Format](https://httpd.apache.org/docs/1.3/logs.html#combined).
   
Our goal in this part is to write a map/reduce programs using SparkSQL that will report the number of hits for each month.  This time you will set up the entire program on your own, but we will give you the SQL statement to generate the output that we desire.

For your reference, here is the form of the desired output:

<pre>
    2010-01 10000
    2010-02 20000
    ...
</pre>

Where `10000` and `20000` are replaced by the actual number of hits in each month.

And here is the SQL statement that you should use to get it:

    SELECT SUBSTR(datetime,1,7),COUNT(1) from logs GROUP BY 1

We have given you a partially-completed `fwiki.py` module in which to put your program, and `fwiki_test.py` to test your log file parser. Your job is to finish `fwiki.py` and finish fwiki_run.py so that it produces the requested output.

## Question 3: JSON

This question is a simple question regarding JSON. You can solve it on your laptop using only Python.

The [Consumer Financial Protection Board](https://www.consumerfinance.gov) allows a sanitized version of its consumer complaints database to be downloaded from [http://www.consumerfinance.gov/data-research/consumer-complaints/#download-the-data](http://www.consumerfinance.gov/data-research/consumer-complaints/#download-the-data). (Please note that you can download the data using either a **http://** or a **https://** URL.

The dataset of credit card complaints can be downloaded from [http://data.consumerfinance.gov/api/views/7zpz-7ury/rows.json](http://data.consumerfinance.gov/api/views/7zpz-7ury/rows.json).  The dataset is in a strange format; if the JSON object is in a dictionary called `data[]`, then the rows are in `data['data']` and the metadata, including the column definitions, are in `data['meta']`. The column definitions are in `data['meta']['view']['columns']`.

Write a Python program that:

1. Downloads the dataset into a Python string.
2. Uses the **json** package to turn the JSON string into Python dictionary.
3. Compute the number of compaints for each year.

Your output should look like this:

```
...
2013    XXX
2014    XXX
...      
```
Where the first column is the year and the `XXX` represents the number of complaints in the year.

You should turn in a program called `complaints.py` with the output in `complaints.txt`.

* Extra Credit Option #1: (0.25 point): How many complaints were there against PayPal Holdings, Inc? Put your answer in `paypal.txt.`

* Extra Credit Option #2: (1 point): Have your program run on Apache SparkSQL, by loading the dataset into a table and computing the result with an SQL query.  Call this program `complaints_spark.py`.

* Extra Credit Option #3: (1 point): Have your program create and then store the results into a DynamoDB table, and then use DynamoDB to calculate the results. Please use your Georgetown NetID for the name of the table. Call this program `complaints_dd.py`.


To get you going, here is the code for downloading the dataset into the variable `data`:

    data = json.loads(urllib.request.urlopen("http://data.consumerfinance.gov/api/views/7zpz-7ury/rows.json").read())
