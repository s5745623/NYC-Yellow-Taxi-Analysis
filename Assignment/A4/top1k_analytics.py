from pyspark import SparkContext
from subprocess import Popen,PIPE

sc = SparkContext.getOrCreate()
top1m = sc.textFile("/top-1m.csv").cache()
top1k = top1m.take(1000)

def google_analytics(url):

    try:
        page = Popen(['curl','-s','-L',url],stdout=PIPE).communicate()[0].decode('utf-8','ignore')
    except:
        print("the web page is not in ASCII or UTF-8.")
    return(("analytics.js" in page) or ("ga.js" in page))

aa = open("top1k_analytics.txt","w")
for line in top1k:
    domains = line.split(",")
    print(domains)
    if google_analytics(domains[1].strip()):
        aa.write('%s, %s\n'%(domains[1].strip(), "True"))
aa.close()