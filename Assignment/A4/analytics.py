
def google_analytics(url):
    from subprocess import Popen,PIPE
    try:
        page = Popen(['curl','-s','-L',url],stdout=PIPE).communicate()[0].decode('utf-8','ignore')
    except:
        print("the web page is not in ASCII or UTF-8.")
    return(("analytics.js" in page) or ("ga.js" in page))