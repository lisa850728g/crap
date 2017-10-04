# coding=utf-8
from lxml import etree, html
from lxml.html import fromstring
from time import sleep
from datetime import datetime, date, time, timedelta
import requests, json, pyodbc, threading, queue
import sys
import MySQLdb
from _mysql_exceptions import OperationalError

class Constant:
    URL_Entrance = "http://www.businessweekly.com.tw/"
    WORKERLIMIT = 1

class News:
    def __init__(self, category, url):
        self.category = category
        self.entranceUrl = url
        self.url = Constant.URL_Entrance + url
        self.lastarticlespublished = ""
        self.cnxn = MySQLdb.connect("139.162.23.125", "root", "DifficulT7130", "cybersite", use_unicode=True, charset="utf8")
        writeLog ('I', "connect to database")
    def do(self):
        #self.cnxn = pyodbc.connect('Driver={DBMaker 5.4 Driver};Database=CYBERSITE;Uid=SYSADM;Pwd=;')
        self.cursor = self.cnxn.cursor()
        writeLog ('I', "search: (%s)" % (self.category))

        canSearchAgain = True

        hasNextPage = True

        while canSearchAgain:
            canSearchAgain = False
            hasNextPage = True
            while hasNextPage:
                hasNextPage = False
                result = tryRequestURL(threading.current_thread().name, self.url)
                root = fromstring(result.text)
                articles = root.xpath("//article[@class='channelnew']/a")
                if len(articles) == 0:
                    writeLog('W', "search: (%s) no result" % self.category)
                    self.lastarticlespublished = ""
                    return
                for article in articles:
                    self.fetchNewsContent(article.get('href')+"&p=0")

            writeLog('I', "category: %s, run end of page" % self.category)

        self.cursor.close()
        self.cnxn.close()

    def fetchNewsContent(self, url):
        result = tryRequestURL(threading.current_thread().name, url)
        root = fromstring(result.text)
        articlespublished = root.xpath("//div[@class='articleDate']")
        if len(articlespublished) == 0:
            writeLog('W', "error to parse datetime in url: %s" % url)
            return
        if articlespublished[0].tag == 'div':
            articlespublished = articlespublished[0].text

        headline = root.xpath("//div[@class='pageIntro']/header[@class='headline']/h1")
        if len(headline) == 0:
            writeLog('W', "error to parse headline in url: %s" % url)
            return
        else:
            headline = headline[0].text

        #views = root.xpath("//span[@class='counts'] | // span[@class='counts']/em")
        views = root.xpath("//span[@class='counts']")
        if len(views) == 0:
            writeLog('W', "error to parse views in url: %s" % url)
            return
        else:
            views = views[0].text.strip("瀏覽數：")
            if views == "":
                view_num = 0
            else:
                view_num = int(views)
            print(view_num)

        contents = root.xpath("//div[@class='articlebody col-md-12 be-changed'] | //div[@class='articlebody']/p")
        if len(contents) == 0:
            writeLog('W', "error to parse content in url: %s" % url)
            return

        author = root.xpath("//div[@class='author']/text()")
        if len(author) == 0:
            author = ' '
        else:
            author = author[0]
            #author = author[0].text.strip("撰文者：")
            print(author)

        writeLog('I', 'headline: %s, category: %s, articlespublished: %s' % (headline, self.category, articlespublished))
        writeLog('I', 'author: %s' % (author))
        self.lastarticlespublished = str(articlespublished)
        self.insert_data(headline, "\n".join([c.text_content().replace(u'\xa0', ' ').strip() for c in contents]), author, articlespublished, url.strip("&p=0")+"&p=1", self.category, view_num)

    def insert_data(self, headline, paragraph, author, createtime, url, category, view_num):
        writeLog("I", "[%s], search record: %s, %s" % (threading.current_thread().name, headline, author))
        paragraph = paragraph.replace("\n", "")
        while True:
            try:
                 self.cursor.execute("select count(*) from business_weekly where headlines=%s and author=%s" , (headline, author))
                 break
            except pyodbc.Error as err:
                 writeLog('E', "dbmaker err happened in find duplicate record: %s" % err)
                 self.reconnectDB()
            else:
                 writeLog('E', "dbmaker didn't caught err with find duplicate record")
                 self.reconnectDB()

        row = self.cursor.fetchone()
        if row[0] != 0:
            writeLog("I", '[%s], headline: %s already in record' % (threading.current_thread().name, headline))
        else:
            while True:
                try:
                    djson ={"JHEADLINE":headline,"JCONTENT":paragraph,"JCATEGORY":category,"JAUTHOR":author,"JARTICLESPUBLISHED":str(createtime),"JURL":url,"JVIEWS":view_num}
                    myjson = json.dumps(djson)
                    try:
                        self.cursor.execute("insert into business_weekly (headlines, content, author, articlespublished, url, category, views, djson) values (%s,%s,%s,%s,%s,%s,%s,%s)", (headline, paragraph, author, createtime, url, category, view_num, myjson))
                    except OperationalError:
                        # Has error string in the content
                        error_log.write("headlines:%s\ncontent:%s\nauthor:%s\narticlespublished:%s\ncategory:%s\n" %(headline, paragraph, author, createtime, category))
                    self.cnxn.commit()
                    writeLog("I", '[%s], headline: %s insert success' % (threading.current_thread().name, headline))
                    break
                except pyodbc.Error as err:
                    writeLog('E', "dbmaker err happened in insert new record: %s" % err)
                    self.reconnectDB()
                else:
                    writeLog('E', "dbmaker didn't caught err with condition insert new record")
                    self.reconnectDB()
                return True

    def reconnectDB(self):
        writeLog('I', "start to reconnect connection")
        #self.cnxn = pyodbc.connect('Driver={DBMaker 5.4 Driver};Database=CYBERSITE;Uid=SYSADM;Pwd=;')
        self.cnxn = MySQLdb.connect("139.162.23.125", "root", "DifficulT7130", "cybersite", use_unicode=True, charset="utf8")
        self.cursor = self.cnxn.cursor()

###### global function ######

def getSelectConditions():
    categoryDict = []
    result = tryRequestURL('main', Constant.URL_Entrance)
    root = fromstring(result.text)
    categorys = root.xpath("//div[@class='area_name2']/a")
    idx = 0
    for category in categorys[0:]:
        idx = idx + 1
        categoryDict.append({'name': category.text, 'url': category.get('href')})
    return categoryDict

def tryRequestURL(threadName, url):
    tryTimes = 5;
    result = ""
    while tryTimes > 0:
        try:
            result = requests.get(url, timeout=10)
        except requests.exceptions.Timeout as e:
            writeLog("W", '[%s], timeout err: %s' % (threadName, e))
            tryTimes -= 1
            writeLog("W", '[%s], last %d retry chance' % (threadName, tryTimes))
            sleep(3)
            continue
        except requests.exceptions.ReadTimeout as e:
            writeLog("W", '[%s], read timeout err: %s' % (threadName, e))
            tryTimes -= 1
            writeLog("W", '[%s], last %d retry chance' % (threadName, tryTimes))
            sleep(3)
            continue
        except requests.packages.urllib3.exceptions.ReadTimeoutError as e:
            writeLog("W", '[%s], urllib3 read timeout err: %s' % (threadName, e))
            tryTimes -= 1
            writeLog("W", '[%s], last %d retry chance' % (threadName, tryTimes))
            sleep(3)
            continue
        except requests.exceptions.TooManyRedirects as e:
            writeLog("W", '[%s], too many redirect %s' % (threadName, e))
            tryTimes -= 1
            writeLog("W", '[%s], last %d retry chance' % (threadName, tryTimes))
            sleep(3)
            continue
        except requests.exceptions.RequestException as e:
            writeLog("W", '[%s], request error: %s' % (threadName, e))
            tryTimes -= 1
            writeLog("W", '[%s], last %d retry chance' % (threadName, tryTimes))
            sleep(3)
            continue
        break
    if result != "":
        result.encoding='utf8'
    return result

def writeLog(level, message):
    global file_
    message = "Log %s %s %s" % (level, datetime.now().time(), message)
    file_.write("%s\n" % message)

def startCrawler(*args):
    queue = args[0]
    while queue.qsize() > 0:
        job = queue.get()
        job.do()

###### end of global function ######

def main():
    global file_
    que = queue.Queue()

    selections = getSelectConditions()
    for category in selections:
        que.put(News(category['name'], category['url']) )

    writeLog("I", "[Info] Worker size={%d}..." % que.qsize())

    newsList = []

    # 將任務分配給thread
    for i in range(Constant.WORKERLIMIT):
        worker = threading.Thread(target=startCrawler, name='thd%d' % i, args=(que,))
        newsList.append(worker)
        worker.start()

    for i in range(Constant.WORKERLIMIT):
        newsList[i].join()

    writeLog("I", 'all thread done')
    file_.close()

if __name__ == "__main__":
    file_ = open('business_weekly.log', mode='a', encoding='utf-8')
    error_log = open('error_business.log', mode='a', encoding='utf-8')
    main()
