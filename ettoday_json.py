# for dbmaker
# for ettoday
# coding=utf-8
from lxml import etree, html
from lxml.html import fromstring
from time import sleep
from datetime import datetime, date, time, timedelta
import requests, json, pyodbc, threading, queue
import sys
import MySQLdb

class Constant:
    URL_Root = "http://www.ettoday.net"
    URL_Entrance = "http://www.ettoday.net/news/news-list.htm"
    WORKERLIMIT = 1

class News:
    def __init__(self, category, url):
        self.category = category
        self.entranceUrl = url
        self.url = Constant.URL_Root + url
        self.lastNewsPublished = ""
        self.cnxn = MySQLdb.connect("139.162.23.125", "root", "DifficulT7130", "cybersite", use_unicode=True, charset="utf8")
        writeLog ('I', "connect to database")
    def do(self):
        # self.cnxn = pyodbc.connect('Driver={MySQL};Database=cybersite;Server=139.162.23.125;Port=3306;UID=root;PWD=DifficulT7130;')
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
                news = root.xpath("//div[@class='part_list_2']/h3/a")
                if len(news) == 0:
                    writeLog('W', "search: (%s) no result" % self.category)
                    self.lastNewsPublished = ""
                    return
                for new in news:
                    self.fetchNewsContent(Constant.URL_Root+new.get('href'))

                pages = root.xpath("//div[@class='menu_page']/a")
                for page in pages:
                    if ">" in page.text.strip():
                        hasNextPage = True
                        self.url = Constant.URL_Root + page.get('href').strip()
                        writeLog('I', 'next url: %s' % self.url)
                        break
            writeLog('I', "category: %s, run end of page" % self.category)

        self.cursor.close()
        self.cnxn.close()

    def fetchNewsContent(self, url):
        result = tryRequestURL(threading.current_thread().name, url)
        root = fromstring(result.text)
        newspublished = root.xpath("//meta[@itemprop='dateCreated'] | //div[@class='date'] | //p[@class='date']")
        if len(newspublished) == 0:
            writeLog('W', "error to parse timestamp in url: %s" % url)
            return
        if newspublished[0].tag == 'meta':
            newspublished = newspublished[0].get('content').strip()
            if "T" in newspublished:
                newspublished = datetime.strptime(newspublished, "%Y-%m-%dT%H:%M:%S")
            else:
                newspublished = datetime.strptime(newspublished, "%Y-%m-%d %H:%M:%S")
        elif newspublished[0].tag == 'div' or newspublished[0].tag == 'p':
            newspublished = newspublished[0].text.strip()
            newspublished = datetime.strptime(newspublished, "%Y-%m-%d %H:%M")

        headline = root.xpath("//h2[@itemprop='headline'] | //h2[@class='title'] | //h1[@class='title'] | //h1[@class='title_article']")
        if len(headline) == 0:
            writeLog('W', "error to parse headline in url: %s" % url)
            return
        else:
            headline = headline[0].text

        contents = root.xpath("//div[@class='story']/section[@itemprop='articleBody']/p | //div[@class='story']/p")
        if len(contents) == 0:
            writeLog('W', "error to parse content in url: %s" % url)
            return

        author = 'ETToday'
        if len(contents) > 3:
            for c in contents[0:5]:
                authorDict = c.text_content().split('／')
                if len(authorDict) == 2:
                    if len(c.text_content().strip()) < 15:
                        author = c.text_content().strip()
                        if "報導" in c.text_content():
                            author = c.text_content().strip()
                            break

        writeLog('I', 'headline: %s, category: %s, newspublished: %s' % (headline, self.category, newspublished))
        writeLog('I', 'author: %s' % (author))
        self.lastNewsPublished = str(newspublished)
        self.insert_data(headline, "\n".join([c.text_content().replace(u'\xa0', ' ').strip() for c in contents]), author, newspublished, url, self.category)

    def insert_data(self, headline, paragraph, author, createtime, url, category):
        writeLog("I", "[%s], search record: %s, %s" % (threading.current_thread().name, headline, author))
        paragraph = paragraph.replace("\n", "")
        while True:
            try:
                 self.cursor.execute("select count(*) from ettoday where headlines=%s and author=%s " , (headline, author))
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
                    djson ={"JHEADLINE":headline,"JCONTENT":paragraph,"JCATEGORY":category,"JAUTHOR":author,"JNEWSPUBLISHED":str(createtime),"JURL":url}
                    myjson = json.dumps(djson)
                    self.cursor.execute("insert into ettoday (headlines, content, author, newspublished, url, category,djson) values (%s,%s,%s,%s,%s,%s,%s)", (headline, paragraph, author, createtime, url, category, myjson))
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
        # self.cnxn = pyodbc.connect('Driver={MySQL};Database=cybersite;Server=139.162.23.125;Port=3306;UID=root;PWD=DifficulT7130;')
        self.cnxn = MySQLdb.connect("139.162.23.125", "root", "DifficulT7130", "cybersite", use_unicode=True, charset="utf8")
        self.cursor = self.cnxn.cursor()

###### global function ######

def getSelectConditions():
    categoryDict = []
    result = tryRequestURL('main', Constant.URL_Entrance)
    root = fromstring(result.text)
    categorys = root.xpath("//div[@class='part_menu_2']/ul/li/a")
    idx = 0
    for category in categorys[2:]:
        idx = idx + 1
        if idx > 1:
            break
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
    #print(message)

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
    file_ = open('ettoday_json.log', mode='a', encoding='utf-8')
    main()
