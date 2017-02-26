# -*- coding:utf-8 -*-
__author__ = "TengWei"

import requests
import httplib
import traceback
import MySQLdb
import urllib
import time
import threading
# from UsefulProxyPool import runningPool
# import threadpool
from mainProcess import *
import time
from requests.exceptions import ProxyError
from NGACSPiderExceptions import ProxyPoolDBEmptyException

class metadataDbOps():
    def __init__(self, Host, User, passwd, databaseName):
        try:
            self.db = MySQLdb.connect(Host, User, passwd, databaseName, charset='utf8')
            self.cursor = self.db.cursor()
        except Exception:
            print traceback.format_exc()
            self.__del__()
        pass

    def __del__(self):

        pass

    def transactionsTemplate(self, sql, succStr='', failedStr=''):
        try:
            self.cursor.execute(sql)
            self.db.commit()
            print succStr
            return True
        except Exception:
            print failedStr
            print traceback.format_exc()
            self.db.rollback()
            return False
        pass

    def createCtrTable(self):
        sql = """
        create table ngacMetaData(
        mdIdnt varchar(220),
        metadataState enum("undownload","downloading", "downloaded") default "undownload",
        primary key(mdIdnt)
        )default charset=utf8;
        """
        succStr = "创建元数据下载控制表格成功！"
        failStr =  "创建元数据下载控制表格失败！"
        self.transactionsTemplate(sql, succStr, failStr)

    def initCtrTable(self):
        sql = "insert into ngacMetaData(mdIdnt) (select mdIdnt from NGACDocsInfo)"
        succStr = "初始化元数据下载控制表格成功！"
        failStr = "初始化元数据下载控制表格失败！"
        self.transactionsTemplate(sql, succStr, failStr)

    def iFinishedDownload(self):
        """ 下载完返回False, 否则返回新的id """
        sql = "select mdIdnt from ngacMetaData where metadataState = 1 limit 1;"
        sql2 = "update ngacMetaData set metadataState = 2 where mdIdnt = '%s'"
        try:
            self.cursor.execute(sql)
            tmplist = self.cursor.fetchone()
            sql2 = sql2 % tmplist[0]
            self.cursor.execute(sql2)
            self.db.commit()
            print tmplist
            if tmplist == None:  # 待下载列表为空：下载完
                print "下载完成！"
                return False
            else:
                print "获得新id"
                return tmplist[0]
        except Exception:
            self.db.rollback()
            print "查询或修改下载状态信息出错！"
            print traceback.format_exc()
        pass
    #
    # def getoneid(self):
    #     pass

    def changestate(self, mdIdnt, newState='3'):
        sql = 'update ngacMetaData set metadataState = ' + newState + ' ' + 'where mdIdnt="' + mdIdnt + '";'
        print sql
        succStr = ""
        failStr = "修改已下载文件" + str(mdIdnt) + ".xml 状态失败！"
        return self.transactionsTemplate(sql, succStr, failStr)
        pass

class proxyPool2():
    def __init__(self, Host, User, passwd, databaseName):
        # self.tp = runningPool(Host, User, passwd, databaseName, timeout=20)
        # self.tp.run()
        # self.pool = self.tp.pool
        self.Timer_p = Timer_ProxyPool(Host, User, passwd, databaseName, timeout=20)

        pass

    def updateProxyPool(self):
        #self.tp.run()
        #self.pool = self.tp.pool
        self.Timer_p.startProxyPool()


    def getoneIp(self):
        """
        if len(self.pool) == 0:
            self.updateProxyPool()
        return self.pool.pop()
        """
        tmpIp = self.Timer_p.getOneProxyIP()
        print tmpIp
        if tmpIp == False:
            raise ProxyPoolDBEmptyException
            return False
        return tmpIp
        pass

class getmetaDatafromNGAC(metadataDbOps, threading.Thread):
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        # "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.5",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "DNT": '1',
        "Host": "catalog.ngac.org.cn",
        "Host": "catalog.ngac.org.cn",
        # "Referer": """http://mail.qq.com/cgi-bin/mail_spam?action=check_link&url=http://catalog.ngac.org.cn/mutualsearch-access/csw?service=CSW%26request=GetRecordById%26version=2.0.2%26ElementSetName=full%26ID=cgdoi.n0001/x00063534.t02_0033&mailid=GAcJb3MGAAQOGQVbQEJxX1JoAFBNdGxLCX13dHZ+UwMB&spam=0""",
        "Upgrade-Insecure-Requests": '1',
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:50.0) Gecko/20100101 Firefox/50.0",
    }

    def __init__(self, Host, User, passwd, databaseName):
        metadataDbOps.__init__(self, Host, User, passwd, databaseName)
        self.pp2 = proxyPool2(Host, User, passwd, "ProxyPool2")
        pass

    def requestWithproxy(self, headers, data_encode, useProxy=True):
        # proxies = {"http": 'http://' + self.pp2.getoneIp()} if useProxy else None
        try:
            proxies = {"http": 'http://' + self.pp2.getoneIp()} if useProxy else None
            res = requests.get("http://catalog.ngac.org.cn/mutualsearch-access/csw?", headers=self.headers, params=data_encode, timeout=60, proxies=proxies)
            return True, res
        except ProxyError:
            print "ProxyError, change proxy Ip and try again..."
            return False,None
        except ProxyPoolDBEmptyException as e:
            print e.Message
            return -1, None
        except Exception as e:    # ReadTimeout: HTTPConnectionPool
            print e
            print "未知异常..."
            return -2, None
        pass


    def getmetaData(self, mdIdnt, prefixPath='./NGACmetadata/', useProxy=False):
        data = {
            "ElementSetName": "full",
            "ID": mdIdnt,
            "request": "GetRecordById",
            "service": "CSW",
            "version": "2.0.2",
        }
        data_encode = urllib.urlencode(data)
        flag, res = self.requestWithproxy(headers=self.headers, data_encode=data_encode,useProxy=True)
        while(flag in (False, -1,-2)):
            if flag == -1:
                time.sleep(30)
            flag, res = self.requestWithproxy(headers=self.headers, data_encode=data_encode,useProxy=True)
                
        #proxies = {"http": 'http://' + self.pp2.getoneIp()} if useProxy else None
        # res = requests.get("http://catalog.ngac.org.cn/mutualsearch-access/csw?", headers=self.headers, params=data_encode, timeout=30, proxies=proxies)
        mdIdnt = mdIdnt.replace('/', '+')
        print res.status_code
        # print res.headers
        # print res.content
        # print res.content
        with open(prefixPath + str(mdIdnt) + ".xml", "w") as wr:
            wr.write(res.content)

            # tmpobj = getDocsByPost()
            # print tmpobj.getDocsInfo()
        pass

    def run(self):
        newid = self.iFinishedDownload()
        while(newid):
            try:
                self.getmetaData(newid, useProxy=True)
            except Exception:
                print "获取数据失败..."
                print traceback.format_exc()
                return False

            if self.changestate(newid) == False:
                return False        
            
            newid = self.iFinishedDownload()

        pass

    def parsexmlstring(self):  # xml.dom.minidom.parseString

        pass
class multiProcesofgettingmetadata():
    def __init__(self, Host, User, passwd, databaseName, multiprcNum=10):
        self.multiprcNum = multiprcNum
        """
        baseSrgList = (Host, User, Passwd, databaseName,)
        self.argslist = []
        for _ in range(multiprcNum):
            self.argslist.append(baseSrgList)
        """
        self.tmpobj = getmetaDatafromNGAC(Host, User, passwd, databaseName)

    def startmultiprocess(self):
        """
        pool = threadpool.ThreadPool(self.multiprcNum)
        requests = threadpool.makeRequests(tmpobj.run())
        [pool.putRequest(req) for req in requests]
        pool.wait()
        """
        ThreadList = []
        for _ in range(self.multiprcNum):
            ThreadList.append(self.tmpobj.run())
         
        for i in range(self.multiprcNum):
            ThreadList[i].start()

        for i in range(self.multiprcNum):
            ThreadList[i].join()

        pass
    pass

if __name__ == "__main__":
    # ********************metadataDbOps类的测试和数据库表的初始化*************************
    # db = metadataDbOps("59.110.157.231", "root", "tw2016941017", "NGACDOCSINFO")
    # db.createCtrTable()       # 创建控制表格
    # db.initCtrTable()         # 初始化控制表格
    # db.iFinishedDownload()  # 测试状态查询

    # ********************运行测试*********************************************************
    """
    tmpobj = getmetaDatafromNGAC("localhost", "root", "tw2016941017", "NGACDOCSINFO")
    tmpobj.run()
    """
    #tmpobj.iFinishedDownload()

    # ****************多线程（线程池）测试*************************************************
    tmpobj = multiProcesofgettingmetadata("localhost", "root", "tw2016941017", "NGACDOCSINFO")
    tmpobj.startmultiprocess()

