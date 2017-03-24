# -*- coding:utf-8 -*-
__author__ = "TengWei"

import requests
import httplib
import traceback
import MySQLdb
import urllib
import time
import random
import os
from UsefulProxyPool import runningPool
from requests.exceptions import ProxyError, Timeout, ConnectionError


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

    def transactionsTemplate(self, sql, succStr='', failedStr='', executemany=False, valuelist=None):
        try:
            if executemany:
                self.cursor.executemany(sql, valuelist)
            else:
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
        metadataState enum("undownload", "downloading","downloaded") default "undownload",
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
        sql = "select mdIdnt from ngacMetaData where metadataState = 1 limit 1 for update;"
        try:
            self.cursor.execute(sql)
            tmplist = self.cursor.fetchone()
            print tmplist
            if tmplist == None:  # 待下载列表为空：下载完
                print "下载完成！"
                return False
            else:
                print "获得新id"
                return tmplist[0]
        except Exception:
            print "查询下载状态信息出错！"
            print traceback.format_exc()
        pass
    #
    # def getoneid(self):
    #     pass

    def changestate(self, mdIdnt, newState='3', executemany=False, valuelist=None):
        # sql = 'update ngacMetaData set metadataState = ' + newState + ' ' + 'where mdIdnt="' + mdIdnt + '";'
        sql = 'update ngacMetaData set metadataState = %s where mdIdnt = "%s";'
        # print sql
        succStr = ""
        if not executemany:
            sql = sql % (newState, mdIdnt)
            failStr = "修改已下载文件" + str(mdIdnt) + ".xml 状态失败！"
        else:
            failStr = "修改已下载文件群状态失败！"
        return self.transactionsTemplate(sql, succStr, failStr, executemany, valuelist)
        pass

class proxyPool2():
    def __init__(self, Host, User, passwd, databaseName):
        self.tp = runningPool(Host, User, passwd, databaseName, timeout=20)
        # self.tp.run(timeRange=1440)
        # self.pool = self.tp.pool
        # print self.pool
        self.pool = set([u'113.233.153.114:8118', u'219.148.108.126:8998', u'125.118.77.48:808', u'171.38.196.230:8123', u'111.76.133.91:808', u'111.76.133.198:808', u'60.13.74.211:80', u'111.76.129.38:808', u'106.46.136.58:808', u'115.28.134.77:80', u'111.76.129.103:808', u'111.76.129.106:808', u'111.76.129.186:808', u'119.162.51.22:8118', u'106.46.136.8:808', u'114.215.80.237:80', u'120.25.123.95:80', u'171.39.26.116:8123', u'222.134.134.250:8118', u'203.90.144.145:80', u'111.76.129.122:808'])
        pass

    def updateProxyPool(self):
        self.tp.run(multiNum=30)
        self.pool = self.tp.pool
        print self.pool

    def getoneIp(self):
        if len(self.pool) == 0:
            self.updateProxyPool()
        return self.pool.pop()
        pass

class getmetaDatafromNGAC(metadataDbOps):
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        # "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.5",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "DNT": '1',
        "Host": "catalog.ngac.org.cn",
        # "Referer": """http://mail.qq.com/cgi-bin/mail_spam?action=check_link&url=http://catalog.ngac.org.cn/mutualsearch-access/csw?service=CSW%26request=GetRecordById%26version=2.0.2%26ElementSetName=full%26ID=cgdoi.n0001/x00063534.t02_0033&mailid=GAcJb3MGAAQOGQVbQEJxX1JoAFBNdGxLCX13dHZ+UwMB&spam=0""",
        "Upgrade-Insecure-Requests": '1',
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 \
         Safari/537.36 Edge/12.10240"
    }
    
    
    def __init__(self, Host, User, passwd, databaseName):
        metadataDbOps.__init__(self, Host, User, passwd, databaseName)
        self.pp2 = proxyPool2("localhost", User, passwd, "ProxyPool2")
        self.prefix = "/home/david/文档/data/NGACmetadata_"     # 存放文件的文件夹路径的前缀
        self.dircount = 1                                       # 上述文件夹的序号：后缀
        self.fullfileNums = 10000                               # 每个文件加放10000条数据
        self.preventfileNums = 0                                # 上次中断前/当前文件夹中文件的个数
        self.getdircount_preNums()                              # 更新dircount 和 preventfileNums 
        self.loopperiod = 100
        # self.getprefixpath()    # 包含更新数据

        """
        self.prefix = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # self.prefix = "/home/david/文档/data/"
        self.prefix = os.path.dirname(self.prefix)        
        self.prefix += "/data/"
        """
        pass

    def getdircount_preNums(self):
        while(self.getfilenums(self.getprefixpath()) >= self.fullfileNums):
            self.dircount += 1
            if os.path.isdir(self.getprefixpath()) == False:
                os.makedirs(self.getprefixpath())
        self.preventfileNums = self.getfilenums(self.getprefixpath())

        #if os.path.isdir(self.getprefixpath()) == False:
        #    os.makedirs(self.getprefixpath())
        pass

    def requestWithproxy(self, headers, data_encode, useProxy=True, newproxyIp=''):
        if useProxy and newproxyIp == '':
            newproxyIp = self.pp2.getoneIp()
        proxies = {"http": 'http://' + newproxyIp} if useProxy else None
        try:
            res = requests.get("http://catalog.ngac.org.cn/mutualsearch-access/csw?", headers=self.headers, params=data_encode, timeout=30, proxies=proxies)
            return True, res, newproxyIp
        except ProxyError:
            print "ProxyError, change proxy Ip and try again..."
            return False, None, ''
        except Timeout:
            print "Proxy request Timeout, change proxy Ip and try again..."
            return False, None, ''
        except ConnectionError:
            print "Connection Error, change proxy Ip and try again..."
            return False, None, ''
        except Exception:
            print "other Error, change proxy Ip and try again..."
            return False, None, ''
        pass

    def getprefixpath(self):
        return self.prefix + str(self.dircount) + "/"

    
    def getfilenums(self, dirpath):
        return len([x for x in os.listdir(dirpath)])


    def getmetaData(self, mdIdnt, prefixPath="/home/david/文档/data/NGACmetadata_1/", useProxy=False):
        data = {
            "ElementSetName": "full",
            "ID": mdIdnt,
            "request": "GetRecordById",
            "service": "CSW",
            "version": "2.0.2",
        }
        data_encode = urllib.urlencode(data)
        flag, res, oldproxyIP= self.requestWithproxy(headers=self.headers, data_encode=data_encode, useProxy=useProxy)
        while(flag == False):
            flag, res, oldproxyIP = self.requestWithproxy(headers=self.headers, data_encode=data_encode, useProxy=useProxy, newproxyIp=oldproxyIP)
        # proxies = {"http": 'http://' + self.pp2.getoneIp()} if useProxy else None
        # res = requests.get("http://catalog.ngac.org.cn/mutualsearch-access/csw?", headers=self.headers, params=data_encode, timeout=30, proxies=proxies)
        mdIdnt = mdIdnt.replace('/', '+')
        if res.status_code != 200:
            print "获取信息失败!", res.status_code
            return False
        # print res.headers
        # print res.content
        # print res.content
        with open(prefixPath + mdIdnt + ".xml", "w") as wr:
            wr.write(res.content)

            # tmpobj = getDocsByPost()
            # print tmpobj.getDocsInfo()
        return True
        pass

    def run(self):
        newid = self.iFinishedDownload() 

        while(newid):
            try:
                if not self.getmetaData(newid, self.getprefixpath(), useProxy=False):                    
                    self.changestate(newid, newState=1) 
                    time.sleep(6)
                    continue
            except Exception:
                print "获取数据失败..."
                print traceback.format_exc()
                return False

            if self.changestate(newid) == False:
                return False
            print "成功"
            newid = self.iFinishedDownload()

            self.preventfileNums += 1
            if  self.preventfileNums > self.fullfileNums:
                self.dircount += 1
                self.preventfileNums = 1
                if os.path.isdir(self.getprefixpath()) == False:
                    os.makedirs(self.getprefixpath())

            if self.preventfileNums%self.loopperiod == 0:
                time.sleep(random.randint(1, 20))

    def parsexmlstring(self):  # xml.dom.minidom.parseString

        pass

if __name__ == "__main__":
    # ********************metadataDbOps类的测试和数据库表的初始化*************************
    # db = metadataDbOps("localhost", "root", "tw2016941017", "NGACDOCSINFO")
    # db.createCtrTable()       # 创建控制表格
    # db.initCtrTable()         # 初始化控制表格
    # db.iFinishedDownload()  # 测试状态查询

    # ********************运行测试*********************************************************
    tmpobj = getmetaDatafromNGAC("localhost", "root", "tw2016941017", "NGACDOCSINFO")
    tmpobj.run()
    pass
