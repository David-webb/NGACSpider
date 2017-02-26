#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'Tengwei'

import requests
import traceback
import httplib
import urllib
import json

# 从网页提取的数据(utf8编码)无法正常显示
# 设置系统内部的编码为utf8
# import sys
# reload(sys)
# sys.setdefaultencoding('utf8')

"""***********文章数据的结构*****************
["documents":[{文章dict},{文章dict}...], "pageTotal": totalpagenum]
"文章dict"结构如下:
{
        abs
        auther
        formTime
        geoInfoName
        hiName
        idContOrg
        mdIdnt      # 用作主键
        mdTitle
        mineralCode
}
"""

class getDocsByPost():

    headers = {
    "Host": "catalog.ngac.org.cn",
    "User-Agent": "Mozilla/5.0 (X11; Linux i586; rv:31.0) Gecko/20100101 Firefox/31.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.5",
    # "Accept-Encoding": "gzip, deflate",
    "Referer": "http://catalog.ngac.org.cn/index/jumpnewindex.action",
    "Connection": "keep-alive",
    "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
}
    aurl = "http://catalog.ngac.org.cn/index/search.action?msg=1"      # post 使用的url
    def __init__(self):
        self.downloadedPgNum = self.readfromsettings()
    
    def readfromsettings(self):
        with open('settings.txt', 'r') as rd:
            pgnum = int(rd.read())
        return pgnum

    def getDocsInfo(self, pageSize=10000):
        totalpageNum = 3443256   # range(3443256)   3443255*10 + 最后一页文章数 = 总文章数
        pgSize1000totalNum = totalpageNum * 10 / pageSize       # 当前模式下的总页数
        finalpageSize = (totalpageNum * 10) % pageSize                 # 最后一页的文章数
        for pgNum in range(self.downloadedPgNum, pgSize1000totalNum):
            try:
                    docsInfoDict = self.getAjaxData(pgNum, pageSize)
                    self.downloadedPgNum = pgNum   # 这一句的位置不能和前一句调换
                    print "got page:" + str(pgNum) + ", Go Next!"
                    with open('DocsInfo3.txt', 'a') as wr:
                        wr.write(docsInfoDict)
                        wr.write('\n')
            except Exception as e:
                print traceback.format_exc()
                with open('settings.txt', 'w') as wr:
                    wr.write(str(self.downloadedPgNum))
                return False

        if self.downloadedPgNum == pgSize1000totalNum -1:
            try:
                docsInfoDict = self.getAjaxData(pgSize1000totalNum, finalpageSize)
                with open('DocsInfo3.txt', 'a') as wr:
                    wr.write(docsInfoDict)
            except Exception as e:
                print "最后一页读取失败...."
                return False
        return True
        pass


    def testIfPgszchgd(self):  # 测试如果每页大小改变了, 会不会导致下载重复的问题
        pageNum = [0, 1]
        finallist = []
        for i in pageNum:
            tmplist = self.getAjaxData(i, 100)
            anslist = []
            for j in tmplist:
                anslist.append(j["mdIdnt"])
            # print anslist
            finallist.append(anslist)
        samecode = [i for i in finallist[0] if i in finallist[1]]
        print samecode
        pass

    def getAjaxData(self, pageIndex, pageSize=10):
        data = {
            "area":"",
            "auther":"",
            "filetype": 0,
            "idContOrg":"",
            "language":"",
            "mdTitle":"",
            "pageIndex":pageIndex,
            "pageSize":pageSize,
            "sortType":"",
            "year":"",
        }
        data_encode = urllib.urlencode(data)  #　请求数据的格式不太一致
        # print data_encode
        # res = requests.post(self.aurl, data=data_encode)
        # print res.status_code
        # with open('tmppage.txt', 'w') as wr:
        #     wr.write(res.text)

        httpClient = None
        try:
            httpClient = httplib.HTTPConnection("catalog.ngac.org.cn", 80, timeout=20)
            httpClient.request(method="POST", url=self.aurl, body=data_encode, headers=self.headers)
            response = httpClient.getresponse()
            # with open('tmppage.txt', 'w') as wr:
            #     wr.write(response.read())
            tmppage = response.read()
            # jsonbuf = json.loads(tmppage)
            # mlist = jsonbuf["documents"]
            # totalpages = jsonbuf["pageTotal"]
            # print response.status
            # print response.reason
            # print response.version
            print response.read()
            # print response.getheaders() # 获取头信息
        except Exception, e:
            print traceback.format_exc()
            return False
        finally:
            if httpClient:
                httpClient.close()
        # print  totalpages
        # return mlist  # len(mlist), totalpages
        return tmppage
        pass

if __name__ == '__main__':
    tmp = getDocsByPost()
    # print tmp.getAjaxData(1)
    tmp.getDocsInfo()

    # j = 0
    # for i in range(0, 10):
    #     print i
    #     j = i

    pass
