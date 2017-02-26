# -*- coding:utf-8 -*-

import MySQLdb
import traceback

class writeDocsInfotoDb():
    
    def readDocsInfo():
        with open('DocsInfo.txt','r') as rd:
            line =  rd.readline()
        # print line
        with open('DocsInfo_0.txt', 'w') as wr:
            wr.write(line)

    pass

class DBOperation():
    """ ***********文章数据的结构*****************
    18 ["documents":[{文章dict},{文章dict}...], "pageTotal": totalpagenum]
    19 "文章dict"结构如下:
    20 {        

    21         abs
    22         auther
    23         formTime
    24         geoInfoName
    25         hiName 
    26         idContOrg 
    27         mdIdnt      # 用作主键   
    28         mdTitle 
    29         mineralCode
    30 }
    31
    """ 

    def __init__(self, SourcePath, User, Passwd, databaseNamejw):
        self.WDI = writeDocsInfotoDb()
        self.dbop = MySQLdb.connect(SourcePath,User, Password, charset='utf8')
        self.cursor = self.dbop.cursor()


    def createTable():
        sql = """create table NGACDocsInfo(
        abs TEXT,
        auther varchar(150),
        formTime varchar(150),
        geoInfoName varchar(150),
        hiName varchar(150),
        idContOrg varchar(150),        
        mdIdnt varchar(100) primary key,
        mdTitle varchar(150),
        mineralCode varchar(150),
        )        
        """
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except:
            print "create table NGACDocsInfo failed !"
            print traceback.format_exc()
            self.db.rollback()

        pass

    def __del__(self):
        self.cursor.close()
        self.dbop.commit()
        pass

    def InsertDocsInfoTotable():
        sql = "INSERT IGNORE INTO NGACDocsInfo 
        pass




if __name__ == '__main__':
    readDocsInfo()
