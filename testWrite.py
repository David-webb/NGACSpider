# -*- coding:utf-8 -*-

def givelinestoDocsInfo(sourcefile, objfile):
    with open(sourcefile, 'r') as rd:
        tmpstr = rd.read()
    
    tmpstr.replace('{"documents":','\n{"documents"')

    with open(objfile, 'w') as wr:
        wr.write(tmpstr)

def tryreadLargefile():
    for i in open('tmppage.txt', 'r'):
        print i
        break


if __name__ == '__main__':
    # tryreadLargefile()
    #givelinestoDocsInfo('tmppage.txt', 'tmppage2.txt')
    givelinestoDocsInfo('DocsInfo.txt', 'DocsInfo2.txt')
    #with open('tmppage2.txt','r') as rd:
    #    tmpstr = rd.read()
    #with open('tmppage.txt','a')as wr:
    #    wr.write(tmpstr)
