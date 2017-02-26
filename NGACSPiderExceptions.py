# -*- coding:utf-8 -*-


class ProxyPoolDBEmptyException(Exception):
    def __init__(self):
        sef.Message =  "Proxy Ips have been run out ,waiting for the latest updating, it could cost a couple minutes..."
    pass
