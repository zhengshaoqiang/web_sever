#!/usr/bin/env python
#-*- coding:utf-8 -*-

import impala
import  pandas as pd
from impala.dbapi import connect


def  _check_one(info=None):
    if not info:
        return None
    elif len(info) > 1:
        raise Exception("Multiple rows returned for Database.get() query")
    else:
        return info[0]

class hivehelper(object):
    '''
    when query recomand u use query_dataframe function
    when dml action recomand u use execute function
    '''
    def __init__(self, host=None, port=None):
        HIVE_HOST = '10.10.166.56'
        HIVE_PORT = 10000
        if not host and not port:
            host=HIVE_HOST
            port=HIVE_PORT
        self.set_host(host)
        self.set_port(port)
        self._db = None
        self.cursor = None
        self.connect()

    def __del__(self):
        self.close()

    def set_host(self, host):
        self.host = host

    def get_host(self):
        return self.host

    def set_port(self, port):
        self.port = port

    def get_port(self):
        return self.port

    def reconnect(self):
        self.close()
        self.connect()

    def connect(self):
        _base_ = {"host": self.get_host(),
                  "port": self.get_port(),
                  "auth_mechanism" :'PLAIN'
                 }
        self._db = connect(**_base_)
        self.cursor = self._cursor()

    def close(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self._db:
            self._db.close()
            self._db = None

    def _cursor(self):
        if self._db:
            return self._db.cursor()
        else:
            raise Exception("Impala not connect")

    def raw_query(self, query, **kwargs):
        '''
        :param query:your sql string
        :param kwargs:
        :return: (table_keys,table_values) table_keys is your columns and table_values is your multi rows results
        '''
        self.cursor.execute(query, parameters=kwargs)
        table_keys = [keys[0] for keys in self.cursor.description]
        table_vales = [value for value in self.cursor]
        return table_keys,table_vales

    def query_dataframe(self,query,**kwargs):
        '''
        :param query:your sql string
        :param kwargs:
        :return: multi rows with dataframe format
        '''
        table_keys, table_vales = self.raw_query(query=query,**kwargs)
        table_keys = [col.split(".")[1:][0] for col in table_keys]
        df = pd.DataFrame.from_records(data=table_vales,columns=table_keys)
        return df

    def get(self, query, **kwargs):
        '''
        :param query:your sql
        :param kwargs:
        :return: one row with dict format
        '''

        rows = self.query(query, **kwargs)
        return _check_one(rows)

    def one(self, query, **kwargs):
        '''
        :param query:your sql string
        :param kwargs:
        :return: the first row of result
        '''
        rows = self.onelist(query, **kwargs)
        return _check_one(rows)

    def query(self, query, **kwargs):
        keys, values = self.raw_query(query, **kwargs)
        return [dict(zip(keys, value)) for value in values]

    def onelist(self, query, **kwargs):
        '''
        :param query:your sql string
        :param kwargs:
        :return: multi rows without column names
        '''
        _, values = self.raw_query(query, **kwargs)
        return values

    def oneset(self, query, **kwargs):
        return set(self.onelist(query, **kwargs))

    def execute(self, query, **kwargs):
        '''
        :param query:your sql string
        :param kwargs:
        :return: None just do dml sql action
        '''
        self.cursor.execute(query, parameters=kwargs)
        return None

def main():
    import json
    c = ImpalaWapper("192.168.1.97")
    while 1:
        tmp = raw_input("impala>>").strip()
        if not tmp:
            continue
        if tmp.lower() in ["q", "quit", "bye"]:
            print "exit client"
            break
        elif tmp.lower() in ["help", "h"]:
            print "COMMAND "
            print "[search] execute get one onelist oneset query raw_query "
            print ">>> query show tables <--> show tables"
            print "[help] help"
            print "[quit] quit bye"
            print "[defult] query"
            continue
        cmd = "query"
        try:
            cmd, sql = tmp.split(" ", 1)
        except ValueError, e:
            sql = tmp

        if cmd not in dir(c):
            cmd = "query"
            sql = tmp
        try:
            print cmd, sql
            data = eval("c.{0}(\"{1}\")".format(cmd, sql))
        except impala.error.HiveServer2Error, e:
            print "[error], %s" % str(e)
            print tmp
            continue
        except Exception, e:
            print str(e)
            continue
        if data is None:
            continue

        try:
            #print json.dumps(data, indent=4, ensure_ascii=False)
            print json.dumps(data, ensure_ascii=False)
        except Exception, e:
            print e, data

if __name__ == "__main__":
    t = hivehelper()
    #查询操作
    sql = """select * from sale_kpi 
        where firm='TMK' """
    df=t.query_dataframe(sql)
    print df
    #dml操作
    # sql = "drop table if EXISTS  tmp_test2"
    # t.execute(sql)
