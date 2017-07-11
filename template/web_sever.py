#coding:utf-8

import os.path
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import time
from datetime import datetime, timedelta
import os
import hive_helper

from tornado.options import define, options
define("port", default=2062, help="run on the given port", type=int)

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("index1.html")

class UserHandler(tornado.web.RequestHandler):
    def post(self):
        user_name = self.get_argument("username")
        name = str(user_name.encode("utf-8"))
        #name = str(user_name.encode('gb18030'))
        #print name
        #print 'name:',name
        # 查询操作
        #print name
        #name1 = "上海销售中心"
        sql = """select * from sale_kpi where firm='{firm}'""".format(firm=name)
        from hive_helper import hivehelper
        t = hivehelper()
        df = t.query_dataframe(sql)
        print df
        '''
        name1 = "上海销售中心"
        sql = """select * from sale_kpi where firm='{firm}'""".format(firm=name1)
        t = hive_helper.hivehelper()
        file = t.raw_query(sql)
        print file
        '''

        self.render("index.html", file=df)

handlers = [
    (r"/", IndexHandler),
    (r"/user", UserHandler)

]


template_path = os.path.join(os.path.dirname(__file__),"template")

if __name__ == "__main__":
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers, template_path)
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()




'''

'''


