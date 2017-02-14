__author__ = 'sibia-10'
import tornado.web
import tornado.gen
from scheduling_module.social_scheduling import EntrySchedulingHandler

from pymongo import MongoClient
import global_settings
db_conn = global_settings.db_conn

class SchedulingHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self,platform,action,entity):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Credentials", "true")
        self.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        self.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")

        print('PLATFORM=====>',platform)

        data={'db':db_conn,'platform':platform}
        scheduling = EntrySchedulingHandler(data)
        handler=scheduling.getHandler()
        print('HANDLER====>',handler)
        func=getattr(handler,action)
        print('FUNC======>',func)
        ret=yield tornado.gen.Task(func,self,platform)
        self.finish(ret)



