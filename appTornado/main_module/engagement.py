__author__ = 'Mrinmoy Ghoshal'
import tornado.web
import tornado.gen
from engagement_module.tw_engagement import TwitterEngagement
from engagement_module.tw_engagement import FacebookEngagement
from engagement_module.tw_engagement import InstagramEngagement
from engagement_module.tw_engagement import VkEngagement
from engagement_module.tw_engagement import LinkedinEngagement

from pymongo import MongoClient
import global_settings

db_conn = global_settings.db_conn

class EngagementHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self,platform,action,entity):
        #if platform is not 'Twitter': self.finish('Error')
        #self.write(platform)
        if platform == 'Tw':
            platform = 'Twitter'
        elif platform == 'Fb':
            platform = 'Facebook'
        elif platform == 'In':
            platform = 'Linkedin'
        elif platform == 'Inst':
            platform = 'Instagram'
        else:
            pass
        print("platform==============================",platform)
        print("action==============================",action)
        print("entity==============================",entity)
        serviceClassName=platform+'Engagement'
        print("serviceClassName==============================",serviceClassName)
        user_id=self.get_argument('user_id')
        print("user_id==============================",user_id)
        page_id=self.get_argument('page_id')
        print("page_id==============================",page_id)
        constructor = globals()[serviceClassName]
        print("constructor==============================",constructor)
        data={'db':db_conn,'user_id':user_id,'page_id':page_id}
        print("data==============================",data)
        instance = constructor(data)
        print("instance==============================",instance)
        func=getattr(instance,action)
        print("func==============================",func)
        ret=yield tornado.gen.Task(func,self)
        self.finish(ret)



#