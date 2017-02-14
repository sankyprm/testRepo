__author__ = 'sibia-10'
import json
import urllib
from datetime import datetime
from pymongo import MongoClient
import global_settings

db_conn = global_settings.db_conn

class VkScheduling(object):

    def addPost(self,req,callback):

        #here msg is a dictionary
        msg = json.loads(req.get_argument('msg'))

        user_id = req.get_argument('user_id')
        page_id = req.get_argument('page_id')
        platform = req.get_argument('platform')

        msg['user_id'] = user_id
        msg['media_acc_id'] = page_id
        msg['created_at'] = datetime.now()
        msg['platform'] = platform
        msg['schedule_date'] = ''
        msg['status'] = 'active'
        msg['returnedValue'] = ''

        print(msg)
        db_conn.sociabyte.scheduling.insert(msg)
        callback('Done')
