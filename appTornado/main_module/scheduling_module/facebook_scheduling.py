__author__ = 'sibia-10'

import urllib
import datetime
import json
from pymongo import MongoClient
from tornado.httpclient import AsyncHTTPClient
import time
import os
import pytz
from tzlocal import get_localzone
import global_settings

db_conn = global_settings.db_conn

class FacebookScheduling(object):


    def addPost(self,req,platform,callback):
        req.set_header("Access-Control-Allow-Origin", "*")
        req.set_header("Access-Control-Allow-Credentials", "true")
        req.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        req.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")
        http_client=AsyncHTTPClient()
        self.db_conn = db_conn
        page_id =  req.get_argument('page_id')
        page_name = req.get_argument('page_name')
        page_avatar = req.get_argument('page_avatar')
        user_type = req.get_argument('user_type')
        #here msg is a dictionary
        #here msg is a dictionary
        pic_flag = req.get_argument('pic_flag')
        print('PIC_FLAG========>>>>>>>',pic_flag)
        if pic_flag == 'true':
            self.message = {
                'user_id' : req.get_argument('user_id'),
                'page_id' : req.get_argument('page_id'),
                'scheduled_date' : req.get_argument('date'),
                'scheduled_time' : req.get_argument('time'),
                'msg' : req.get_argument('msg'),
                'link' : req.get_argument('link'),
                'created_at' : datetime.datetime.now(),
                'status' : 'active',
                'platform' : platform,
                'returnedValue' : '',
                'user_type':user_type.split(','),
                'page_name':page_name,
                'page_avatar':page_avatar
            }

            fileinfo = req.request.files['datafile'][0]

            path = '/home/ubuntu/sociabyte/sociabyte_app/public/upload/schedule_pics'

            fbody = (fileinfo['body'])
            file_name = fileinfo['filename']
            f = file_name.split('.')
            f_length = len(f)
            extensn = f[f_length - 1]
            final_file_name = str(page_id)+'_'+str(int(time.time()))+'.'+str(extensn)
            os.chdir(path)
            output_file = open(final_file_name, 'w')
            output_file.write(fbody)

            res=self.db_conn.sociabyte.user.find({'_id':req.get_argument('user_id')},{'brands':1})
            from bson.objectid import ObjectId
            ids=[ObjectId(a) for a in res[0]['brands']]
            #print('IDS_BRAND=====>',ids)
            access_token_result = self.db_conn.sociabyte.brands.find({"associated_accounts.fb_accounts.page_id":req.get_argument('page_id'),'_id':{'$in':ids}}, {"associated_accounts.fb_accounts.token":1,"associated_accounts.fb_accounts.page_id":1, "_id": 0, "associated_accounts.fb_accounts.name":1})
            access_token = ''

            for acc in access_token_result:
                counter = 0
                for acc_unit in acc['associated_accounts']['fb_accounts']:
                    if acc['associated_accounts']['fb_accounts'][counter]['page_id'] == req.get_argument('page_id'):
                        access_token = acc['associated_accounts']['fb_accounts'][counter]['token']
                        page_name= acc['associated_accounts']['fb_accounts'][counter]['name']

                    else:
                        pass
                    counter = counter+1
            print(access_token)
            sc_date = req.get_argument('date')
            sc_time = req.get_argument('time')
            date_time = sc_date+' '+ sc_time
            date_time_new = datetime.datetime.strptime(date_time,"%m/%d/%Y %I:%M %p")
            tt = date_time_new.timetuple()
            dlist = []
            for i in tt:
                dlist.append(i)
            utz = self.db_conn.sociabyte.user.find({'_id' : str(req.get_argument('user_id'))},{'address.timezone' : 1,'_id' : 0})
            ut = ''
            for utimezone in utz:
                ut = utimezone['address']['timezone']
            ut = pytz.timezone(ut)
            user_time = datetime.datetime(dlist[0],dlist[1],dlist[2],dlist[3],dlist[4],dlist[5],tzinfo = ut)
            sys_timezone = get_localzone()
            sys_time = user_time.astimezone(sys_timezone)
            self.scheduled_timestamp = int((sys_time - datetime.datetime(1970, 1, 1, 0, 0, 0,tzinfo = pytz.UTC)).total_seconds())-660
            print('DATE_TIME_NEW=========>',date_time_new)
            #self.scheduled_timestamp = (date_time_new - datetime.datetime(1970, 1, 1, 0, 0)).total_seconds()
            print('TIMESTAMP====>',self.scheduled_timestamp)
            current_timestamp = time.time()
            if self.scheduled_timestamp <= current_timestamp:
                callback('Invalid Time')
            else:
                self.message['pic'] = final_file_name
                self.message['scheduled_timestamp'] = self.scheduled_timestamp
                self.message['access_token'] = access_token

                self.db_conn.sociabyte.scheduling.insert(self.message)

                picture_location = '/home/ubuntu/sociabyte/sociabyte_app/public/upload/schedule_pics/'+final_file_name
                photo = open(picture_location,'rb')

                endpoint='https://graph.facebook.com/v2.1/'+str(req.get_argument('page_id'))+'/feed'
                print('ENDPOINT====>',endpoint)
                #data={'access_token' : str(access_token), 'message' : str(req.get_argument('msg')), 'link ': str(req.get_argument('link')), 'picture' : str(req.get_argument('pic')), 'scheduled_publish_time' : int(scheduled_timestamp), 'published' : 'false'}
                data={'access_token' : str(access_token), 'message' : str(req.get_argument('msg')),'published' : 'false', 'link': str(req.get_argument('link')) ,'link.picture' : photo,'scheduled_publish_time' : int(self.scheduled_timestamp)}
                encodedData=urllib.urlencode(data)
                print('ENCODED==>',encodedData)

                def handle_request(response):
                    if response.error:
                        print ("Errors:", response.error)
                    else:
                        print('RETURN IDDDDDD==========>',json.loads(response.body)['id'])
                        self.db_conn.sociabyte.scheduling.update({'scheduled_timestamp' : self.scheduled_timestamp, 'platform' : str(platform)},{'$set':{'returnedValue':json.loads(response.body)['id'],'status' : 'inactive'}})
                http_client.fetch(endpoint,handle_request, method='POST', headers= {'Content-Type':'application/x-www-form-urlencoded'}, body=encodedData)
                #self.message['status'] = 'inactive'
                print(self.message)
                callback('Done')
        else:

            self.message = {
                'user_id' : req.get_argument('user_id'),
                'page_id' : req.get_argument('page_id'),
                'scheduled_date' : req.get_argument('date'),
                'scheduled_time' : req.get_argument('time'),
                'msg' : req.get_argument('msg'),
                'link' : req.get_argument('link'),
                'created_at' : datetime.datetime.now(),
                'status' : 'active',
                'platform' : platform,
                'returnedValue' : '',
                'pic' : 'no_image.png',
                'user_type':user_type.split(','),
                'page_name':page_name,
                'page_avatar':page_avatar
            }
            res=self.db_conn.sociabyte.user.find({'_id':req.get_argument('user_id')},{'brands':1})
            from bson.objectid import ObjectId
            ids=[ObjectId(a) for a in res[0]['brands']]
            #print('IDS_BRAND=====>',ids)
            access_token_result = self.db_conn.sociabyte.brands.find({"associated_accounts.fb_accounts.page_id":req.get_argument('page_id'),'_id':{'$in':ids}}, {"associated_accounts.fb_accounts.token":1,"associated_accounts.fb_accounts.page_id":1, "_id": 0, "associated_accounts.fb_accounts.name":1})
            access_token = ''

            for acc in access_token_result:
                counter = 0
                for acc_unit in acc['associated_accounts']['fb_accounts']:
                    if acc['associated_accounts']['fb_accounts'][counter]['page_id'] == req.get_argument('page_id'):
                        access_token = acc['associated_accounts']['fb_accounts'][counter]['token']
                        page_name= acc['associated_accounts']['fb_accounts'][counter]['name']

                    else:
                        pass
                    counter = counter+1
            print(access_token)
            sc_date = req.get_argument('date')
            sc_time = req.get_argument('time')
            date_time = sc_date+' '+ sc_time
            date_time_new = datetime.datetime.strptime(date_time,"%m/%d/%Y %I:%M %p")
            tt = date_time_new.timetuple()
            dlist = []
            for i in tt:
                dlist.append(i)
            utz = self.db_conn.sociabyte.user.find({'_id' : str(req.get_argument('user_id'))},{'address.timezone' : 1,'_id' : 0})
            ut = ''
            for utimezone in utz:
                ut = utimezone['address']['timezone']
            ut = pytz.timezone(ut)
            user_time = datetime.datetime(dlist[0],dlist[1],dlist[2],dlist[3],dlist[4],dlist[5],tzinfo = ut)
            sys_timezone = get_localzone()
            sys_time = user_time.astimezone(sys_timezone)
            self.scheduled_timestamp = int((sys_time - datetime.datetime(1970, 1, 1, 0, 0, 0,tzinfo = pytz.UTC)).total_seconds())-660
            print('DATE_TIME_NEW=========>',date_time_new)
            #self.scheduled_timestamp = (date_time_new - datetime.datetime(1970, 1, 1, 0, 0)).total_seconds()
            print('TIMESTAMP====>',self.scheduled_timestamp)
            current_timestamp = time.time()
            print('CURRENT ================================>',current_timestamp)
            if self.scheduled_timestamp <= current_timestamp:
                callback('Invalid Time')
            else:
                self.message['scheduled_timestamp'] = self.scheduled_timestamp
                self.message['access_token'] = access_token

                self.db_conn.sociabyte.scheduling.insert(self.message)


                endpoint='https://graph.facebook.com/v2.1/'+str(req.get_argument('page_id'))+'/feed'
                print('ENDPOINT====>',endpoint)
                #data={'access_token' : str(access_token), 'message' : str(req.get_argument('msg')), 'link ': str(req.get_argument('link')), 'picture' : str(req.get_argument('pic')), 'scheduled_publish_time' : int(scheduled_timestamp), 'published' : 'false'}
                data={'access_token' : str(access_token), 'message' : str(req.get_argument('msg')),'published' : 'false', 'link': str(req.get_argument('link')) ,'scheduled_publish_time' : int(self.scheduled_timestamp)}
                encodedData=urllib.urlencode(data)
                print('ENCODED==>',encodedData)

                def handle_request(response):
                    if response.error:
                        print ("Errors:", response.error)
                    else:
                        print('RETURN IDDDDDD==========>',json.loads(response.body)['id'])
                        self.db_conn.sociabyte.scheduling.update({'scheduled_timestamp' : self.scheduled_timestamp, 'platform' : str(platform)},{'$set':{'returnedValue':json.loads(response.body)['id'],'status' : 'inactive'}})
                http_client.fetch(endpoint,handle_request, method='POST', headers= {'Content-Type':'application/x-www-form-urlencoded'}, body=encodedData)
                #self.message['status'] = 'inactive'
                print(self.message)
                callback('Done')


    def viewScheduledPost(self,req,platform,callback):

        req.set_header("Access-Control-Allow-Origin", "*")
        req.set_header("Access-Control-Allow-Credentials", "true")
        req.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        req.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")

        user_id = req.get_argument('user_id')
        page_id = req.get_argument('page_id')

        print(user_id)
        print(page_id)

        db = global_settings.db_conn
        posts = db.sociabyte.scheduling.find({'status' : 'inactive','platform' : str(platform), 'page_id':str(page_id)},{'_id' : 0})
        print(posts)
        scheduledPostList = []
        scheduledPost = {
            'scheduled_date' : '',
            'msg' : '',
            'scheduled_time' : '',
            'pic' : ''
        }
        for each_post in posts:
            scheduledPost = {
                'scheduled_date' : each_post['scheduled_date'],
                'msg' : each_post['msg'],
                'scheduled_time' : each_post['scheduled_time'],
                'pic' : each_post['pic']
            }
            scheduledPostList.append(scheduledPost)
        print('LIST======>',scheduledPostList)
        callback(json.dumps(scheduledPostList))



