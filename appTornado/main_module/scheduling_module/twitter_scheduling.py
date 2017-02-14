__author__ = 'sibia-10'

import json
import urllib
import datetime
from pymongo import MongoClient
from urlparse import urlparse
from tornado.httpclient import AsyncHTTPClient
import base64
import pytz
from tzlocal import get_localzone
import os
import time
import global_settings

class TwitterScheduling(object):

    def addTweet(self,req,platform,callback):

        req.set_header("Access-Control-Allow-Origin", "*")
        req.set_header("Access-Control-Allow-Credentials", "true")
        req.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        req.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")

        self.http_client = AsyncHTTPClient()
        self.user_id = req.get_argument('user_id')
        self.db_conn = global_settings.db_conn
        #here msg is a dictionary
        page_id =  req.get_argument('page_id')
        pic_flag = req.get_argument('pic_flag')
        page_name = req.get_argument('page_name')
        page_avatar = req.get_argument('page_avatar')
        user_type = req.get_argument('user_type')
        if pic_flag == 'true':
            self.message = {
                'user_id' : req.get_argument('user_id'),
                'page_id' : req.get_argument('page_id'),
                'scheduled_date' : req.get_argument('date'),
                'scheduled_time' : req.get_argument('time'),
                'created_at' : datetime.datetime.now(),
                'status' : 'active',
                'platform' : platform,
                'returnedValue' : '',
                'user_type':user_type.split(','),
                'page_name':page_name,
                'page_avatar':page_avatar
            }
            path = '/home/ubuntu/sociabyte/sociabyte_app/public/upload/schedule_pics'
            fileinfo = req.request.files['datafile'][0]
            #print(fileinfo)
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
            access_token_result = self.db_conn.sociabyte.brands.find({"associated_accounts.tw_accounts.page_id":req.get_argument('page_id'),'_id':{'$in':ids}}, {"associated_accounts.tw_accounts.token":1,"associated_accounts.tw_accounts.page_id":1, "_id": 0, "associated_accounts.tw_accounts.tokenSecret":1})
            access_token = ''
            tokenSecret = ''
            for acc in access_token_result:
                counter = 0
                for acc_unit in acc['associated_accounts']['tw_accounts']:
                    if acc['associated_accounts']['tw_accounts'][counter]['page_id'] == req.get_argument('page_id'):
                        access_token = acc['associated_accounts']['tw_accounts'][counter]['token']
                        tokenSecret= acc['associated_accounts']['tw_accounts'][counter]['tokenSecret']

                    else:
                        pass
                    counter = counter+1
            self.message['token'] = access_token
            self.message['tokenSecret'] = tokenSecret
            self.message['pic'] = final_file_name
            msg = req.get_argument('msg')
            m = msg.split()
            posDictList = []
            linkList = []
            for string in m:
                posDict = {'string' : string,
                           'position' : m.index(string)}
                posDictList.append(posDict)
            def funLink(a):
                parse_url = urlparse(str(a))
                if ((parse_url[0] == 'http') or (parse_url[0] == 'https')) and (parse_url[1] != ''):
                    return a
                else:
                    return None
            def funMain(each):
                if each == None:
                    pass
                else:
                    linkList.append(each)
            link = [ funMain(each) for each in [funLink(a) for a in m]]
            # url shortening and update the message
            for l in linkList:
                for i in range(len(posDictList)):
                    if l == posDictList[i]['string']:
                        l_position = posDictList[i]['position']
                        bitly_token = self.db_conn.sociabyte.bitly.find({'user_id' : self.user_id},{'access_token' : 1,'_id' : 0})
                        bitly = ''
                        for acs_bitly in bitly_token:
                            bitly = acs_bitly['access_token']

                        short_url = urllib.urlopen('https://api-ssl.bitly.com/v3/shorten?longUrl='+l+'&format=json&access_token='+bitly)
                        for info_url in short_url:
                            info_url = json.loads(info_url)
                            url = info_url['data']['url']
                            m[l_position] = url
                    else:
                        pass

            modified_msg = ' '.join(m)
            self.message['msg'] = modified_msg

            sc_date = req.get_argument('date')
            sc_time = req.get_argument('time')
            date_time = sc_date+' '+ sc_time
            date_time_new = datetime.datetime.strptime(date_time,"%m/%d/%Y %I:%M %p")
            tt = date_time_new.timetuple()
            dlist = []
            for i in tt:
                dlist.append(i)
            utz = self.db_conn.sociabyte.user.find({'_id' : str(self.user_id)},{'address.timezone' : 1,'_id' : 0})
            ut = ''
            for utimezone in utz:
                ut = utimezone['address']['timezone']
            ut = pytz.timezone(ut)
            user_time = datetime.datetime(dlist[0],dlist[1],dlist[2],dlist[3],dlist[4],dlist[5],tzinfo = ut)
            sys_timezone = get_localzone()
            sys_time = user_time.astimezone(sys_timezone)
            self.scheduled_timestamp = int((sys_time - datetime.datetime(1970, 1, 1, 0, 0, 0,tzinfo = pytz.UTC)).total_seconds())-660
            print "scheduled_timestamp", self.scheduled_timestamp
            self.message['scheduled_timestamp'] = self.scheduled_timestamp
            current_timestamp = time.time()
            if self.scheduled_timestamp <= current_timestamp:
                callback('Invalid Time')
            else:
                print(self.message)
                self.db_conn.sociabyte.scheduling.insert(self.message)
                callback('Done')
        else:
            self.message = {
                'user_id' : req.get_argument('user_id'),
                'page_id' : req.get_argument('page_id'),
                'scheduled_date' : req.get_argument('date'),
                'scheduled_time' : req.get_argument('time'),
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
            access_token_result = self.db_conn.sociabyte.brands.find({"associated_accounts.tw_accounts.page_id":req.get_argument('page_id'),'_id':{'$in':ids}}, {"associated_accounts.tw_accounts.token":1,"associated_accounts.tw_accounts.page_id":1, "_id": 0, "associated_accounts.tw_accounts.tokenSecret":1})
            access_token = ''
            tokenSecret = ''
            for acc in access_token_result:
                counter = 0
                for acc_unit in acc['associated_accounts']['tw_accounts']:
                    if acc['associated_accounts']['tw_accounts'][counter]['page_id'] == req.get_argument('page_id'):
                        access_token = acc['associated_accounts']['tw_accounts'][counter]['token']
                        tokenSecret= acc['associated_accounts']['tw_accounts'][counter]['tokenSecret']

                    else:
                        pass
                    counter = counter+1
            self.message['token'] = access_token
            self.message['tokenSecret'] = tokenSecret
            msg = req.get_argument('msg')
            m = msg.split()
            posDictList = []
            linkList = []
            for string in m:
                posDict = {'string' : string,
                           'position' : m.index(string)}
                posDictList.append(posDict)
            def funLink(a):
                parse_url = urlparse(str(a))
                if ((parse_url[0] == 'http') or (parse_url[0] == 'https')) and (parse_url[1] != ''):
                    return a
                else:
                    return None
            def funMain(each):
                if each == None:
                    pass
                else:
                    linkList.append(each)
            link = [ funMain(each) for each in [funLink(a) for a in m]]
            # url shortening and update the message
            for l in linkList:
                for i in range(len(posDictList)):
                    if l == posDictList[i]['string']:
                        l_position = posDictList[i]['position']
                        bitly_token = self.db_conn.sociabyte.bitly.find({'user_id' : self.user_id},{'access_token' : 1,'_id' : 0})
                        bitly = ''
                        for acs_bitly in bitly_token:
                            bitly = acs_bitly['access_token']

                        short_url = urllib.urlopen('https://api-ssl.bitly.com/v3/shorten?longUrl='+l+'&format=json&access_token='+bitly)
                        for info_url in short_url:
                            info_url = json.loads(info_url)
                            url = info_url['data']['url']
                            m[l_position] = url
                    else:
                        pass

            modified_msg = ' '.join(m)
            self.message['msg'] = modified_msg
            sc_date = req.get_argument('date')
            sc_time = req.get_argument('time')
            date_time = sc_date+' '+ sc_time
            date_time_new = datetime.datetime.strptime(date_time,"%m/%d/%Y %I:%M %p")
            tt = date_time_new.timetuple()
            dlist = []
            for i in tt:
                dlist.append(i)
            utz = self.db_conn.sociabyte.user.find({'_id' : str(self.user_id)},{'address.timezone' : 1,'_id' : 0})
            ut = ''
            for utimezone in utz:
                ut = utimezone['address']['timezone']
            ut = pytz.timezone(ut)
            user_time = datetime.datetime(dlist[0],dlist[1],dlist[2],dlist[3],dlist[4],dlist[5],tzinfo = ut)
            sys_timezone = get_localzone()
            sys_time = user_time.astimezone(sys_timezone)
            self.scheduled_timestamp = int((sys_time - datetime.datetime(1970, 1, 1, 0, 0, 0,tzinfo = pytz.UTC)).total_seconds())-660
            self.message['scheduled_timestamp'] = self.scheduled_timestamp
            print "scheduled_timestamp", self.scheduled_timestamp
            current_timestamp = time.time()
            if self.scheduled_timestamp <= current_timestamp:
                callback('Invalid Time')
            else:
                self.db_conn.sociabyte.scheduling.insert(self.message)
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
        posts = db.sociabyte.scheduling.find({'status' : 'active','platform' : str(platform), 'page_id':str(page_id)},{'_id' : 0})
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


    def viewPublishedPost(self,req,platform,callback):

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
        pubPostList = []
        pubPost = {
            'scheduled_date' : '',
            'msg' : '',
            'scheduled_time' : '',
            'pic' : ''
        }
        for each_post in posts:
            pubPost = {
                'scheduled_date' : each_post['scheduled_date'],
                'msg' : each_post['msg'],
                'scheduled_time' : each_post['scheduled_time'],
                'pic' : each_post['pic']
            }


            pubPostList.append(pubPost)
        print('LIST======>',pubPostList)
        callback(json.dumps(pubPostList))



