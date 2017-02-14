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
import global_settings

db_conn = global_settings.db_conn

class LinkedinScheduling(object):

    def createShare(self,req,platform,callback):

        req.set_header("Access-Control-Allow-Origin", "*")
        req.set_header("Access-Control-Allow-Credentials", "true")
        req.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        req.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")

        self.http_client = AsyncHTTPClient()
        self.user_id = req.get_argument('user_id')
        self.db_conn = global_settings.db_conn
        #here msg is a dictionary
        self.message = {
            'user_id' : req.get_argument('user_id'),
            'page_id' : req.get_argument('page_id'),
            'scheduled_date' : req.get_argument('date'),
            'scheduled_time' : req.get_argument('time'),
            #'pic' : req.get_argument('pic'),
            'created_at' : datetime.datetime.now(),
            'status' : 'active',
            'platform' : platform,
            'returnedValue' : ''
        }

        res=self.db_conn.sociabyte.user.find({'_id':req.get_argument('user_id')},{'brands':1})
        from bson.objectid import ObjectId
        ids=[ObjectId(a) for a in res[0]['brands']]
        #print('IDS_BRAND=====>',ids)
        access_token_result = self.db_conn.sociabyte.brands.find({"associated_accounts.in_accounts.page_id":req.get_argument('page_id'),'_id':{'$in':ids}}, {"associated_accounts.in_accounts.token":1,"associated_accounts.in_accounts.page_id":1, "_id": 0})
        access_token = ''
        counter = 0
        for acc in access_token_result:
            for acc_unit in acc['associated_accounts']['in_accounts']:
                if acc['associated_accounts']['in_accounts'][counter]['page_id'] == req.get_argument('page_id'):
                    access_token = acc['associated_accounts']['in_accounts'][counter]['token']

                else:
                    pass
                counter = counter+1

        self.message['token'] = access_token

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
            print("USER TIMEZONE======>",ut)
        ut = pytz.timezone(ut)
        user_time = datetime.datetime(dlist[0],dlist[1],dlist[2],dlist[3],dlist[4],dlist[5],tzinfo = ut)
        sys_timezone = get_localzone()
        sys_time = user_time.astimezone(sys_timezone)
        print('SYSTEM TIME=====>',sys_time)

        self.scheduled_timestamp = (sys_time - datetime.datetime(1970, 1, 1, 0, 0, 0,tzinfo = pytz.UTC)).total_seconds()
        print('SCHEDULED TIMESTAMP=======>',self.scheduled_timestamp)
        self.message['scheduled_timestamp'] = self.scheduled_timestamp

        print(self.message)
        self.db_conn.sociabyte.scheduling.insert(self.message)
        callback('Done')