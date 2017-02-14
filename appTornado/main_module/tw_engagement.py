__author__ = 'Sibia'
from twython import Twython
import json
from datetime import datetime
import facebook
import urllib
from bson import ObjectId
import zmq
import time

class TwitterEngagement(object):

    def __init__(self,data):
        self.db=data['db']
        self.user_id=data['user_id']
        self.tw_user_id=data['page_id']
        self.complist = []
        res=self.db.sociabyte.user.find({'_id':self.user_id},{'brands':1})
        from bson.objectid import ObjectId
        ids=[ObjectId(a) for a in res[0]['brands']]
        access_token_result = self.db.sociabyte.brands.find({"associated_accounts.tw_accounts.page_id":self.tw_user_id,'_id':{'$in':ids}}, {"associated_accounts.tw_accounts.token":1,"associated_accounts.tw_accounts.page_id":1, "_id": 0, "associated_accounts.tw_accounts.tokenSecret":1,"associated_accounts.tw_accounts.name":1,"associated_accounts.tw_accounts.competitor":1})
        self.token = ''
        self.token_secret = ''
        for acc in access_token_result:
            counter = 0
            for acc_unit in acc['associated_accounts']['tw_accounts']:
                if acc['associated_accounts']['tw_accounts'][counter]['page_id'] == self.tw_user_id:
                    self.token = acc['associated_accounts']['tw_accounts'][counter]['token']
                    self.token_secret = acc['associated_accounts']['tw_accounts'][counter]['tokenSecret']
                    page_name= acc['associated_accounts']['tw_accounts'][counter]['name']
                    self.complist = acc['associated_accounts']['tw_accounts'][counter]['competitor']
                else:
                    pass
                counter = counter+1
        print(self.token)
        print(self.token_secret)
        self.twitter=Twython('Qwu1NiiV2uzGp3Hj0UqD1CKo2', '6CiVt3lDPnD4qOaccPOxpcPeTNkZ8h8ljelavK79xKsqpgEEYF', self.token, self.token_secret)
        pass


    def searchPage(self,req,callback):
        req.set_header("Access-Control-Allow-Origin", "*")
        req.set_header("Access-Control-Allow-Credentials", "true")
        req.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        req.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")
        username=req.get_argument('username')
        if username[0:8]=="https://":
            url_list = username.split( "/")
            screenName = url_list[3].split("?")[0]
        else:
            screenName=username
        try:
            res=self.twitter.lookup_user(screen_name=screenName)
            print(res)
            callback(json.dumps(res))
        except Exception as e:
            callback('NotFound')

    def addComp(self,req,callback):
        import tw_benchmark.tw_benchmark as tw_benchmark
        req.set_header("Access-Control-Allow-Origin", "*")
        req.set_header("Access-Control-Allow-Credentials", "true")
        req.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        req.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")
        id=req.get_argument('compId')
        print (id)
        brand_id = req.get_argument('brand_id')
        print (brand_id)
        #try:
        if id not in self.complist:
            self.db.sociabyte.brands.update({'associated_accounts.tw_accounts.page_id':self.tw_user_id, '_id':ObjectId(brand_id)}, {'$addToSet':{'associated_accounts.tw_accounts.$.competitor':id}})
            tw_benchmark.userTimeline(self.db.sociabyte, id, self.twitter, brand_id, self.tw_user_id)
            tw_benchmark.followersFetch(self.db.sociabyte,id, self.twitter, brand_id, self.tw_user_id)
            tw_benchmark.userInfo(self.db.sociabyte, id, self.twitter, brand_id, self.tw_user_id)
            tw_benchmark.followerDetails(self.db.sociabyte,id, self.twitter, brand_id, self.tw_user_id)
            tw_benchmark.dailyReachCalculator(self.db.sociabyte,id, brand_id, self.tw_user_id)
            tw_benchmark.genderClassifier(self.db.sociabyte,id)
            sendDict = {
                'acc_id':id,
                'code':'tw_comp'
            }
            sendDict = json.dumps(sendDict)
            port = '5572'
            context = zmq.Context()
            print ("Connecting to server...")
            socket = context.socket(zmq.PUB)
            socket.bind("tcp://*:%s" % port)
            print ("Sending request ")
            a=0

            while a<2:
                socket.send_multipart(['',sendDict])
                print "send successfull"
                a+=1
                time.sleep(1)
            #  Get the reply.
            socket.close()
            print "done====>>>"
            callback("Done")
        else:
            callback("existing")
        """except Exception as e:
            self.db.sociabyte.brands.update({'associated_accounts.tw_accounts.page_id':self.tw_user_id, '_id':ObjectId(brand_id)}, {'$pull':{'associated_accounts.tw_accounts.$.competitor':id}})
            print('Im in exception')
            callback('NotDone')"""
class FacebookEngagement(object):

    def __init__(self,data):
        self.db=data['db']
        self.user_id=data['user_id']
        self.fb_account_id=data['page_id']
        self.complist = []
        res=self.db.sociabyte.user.find({'_id':self.user_id},{'brands':1})
        from bson.objectid import ObjectId
        ids=[ObjectId(a) for a in res[0]['brands']]
        access_token_result = self.db.sociabyte.brands.find({"associated_accounts.fb_accounts.page_id":self.fb_account_id,'_id':{'$in':ids}}, {"associated_accounts.fb_accounts.token":1,"associated_accounts.fb_accounts.page_id":1, "_id": 0, "associated_accounts.fb_accounts.name":1, "associated_accounts.fb_accounts.competitor":1})
        self.access_token = ''

        for acc in access_token_result:
            counter = 0
            for acc_unit in acc['associated_accounts']['fb_accounts']:
                if acc['associated_accounts']['fb_accounts'][counter]['page_id'] == self.fb_account_id:
                    self.access_token = acc['associated_accounts']['fb_accounts'][counter]['token']
                    page_name= acc['associated_accounts']['fb_accounts'][counter]['name']
                    self.complist = acc['associated_accounts']['fb_accounts'][counter]['competitor']
                else:
                    pass
                counter = counter+1
        print(self.access_token)
        pass


    def searchPage(self,req,callback):
        req.set_header("Access-Control-Allow-Origin", "*")
        req.set_header("Access-Control-Allow-Credentials", "true")
        req.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        req.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")
        username=req.get_argument('username')
        if username[0:8]=="https://":
            url_list = username.split( "/")
            screenName = url_list[3].split("?")[0]
        else:
            screenName=username
        try:
            self.fb_handle = urllib.urlopen('https://graph.facebook.com/v2.2/'+screenName+'?access_token='+self.access_token)
            res = json.loads(self.fb_handle.read())
            callback(json.dumps(res))
        except Exception as e:
            res = {'error' : 'NotFound'}
            callback(json.dumps(res))

    def addComp(self,req,callback):
        import fb_benchmark.fb_benchmark as fb_benchmark
        req.set_header("Access-Control-Allow-Origin", "*")
        req.set_header("Access-Control-Allow-Credentials", "true")
        req.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        req.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")
        id=req.get_argument('compId')
        brand_id = req.get_argument('brand_id')
        #try:
        if id not in self.complist:
            self.db.sociabyte.brands.update({'associated_accounts.fb_accounts.page_id':self.fb_account_id, '_id':ObjectId(brand_id)}, {'$addToSet':{'associated_accounts.fb_accounts.$.competitor':id}})
            fb_handle = facebook.GraphAPI(self.access_token)
            fb_benchmark.userAccountsInfo(self.db.sociabyte, id, fb_handle, brand_id, self.fb_account_id)
            fb_benchmark.pagePosts(id, self.db.sociabyte, fb_handle, brand_id, self.fb_account_id)
            fb_benchmark.comments_fetch(id, self.db.sociabyte, fb_handle, brand_id, self.fb_account_id)
            fb_benchmark.insight_fetch(id, self.db.sociabyte, fb_handle, brand_id, self.fb_account_id)
            sendDict = {
                'acc_id':id,
                'code':'fb_comp'
            }
            sendDict = json.dumps(sendDict)
            port = '5571'
            context = zmq.Context()
            print ("Connecting to server...")
            socket = context.socket(zmq.PUB)
            socket.bind("tcp://*:%s" % port)
            print ("Sending request ")
            a=0

            while a<2:
                socket.send_multipart(['',sendDict])
                print "send successfull"
                a+=1
                time.sleep(1)
            #  Get the reply.
            socket.close()
            callback('Done')
        else:
            callback("existing")
        #except Exception as e:
        #    self.db.sociabyte.brands.update({'associated_accounts.fb_accounts.page_id':self.fb_account_id, '_id':ObjectId(brand_id)}, {'$pull':{'associated_accounts.fb_accounts.$.competitor':id}})
        #    print('Im in exception')
        #    callback('NotDone')
class InstagramEngagement(object):

    def __init__(self,data):
        self.db=data['db']
        self.user_id=data['user_id']
        self.ins_user_id=data['page_id']
        res=self.db.sociabyte.user.find({'_id':self.user_id},{'brands':1})
        from bson.objectid import ObjectId
        ids=[ObjectId(a) for a in res[0]['brands']]
        access_token_result = self.db.sociabyte.brands.find({"associated_accounts.ins_accounts.page_id":self.ins_user_id,'_id':{'$in':ids}}, {"associated_accounts.ins_accounts.token":1,"associated_accounts.ins_accounts.page_id":1, "_id": 0, "associated_accounts.ins_accounts.name":1,"associated_accounts.ins_accounts.competitor":1})
        self.access_token = ''
        self.complist = []
        for acc in access_token_result:
            counter = 0
            for acc_unit in acc['associated_accounts']['ins_accounts']:
                if acc['associated_accounts']['ins_accounts'][counter]['page_id'] == self.ins_user_id:
                    self.access_token = acc['associated_accounts']['ins_accounts'][counter]['token']
                    page_name= acc['associated_accounts']['ins_accounts'][counter]['name']
                    self.complist = acc['associated_accounts']['ins_accounts'][counter]['competitor']

                else:
                    pass
                counter = counter+1
        print(self.access_token)
        pass


    def searchPage(self,req,callback):
        req.set_header("Access-Control-Allow-Origin", "*")
        req.set_header("Access-Control-Allow-Credentials", "true")
        req.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        req.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")
        username=req.get_argument('username')
        if username[0:8]=="https://":
            url_list = username.split( "/")
            screenName = url_list[3].split("?")[0]
        else:
            screenName=username
        try:
            self.fb_handle = urllib.urlopen('https://api.instagram.com/v1/users/search?q='+screenName+'&access_token='+self.access_token)
            res = json.loads(self.fb_handle.read())
            matched_page_id = res['data'][0]['id']
            print(matched_page_id)
            res_id = urllib.urlopen('https://api.instagram.com/v1/users/'+matched_page_id+'?access_token='+self.access_token)
            res_json = json.loads(res_id.read())
            #res_json['link'] = 'http://instagram.com/'+res_json['username']
            #print(res_json['link'])
            print res_json
            callback(json.dumps(res_json))
        except Exception as e:
           callback('NotFound')

    def addComp(self,req,callback):
        import inst_benchmark.inst_benchmark as inst_benchmark
        req.set_header("Access-Control-Allow-Origin", "*")
        req.set_header("Access-Control-Allow-Credentials", "true")
        req.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        req.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")
        id=req.get_argument('compId')
        print (id)
        brand_id = req.get_argument('brand_id')
        print (brand_id)
        try:
            if id not in self.complist:
                self.db.sociabyte.brands.update({'associated_accounts.ins_accounts.page_id':self.ins_user_id, '_id':ObjectId(brand_id)}, {'$addToSet':{'associated_accounts.ins_accounts.$.competitor':id}})
                inst_benchmark.userFetch(self.db.sociabyte, id, self.access_token, brand_id, self.ins_user_id)
                inst_benchmark.recentMediaFetch(self.db.sociabyte,id, self.access_token, brand_id, self.ins_user_id)

                media_id_list = []
                media_id_list_response = self.db.sociabyte.inst_media_comp.find({"user_id":id}, {"_id":0, 'media_id':1})
                print 'media_id_list_response====>',media_id_list_response
                for media_id in media_id_list_response:
                    media_id_list.append(media_id)
                print 'media_id_list=====>',media_id_list
                inst_benchmark.commentFetch(self.db.sociabyte, id, self.access_token,media_id_list, brand_id, self.ins_user_id)
                inst_benchmark.followedByFetch(self.db.sociabyte,id, self.access_token, brand_id, self.ins_user_id)
                inst_benchmark.dailyReachCalculator(self.db.sociabyte,id, brand_id, self.ins_user_id)
                inst_benchmark.genderClassifier(self.db.sociabyte,id)
                sendDict = {
                    'acc_id':id,
                    'code':'inst_comp'
                }
                sendDict = json.dumps(sendDict)
                port = '5573'
                context = zmq.Context()
                print ("Connecting to server...")
                socket = context.socket(zmq.PUB)
                socket.bind("tcp://*:%s" % port)
                print ("Sending request ")
                a=0
                while a<2:
                    socket.send_multipart(['',sendDict])
                    print "send successfull"
                    a+=1
                    time.sleep(1)
                #  Get the reply.
                socket.close()
                callback('Done')
            else:
                callback("existing")
        except Exception as e:
            self.db.sociabyte.brands.update({'associated_accounts.ins_accounts.page_id':self.ins_user_id, '_id':ObjectId(brand_id)}, {'$pull':{'associated_accounts.ins_accounts.$.competitor':id}})
            print('Im in exception')
            callback('NotDone')
class VkEngagement(object):
    def __init__(self,data):
        self.db=data['db']
        self.user_id=data['user_id']
        self.vk_user_id=data['page_id']
        res=self.db.sociabyte.user.find({'_id':self.user_id},{'brands':1})
        from bson.objectid import ObjectId
        ids=[ObjectId(a) for a in res[0]['brands']]
        access_token_result = self.db.sociabyte.brands.find({"associated_accounts.vk_accounts.page_id":self.vk_user_id,'_id':{'$in':ids}}, {"associated_accounts.vk_accounts.token":1,"associated_accounts.vk_accounts.page_id":1, "_id": 0, "associated_accounts.vk_accounts.name":1,"associated_accounts.vk_accounts.competitor":1})
        self.access_token = ''
        self.complist = []
        counter = int()
        for acc in access_token_result:
            counter = 0
            for acc_unit in acc['associated_accounts']['vk_accounts']:
                if acc['associated_accounts']['vk_accounts'][counter]['page_id'] == self.vk_user_id:
                    self.access_token = acc['associated_accounts']['vk_accounts'][counter]['token']
                    page_name= acc['associated_accounts']['vk_accounts'][counter]['name']
                    self.complist = acc['associated_accounts']['vk_accounts'][counter]['competitor']

                else:
                    pass
                counter = counter+1
        print(self.access_token)
        pass

    def searchPage(self,req,callback):
        req.set_header("Access-Control-Allow-Origin", "*")
        req.set_header("Access-Control-Allow-Credentials", "true")
        req.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        req.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")
        username=req.get_argument('username')
        print(username)
        try:
            profile_field = "city,photo,followers_count,screen_name"
            self.vk_handle = urllib.urlopen('https://api.vk.com/method/users.search?q='+username+'&access_token='+self.access_token+'&fields='+profile_field)
            res = json.loads(self.vk_handle.read())
            import pprint
            pprint.pprint(res)
            city_id = res['response'][1]['city']
            self.city_name = urllib.urlopen('https://api.vk.com/method/database.getCitiesById?city_ids='+str(city_id)+'&access_token='+self.access_token)
            res_city_name = json.loads(self.city_name.read())
            res['response'][1]['city'] = res_city_name['response'][0]['name']

            callback(json.dumps(res))
        except Exception as e:
            callback('NotFound')
            
    def addComp(self,req,callback):
        import vk_benchmark.vk_benchmark as vk_benchmark
        req.set_header("Access-Control-Allow-Origin", "*")
        req.set_header("Access-Control-Allow-Credentials", "true")
        req.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        req.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")
        id=req.get_argument('compId')
        print (id)
        brand_id = req.get_argument('brand_id')
        print (brand_id)
        try:
            if id not in self.complist:
                self.db.sociabyte.brands.update({'associated_accounts.vk_accounts.page_id':self.vk_user_id, '_id':ObjectId(brand_id)}, {'$addToSet':{'associated_accounts.vk_accounts.$.competitor':id}})
                vk_benchmark.userFetch(self.db.sociabyte, id, self.access_token, brand_id, self.vk_user_id)
                foll=vk_benchmark.userFollowers(self.db.sociabyte,id, self.access_token, brand_id, self.vk_user_id)
                frnd=vk_benchmark.userFriends(self.db.sociabyte,id, self.access_token, brand_id,self.vk_user_id)
                wall=vk_benchmark.getWallPosts(self.db.sociabyte,id, self.access_token, brand_id, self.vk_user_id)
                if wall=='error' or foll=='error' or frnd=='error':
                    self.db.sociabyte.brands.update({'associated_accounts.vk_accounts.page_id':self.vk_user_id, '_id':ObjectId(brand_id)}, {'$pull':{'associated_accounts.vk_accounts.$.competitor':id}})
                    obj = {'status' : 'NotDone'}
                    callback(json.dumps(obj))
                else:
                    vk_benchmark.genderClassifier(self.db.sociabyte,id)
                    vk_benchmark.dailyReachCalculator(self.db.sociabyte,id,brand_id,self.vk_user_id)
                    vk_benchmark.agegroupCalculator(self.db.sociabyte,id)
                    friends_count = self.db.sociabyte.vk_user_comp.find({'uid' : int(id)},{'counters.friends' : 1,'followers_count' : 1,'photo_50' : 1,'first_name' : 1,'last_name' : 1,'uid' : 1,'_id' : 0})
                    f_count = ''
                    first_name = ''
                    last_name = ''
                    followers_count = ''
                    photo_50 = ''
                    screen_name = ''
                    for count in friends_count:
                        f_count = str(count['counters']['friends'])
                        followers_count = str(count['followers_count'])
                        photo_50 = str(count['photo_50'])
                        first_name = str(count['first_name'])
                        last_name = str(count['last_name'])
                        screen_name = str('id'+str(count['uid']))
                    full_name = str(first_name)+' '+str(last_name)
                    obj = {'friends_count' : f_count,
                           'followers_count' : followers_count,
                           'photo' : photo_50,
                           'full_name' : full_name,
                           'screen_name' : screen_name,
                           'status' : 'Done'}
                    print('obj=>>>',obj)
                    callback(json.dumps(obj))
            else:
                obj = {'status' : 'existing'}
                callback(json.dumps(obj))
        except Exception as e:
            self.db.sociabyte.brands.update({'associated_accounts.vk_accounts.page_id':self.vk_user_id, '_id':ObjectId(brand_id)}, {'$pull':{'associated_accounts.vk_accounts.$.competitor':id}})
            obj = {'status' : 'NotDone'}
            callback(json.dumps(obj))
class LinkedinEngagement(object):

    def __init__(self,data):
        self.db=data['db']
        self.user_id=data['user_id']
        self.linkdin_user_id=data['page_id']
        res=self.db.sociabyte.user.find({'_id':self.user_id},{'brands':1})
        from bson.objectid import ObjectId
        ids=[ObjectId(a) for a in res[0]['brands']]
        access_token_result = self.db.sociabyte.brands.find({"associated_accounts.in_accounts.page_id":self.linkdin_user_id,'_id':{'$in':ids}}, {"associated_accounts.in_accounts.token":1,"associated_accounts.in_accounts.page_id":1, "_id": 0, "associated_accounts.in_accounts.name":1,"associated_accounts.in_accounts.competitor":1})
        self.access_token = ''
        self.complist = []
        for acc in access_token_result:
            counter = 0
            for acc_unit in acc['associated_accounts']['in_accounts']:
                if acc['associated_accounts']['in_accounts'][counter]['page_id'] == self.linkdin_user_id:
                    self.access_token = acc['associated_accounts']['in_accounts'][counter]['token']
                    page_name= acc['associated_accounts']['in_accounts'][counter]['name']
                    self.complist = acc['associated_accounts']['in_accounts'][counter]['competitor']
                else:
                    pass
                counter = counter+1
        print(self.access_token)
        pass


    def searchPage(self,req,callback):
        req.set_header("Access-Control-Allow-Origin", "*")
        req.set_header("Access-Control-Allow-Credentials", "true")
        req.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        req.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")
        username=req.get_argument('username')
        print(username)
        try:
            field_selectors = "company-type,logo-url,employee-count-range,num-followers"
            self.linkdin_handle = urllib.urlopen('https://api.linkedin.com/v1/company-search?keywords='+username+'&format=json&oauth2_access_token='+self.access_token)
            res = json.loads(self.linkdin_handle.read())
            print('res============================>',res)
            companyDetails = res['companies']['values']
            obj = {
                'companyDetails' : companyDetails,
                'status' : 'Done'
            }
            print('companyDetails========================>',companyDetails)
            print('obj=====================>',obj)
            callback(json.dumps(obj))
        except Exception as e:
            obj = {'status' : 'NotFound'}
            callback(json.dumps(obj))

    def addComp(self,req,callback):
        import li_benchmark.li_benchmark as li_benchmark
        req.set_header("Access-Control-Allow-Origin", "*")
        req.set_header("Access-Control-Allow-Credentials", "true")
        req.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        req.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")
        id = req.get_argument('compId')
        brand_id = req.get_argument('brand_id')
        print(id)
        print('comList===============================>',self.complist)
        try:
            if id not in self.complist:
                self.db.sociabyte.brands.update({'associated_accounts.in_accounts.page_id':self.linkdin_user_id, '_id':ObjectId(brand_id)}, {'$addToSet':{'associated_accounts.in_accounts.$.competitor':id}})
                shares_list = []
                return_value = li_benchmark.basicInfoFetch(id, self.db.sociabyte, self.access_token, brand_id, self.linkdin_user_id)
                #li_benchmark.compShareInfo(id, self.db.sociabyte, self.access_token, brand_id, self.linkdin_user_id)
                #share_response = self.db.sociabyte.li_shares_comp.find({'updateContent.company.id':int(id)}, {'updateKey':1, '_id':0})
                #for shares in share_response:
                    #shares_list.append(shares['updateKey'])
                #print('shares_list=============>',shares_list)
                #li_benchmark.commentFetch(id, shares_list, self.db.sociabyte, self.access_token, brand_id, self.linkdin_user_id)
                obj_value = {}
                if return_value == 1:
                    ret = self.db.sociabyte.li_basic_info_comp.find({'id' : int(id)},{'logoUrl':1,'name':1,'numFollowers':1,'employeeCountRange.name':1,'id':1, '_id':0})
                    for r in ret:
                        try:
                            e_count = r['employeeCountRange']['name']
                        except:
                            e_count = 'NOT AVAILABLE'
                        try:
                            logo = r['logoUrl']
                        except:
                            logo = '/images/no_image.png'
                        obj_value = {
                            'logoUrl' : logo ,
                            'name' : r['name'],
                            'numFollowers' : r['numFollowers'],
                            'employeeCount' : e_count,
                            'competitor_id' : int(r['id'])
                                    }
                print('obj_value===================>',obj_value)
                dt = {
                    'value':obj_value,
                    'status':'Done'
                }
                callback(json.dumps(dt))
            else:
               obj = {'status' : 'existing'}
               callback(json.dumps(obj))
        except Exception as e:
            self.db.sociabyte.brands.update({'associated_accounts.in_accounts.page_id':self.linkdin_user_id, '_id':ObjectId(brand_id)}, {'$pull':{'associated_accounts.in_accounts.$.competitor':id}})
            print('Im in exception')
            obj = {'status' : 'NotDone'}
            callback(json.dumps(obj))