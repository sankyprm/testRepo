from pymongo import MongoClient,DESCENDING
from tzlocal import get_localzone
from datetime import datetime, timedelta
from pytz import timezone
import time, calendar
from bson import ObjectId
import facebook
from twython import Twython
import json
import urllib
import zmq
import global_settings

db_conn = global_settings.db_conn
db_connection = db_conn.sociabyte

def findAppropriateUser(db_connection_formal):
    list_final_dict = []
    user_response = db_connection_formal.user.find()
    for user in user_response:
        ut = user['address']['timezone']
        ut = timezone(ut)
        sys_timezone = get_localzone()
        local_time = sys_timezone.localize(datetime.now())
        local_timestamp = calendar.timegm(local_time.utctimetuple())
        user_time = ut.localize(datetime.now())
        user_timestamp = calendar.timegm(user_time.utctimetuple())
        user_runtime = user_time.replace(hour=6, minute=0, second=0, microsecond=0)
        user_runtimestamp = calendar.timegm(user_runtime.utctimetuple())
        min_range =user_runtimestamp-1800
        max_range = user_runtimestamp+1800
        print sys_timezone, '=====', ut
        print min_range, '=====', max_range
        if user_timestamp>=min_range or user_timestamp<max_range:
            print 'user_timestamp===>>>>',user_timestamp
            print 'user_runtimestamp===>>', user_runtimestamp
            list_final_dict=map(findAllAccount, user['brands'])
        return list_final_dict
def findAllAccount(brand_id_formal):
    fb_account_list = []
    tw_account_list = []
    inst_account_list = []
    li_account_list = []
    vk_account_list = []
    youtube_account_list = []
    brand_response = db_connection.brands.find({'_id':ObjectId(brand_id_formal)}, {'associated_accounts':1, '_id':0})
    for brands in brand_response:
        for accounts in brands['associated_accounts']['fb_accounts']:
            fb_small_list = [accounts['competitor'], accounts['page_id'], accounts['token'],brand_id_formal]
            fb_account_list.append(fb_small_list)
        for accounts in brands['associated_accounts']['tw_accounts']:
            tw_small_list = [accounts['competitor'], accounts['page_id'], accounts['token'], accounts['tokenSecret'],brand_id_formal]
            tw_account_list.append(tw_small_list)
        for accounts in brands['associated_accounts']['ins_accounts']:
            inst_small_list = [accounts['competitor'], accounts['page_id'], accounts['token'],brand_id_formal]
            inst_account_list.append(inst_small_list)
        for accounts in brands['associated_accounts']['in_accounts']:
            li_small_list = [accounts['competitor'], accounts['page_id'],  accounts['token'],brand_id_formal]
            li_account_list.append(li_small_list)
        for accounts in brands['associated_accounts']['vk_accounts']:
            vk_small_list = [accounts['competitor'], accounts['page_id'], accounts['token'],brand_id_formal]
            vk_account_list.append(vk_small_list)
        for accounts in brands['associated_accounts']['utube_accounts']:
            youtube_small_list = [accounts['competitor'], accounts['page_id'], accounts['accessToken'], accounts['refreshToken'],brand_id_formal]
            youtube_account_list.append(youtube_small_list)
    final_dict={
        'fb':fb_account_list,
        'tw':tw_account_list,
        'inst':inst_account_list,
        'li':li_account_list,
        'vk':vk_account_list,
        'utube':youtube_account_list
    }
    return final_dict
def runFbDaily(formal_arguments):
    import fb_benchmark.fb_benchmark as fb_comp_daily_services
    comp_account_id_list, fb_account_id, token, brand_id = formal_arguments
    fb_handle = facebook.GraphAPI(token)
    for account_id in comp_account_id_list:
        fb_comp_daily_services.userAccountsInfo(db_connection, account_id, fb_handle, brand_id,fb_account_id)
        fb_comp_daily_services.pagePosts(account_id, db_connection, fb_handle, brand_id,fb_account_id)
        fb_comp_daily_services.postUpdate(account_id, db_connection, fb_handle, brand_id,fb_account_id)
        fb_comp_daily_services.commentFetch(account_id, db_connection, fb_handle, brand_id,fb_account_id)
        fb_comp_daily_services.like_fetch(account_id, db_connection, fb_handle, brand_id,fb_account_id)
        fb_comp_daily_services.insight_fetch(account_id, db_connection, fb_handle, brand_id,fb_account_id)
        sendDict = {
                'acc_id':account_id,
                'code':'fb'
            }
        sendDict = json.dumps(sendDict)
        port = '5559'
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
def runTwDaily(formal_arguments):
    import tw_benchmark.tw_benchmark as tw_comp_daily_services
    comp_account_id_list,tw_account_id, token, token_secret, brand_id = formal_arguments
    tw_handle = Twython('Qwu1NiiV2uzGp3Hj0UqD1CKo2', '6CiVt3lDPnD4qOaccPOxpcPeTNkZ8h8ljelavK79xKsqpgEEYF', token, token_secret)
    for account_id in comp_account_id_list:
        tw_comp_daily_services.userInfo(db_connection,account_id, tw_handle, brand_id, tw_account_id)
        tw_comp_daily_services.userTimeline(db_connection, account_id, tw_handle, brand_id, tw_account_id )
        tw_comp_daily_services.timelineUpdate(db_connection, account_id, tw_handle, brand_id, tw_account_id )
        tw_comp_daily_services.followersFetch(db_connection, account_id, tw_handle, brand_id, tw_account_id)
        tw_comp_daily_services.followerDetails(db_connection, account_id, tw_handle, brand_id, tw_account_id )
        tw_comp_daily_services.genderClassifier(db_connection, account_id, brand_id, tw_account_id)
        tw_comp_daily_services.dailyReachCalculator(db_connection, account_id)
        sendDict = {
                'acc_id':account_id,
                'code':'tw'
            }
        sendDict = json.dumps(sendDict)
        port = '5560'
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
def runInstDaily(formal_arguments):
    import inst_benchmark.inst_benchmark as inst_comp_daily_services
    comp_account_id_list,inst_account_id, token, brand_id = formal_arguments
    for account_id in comp_account_id_list:
        inst_comp_daily_services.userFetch(db_connection,account_id, token, brand_id, inst_account_id)
        inst_comp_daily_services.recentMediaFetch(db_connection,account_id, token, brand_id, inst_account_id)
        inst_comp_daily_services.recentMediaUpdate(db_connection, account_id,  token, brand_id, inst_account_id)
        inst_comp_daily_services.commentFetch( db_connection, account_id, token, brand_id, inst_account_id)
        inst_comp_daily_services.likeFetch(db_connection,account_id, token, brand_id, inst_account_id)
        inst_comp_daily_services.dailyReachCalculator(db_connection,account_id, brand_id, inst_account_id )
        inst_comp_daily_services.genderClassifier(db_connection,account_id, brand_id, inst_account_id)
        sendDict = {
                'acc_id':account_id,
                'code':'inst'
            }
        sendDict = json.dumps(sendDict)
        port = '5561'
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
def runVkDaily(formal_arguments):
    import vk_benchmark.vk_benchmark as vk_comp_daily_services
    comp_account_id_list,vk_account_id, token, brand_id= formal_arguments
    for account_id in comp_account_id_list:
        vk_comp_daily_services.userFetch(db_connection, account_id, token, brand_id, vk_account_id)
        vk_comp_daily_services.getNewWallPosts(db_connection,account_id, token, brand_id, vk_account_id)
        vk_comp_daily_services.wallPostUpdate(db_connection, account_id, token, brand_id, vk_account_id)
        vk_comp_daily_services.dailyReachCalculator(db_connection, account_id, )
def runLiDaily(formal_arguments):
    import li_benchmark.li_benchmark as li_comp_daily_services
    comp_account_id_list,li_account_id, token, brand_id = formal_arguments
    for account_id in comp_account_id_list:
        li_comp_daily_services.basicInfoFetch(db_connection,account_id,token, brand_id, li_account_id)
def userMap(userlist):
    print 'userlist=====>>>>', userlist
    map(brandsMap, userlist)
def brandsMap(brandslist):
    print 'brandsMap=====>>>', brandslist
    media_access(brandslist)
def media_access(medialist):
    for keys in medialist.keys():
        if keys=='fb':
            map(runFbDaily, medialist[keys])
        if keys=='tw':
            map(runTwDaily, medialist[keys])
        if keys=='inst':
            map(runInstDaily, medialist[keys])
        if keys=='vk':
            map(runVkDaily, medialist[keys])
        if keys=='li':
            map(runLiDaily, medialist[keys])

def main():
    list_final = findAppropriateUser(db_connection)
    print 'len==>', list_final
    if len(list_final)!=0:
        userMap(list_final)
main()