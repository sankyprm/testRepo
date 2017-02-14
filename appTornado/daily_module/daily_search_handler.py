from pymongo import MongoClient,DESCENDING
from tzlocal import get_localzone
from datetime import datetime, timedelta
from pytz import timezone
import time, calendar
from bson import ObjectId
import fb_daily.fb_daily as fb_daily_services
import tw_daily.tw_daily as tw_daily_services
import inst_daily.inst_daily as inst_daily_services
import vk_daily.vk_services_daily as vk_daily_services
import li_daily.li_daily_services as li_daily_services
import utube_daily.utube_daily as utube_daily_services
import facebook
from twython import Twython
import json
import urllib
import zmq
from multiprocessing import Pool
import global_settings
db_conn = global_settings.db_conn
db_connection = db_conn.sociabyte

def findAppropriateUser(db_connection_formal):
    list_final_dict = []
    user_response = db_connection_formal.user.find()
    for user in user_response:
        print "user===>>>", user
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
        print "brands=====>>>>", user['brands']
        if user_timestamp>=min_range and user_timestamp<max_range:
            print 'user_timestamp===>>>>',user_timestamp
            print 'user_runtimestamp===>>', user_runtimestamp
            list_final_dict.append(map(findAllAccount, user['brands']))
    print "list dict final====>>>>", list_final_dict
    return list_final_dict
def findAllAccount(brand_id_formal):
    fb_account_list = []
    tw_account_list = []
    inst_account_list = []
    li_account_list = []
    vk_account_list = []
    youtube_account_list = []
    project_list = []
    brand_response = db_connection.brands.find({'_id':ObjectId(brand_id_formal)}, {'associated_accounts':1, 'projects':1, '_id':0})
    for brands in brand_response:
        for accounts in brands['associated_accounts']['fb_accounts']:
            fb_small_list = [accounts['page_id'], accounts['token']]
            fb_account_list.append(fb_small_list)
        for accounts in brands['associated_accounts']['tw_accounts']:
            tw_small_list = [accounts['page_id'], accounts['token'], accounts['tokenSecret']]
            tw_account_list.append(tw_small_list)
        for accounts in brands['associated_accounts']['ins_accounts']:
            inst_small_list = [accounts['page_id'], accounts['token']]
            inst_account_list.append(inst_small_list)
        for accounts in brands['associated_accounts']['in_accounts']:
            li_small_list = [accounts['page_id'], accounts['token']]
            li_account_list.append(li_small_list)
        for accounts in brands['associated_accounts']['vk_accounts']:
            vk_small_list = [accounts['page_id'], accounts['token']]
            vk_account_list.append(vk_small_list)
        for accounts in brands['associated_accounts']['utube_accounts']:
            youtube_small_list = [accounts['page_id'], accounts['accessToken'], accounts['refreshToken']]
            youtube_account_list.append(youtube_small_list)
        project_list = brands['projects']
    final_dict={
        'fb':fb_account_list,
        'tw':tw_account_list,
        'inst':inst_account_list,
        'li':li_account_list,
        'vk':vk_account_list,
        'utube':youtube_account_list,
        'projects':project_list
    }
    return final_dict
def runFbDaily(formal_arguments):
    account_id, token = formal_arguments
    fb_handle = facebook.GraphAPI(token)
    fb_daily_services.userAccountsInfo(db_connection, account_id, fb_handle)
    fb_daily_services.pagePosts(account_id, db_connection, fb_handle, token)
    fb_daily_services.postUpdate(account_id, db_connection, fb_handle, token)
    fb_daily_services.commentFetch(account_id, db_connection, fb_handle)
    fb_daily_services.like_fetch(account_id, db_connection, fb_handle)
    fb_daily_services.insight_fetch(account_id, db_connection, fb_handle)
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
    acount_id, token, token_secret = formal_arguments
    tw_handle = Twython('Qwu1NiiV2uzGp3Hj0UqD1CKo2', '6CiVt3lDPnD4qOaccPOxpcPeTNkZ8h8ljelavK79xKsqpgEEYF', token, token_secret)
    tw_daily_services.userInfo(acount_id, tw_handle, db_connection)
    tw_daily_services.userMentionTimeline(acount_id, tw_handle, db_connection)
    tw_daily_services.userTimeline(acount_id, tw_handle, db_connection)
    tw_daily_services.timelineUpdate(acount_id, tw_handle, db_connection)
    tw_daily_services.retweetsFetch(acount_id, tw_handle, db_connection)
    tw_daily_services.followersFetch(acount_id, tw_handle, db_connection)
    tw_daily_services.followerDetails(acount_id, tw_handle, db_connection)
    tw_daily_services.genderClassifier(acount_id, db_connection)
    tw_daily_services.dailyReachCalculator(acount_id, db_connection)
    sendDict = {
            'acc_id':acount_id,
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
    acount_id, token= formal_arguments
    inst_daily_services.userFetch(acount_id, db_connection, token, urllib, json)
    inst_daily_services.recentMediaFetch(acount_id, db_connection, token, urllib, json)
    inst_daily_services.recentMediaUpdate(acount_id, db_connection, token, urllib, json)
    inst_daily_services.commentFetch(acount_id, db_connection, token, urllib, json)
    inst_daily_services.likeFetch(acount_id, db_connection, token, urllib, json)
    inst_daily_services.dailyReachCalculator(acount_id, db_connection)
    inst_daily_services.genderClassifier(acount_id, db_connection)
    sendDict = {
            'acc_id':acount_id,
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
    acount_id, token= formal_arguments
    vk_daily_services.userFetch(acount_id, token, db_connection, json, urllib)
    vk_daily_services.getNewWallPosts(acount_id, token, db_connection, json, urllib)
    vk_daily_services.wallPostUpdate(acount_id, token, db_connection, json, urllib)
    vk_daily_services.wallPostUpdate(acount_id, token, db_connection, json, urllib)
    vk_daily_services.getWallComments(acount_id, token, db_connection, json, urllib)
    vk_daily_services.getWallLikes(acount_id, token, db_connection, json, urllib)
    vk_daily_services.dailyReachCalculator(acount_id, db_connection)
def runLiDaily(formal_arguments):
    account_id, token = formal_arguments
    li_daily_services.basicInfoFetch(account_id, db_connection, token, urllib, json)
    share_list = li_daily_services.compShareInfo(account_id, db_connection, token, urllib, json)
    li_daily_services.updateShares(account_id, db_connection, token, urllib, json)
    li_daily_services.historicalFollowerStatistics(account_id, db_connection, token, urllib, json)
    li_daily_services.historicalStatusUpdateStat(account_id, db_connection, token, urllib, json)
    li_daily_services.commentFetch(account_id, share_list, db_connection, token, urllib, json)
    li_daily_services.companyStatistics(account_id, db_connection, token, urllib, json)
    sendDict = {
            'acc_id':account_id,
            'code':'li'
        }
    sendDict = json.dumps(sendDict)
    port = '5563'
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
def runUtubeDaily(formal_arguments):
    acount_id, token, refresh_token = formal_arguments
    if utube_daily_services.verifyToken(token, urllib, json)==False:
        utube_daily_services.updateToken(acount_id, token, refresh_token, urllib, json, db_connection)
        access_token_response = db_connection.brands.find({'associated_accounts.utube_accounts.page_id':str(acount_id)}, {'associated_accounts.utube_accounts.accessToken':1, 'associated_accounts.utube_accounts.page_id':1,'_id':0})
        counter = 0
        for acc in access_token_response:
            print acc, "===="
            for acc_unit in acc['associated_accounts']['utube_accounts']:
                if acc['associated_accounts']['utube_accounts'][counter]['page_id']==acount_id:
                    token = acc['associated_accounts']['utube_accounts'][counter]['accessToken']
                else:
                    pass
                counter = counter+1
    else:
        pass
    utube_daily_services.basicDataFetch(acount_id, token, db_connection, urllib, json)
    utube_daily_services.channelGrowth(acount_id, token, db_connection, urllib, json)
    utube_daily_services.insightTrafficSource(acount_id, token, db_connection, urllib, json)
    utube_daily_services.activePlatform(acount_id, token, db_connection, urllib, json)
    utube_daily_services.insightFromMedia(acount_id, token, db_connection, urllib, json)
    utube_daily_services.insightPlayback(acount_id, token, db_connection, urllib, json)
    video_id_list = utube_daily_services.getAllVideos(acount_id, token, db_connection, urllib, json)
    utube_daily_services.getAllComments(acount_id, token, db_connection, urllib, json, video_id_list)
    utube_daily_services.topVideos(acount_id, token, db_connection, urllib, json)
    sendDict = {
            'acc_id':acount_id,
            'code':'utube'
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
def runProjectDaily(formal_arguments):
    import project_daily.project_daily as project_services_parm
    import brand_overall.brand_overall_daily as brand_overall
    project_id = formal_arguments
    project_details_response = db_conn.sociabyte.projects.find({'_id':ObjectId(project_id)}, {'_id':0})
    for project in project_details_response:
        print project
        category = project['category']
        languages = project['languages']
        sources = project['sources']
        countries = project['country']
        account_type =project['project_plan']
        brand_id = project['owner_brands']
    print type(category), '===', type(languages), '====', type(sources), '====', type(countries), '====', type(brand_id)
    list_for_google = []
    arg_list_final1 = []
    arg_list_final2 = []
    for pair in category:
        cat = pair['name']
        query = pair['query']
        temp_list = [cat, query, project_id]
        list_for_google.append(temp_list)
        arg_list1 = [query, project_id, account_type, cat, languages]
        arg_list2 = [query, project_id, account_type, cat, countries, sources, languages]
        arg_list_final1.append(arg_list1)
        arg_list_final2.append(arg_list2)
    for arg_val in list_for_google:
        try:
            project_services_parm.FeedUrlFetch(arg_val)
        except:
            pass
    print "arglistfinal1====>>>>>",arg_list_final1
    try:
        process_pool = Pool(processes=4)
        process_pool.map(project_services_parm.dataFeed, arg_list_final1)  # other sources should be fetched like this
        process_pool.close()
        process_pool.join()
    except:
        pass
    # code to fetch the rss feed link
    #for temp_val in arg_list_final2:
    #    project_services_temp.dataFeedRss(temp_val)
    try:
        process_pool = Pool(processes=4)
        process_pool.map(project_services_parm.dataFeedRss, arg_list_final2)  # other sources should be fetched like this
        process_pool.close()
        process_pool.join()
    except:
        pass
    access_token_response = db_conn.sociabyte.brands.find({'_id':ObjectId(brand_id)}, {"associated_accounts.tw_accounts.token":1, "associated_accounts.tw_accounts.tokenSecret":1, "_id":0})
    counter = 0
    token = ''
    for acc in access_token_response:
        for acc_unit in acc['associated_accounts']['tw_accounts']:
            token = acc['associated_accounts']['tw_accounts'][counter]['token']
            token_secret = acc['associated_accounts']['tw_accounts'][counter]['tokenSecret']
            counter = counter+1
    if token!='':
        tw_handle = Twython('Qwu1NiiV2uzGp3Hj0UqD1CKo2', '6CiVt3lDPnD4qOaccPOxpcPeTNkZ8h8ljelavK79xKsqpgEEYF', token, token_secret)
    else:
        tw_handle = Twython('inBAC5z1EiCt8k6z32dqw', 's8IK3HT8cWReb2YUSV8kTQvf3qANXwKZhCZORw5XAVg', '177512438-v68R6ZeW9dTZB6PzZgSZSvjnIfAI4rscDioxIcfs', 'oVw6W90eXspQrlAbib6QivHKJ2nJ6nf8dPOK7DxVic')
    for tw_arg in list_for_google:
        try:
            project_services_parm.fetchTwitterFeeds(tw_arg, tw_handle) #place the keys properly
        except:
            pass
        try:
            project_services_parm.fetchVkFeeds(tw_arg) #place the keys properly
        except:
            pass
    sendDict = {
        'acc_id':str(project_id),
        'code':'projectParm'
    }
    sendDict = json.dumps(sendDict)
    port = '5557'
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
    checker = 1
    while checker:
        check_status = db_conn.sociabyte.projects.find({'_id':ObjectId(project_id)}, {'final_fetch':1, '_id':0})
        for status in check_status:
            final_status = status['final_fetch']
        if final_status == 'complete':
            checker=0
        if final_status == 'incomplete':
            time.sleep(1)
            continue

    print 'okkk'
    brand_overall.executorOverall(db_conn, brand_id, 'project')
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
        #if keys=='utube':
        #    map(runUtubeDaily, medialist[keys])
        if keys=='projects':
            map(runProjectDaily, medialist[keys])
def main():
    list_final = findAppropriateUser(db_connection)
    print 'len==>', list_final
    if len(list_final)!=0:
        map(userMap, list_final)
main()