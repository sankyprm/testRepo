__author__ = 'sibiaanalytics'
from nltk.corpus import names
import random
from bson.objectid import ObjectId
import nltk
import time
import datetime
from datetime import date
def cityFinder(city_id_formal, db_connection_formal, json_formal, urllib_formal):
    if city_id_formal!=0:
        city_info= urllib_formal.urlopen('https://api.vk.com/method/database.getCitiesById?city_ids='+str(city_id_formal))
        city_info_response = json_formal.loads(city_info.read())
        city_json = {
            'id':0,
            'name':''
        }
        city_json['id'] = city_id_formal
        city_json['name'] = city_info_response['response'][0]['name']
        db_connection_formal.vk_city.insert(city_json)
        city = city_info_response['response'][0]['name']
    else:
        city_info_response = json_formal.loads(city_info.read())
        city_json = {
            'id':0,
            'name':''
        }
        city_json['id'] = city_id_formal
        city_json['name'] = 'Undefined'
        db_connection_formal.vk_city.insert(city_json)
        city = 'Undefined'
    return city
def countryFinder(country_id_formal, db_connection_formal, json_formal, urllib_formal):
    if country_id_formal!=0:
        country_info = urllib_formal.urlopen('https://api.vk.com/method/database.getCountriesById?country_ids='+str(country_id_formal))
        country_info_response = json_formal.loads(country_info.read())
        country_json = {
            'id':0,
            'name':''
        }
        country_json['id'] = country_id_formal
        country_json['name'] = country_info_response['response'][0]['name']
        db_connection_formal.vk_country.insert(country_json)
        country = country_info_response['response'][0]['name']
    else:
        country_info = urllib_formal.urlopen('https://api.vk.com/method/database.getCountriesById?country_ids='+str(country_id_formal))
        country_info_response = json_formal.loads(country_info.read())
        country_json = {
            'id':0,
            'name':''
        }
        country_json['id'] = country_id_formal
        country_json['name'] = 'Undefined'
        db_connection_formal.vk_country.insert(country_json)
        country = 'Undefined'
    return country
def userFetch(user_id_formal, access_token_formal, db_connection_formal, json_formal, urllib_formal):
    profile_fields = "sex, bdate, city, country, photo_50, contacts, followers_count, status, counters"
    user_info = urllib_formal.urlopen('https://api.vk.com/method/users.get?user_id='+user_id_formal+'&access_token='+access_token_formal+'&fields='+profile_fields)
    user_info_response = json_formal.loads(user_info.read())
    city_name=''
    country_name = ''
    city_response = db_connection_formal.vk_city.find({'id':user_info_response['response'][0]['city']})
    for city in city_response:
        city_name = city['name']
    if city_name == '':
        city_name=cityFinder(user_info_response['response'][0]['city'], db_connection_formal, json_formal, urllib_formal)
    country_response = db_connection_formal.vk_country.find({'id':user_info_response['response'][0]['country']})
    for country in country_response:
        country_name = country['name']
    if country_name == '':
        country_name=countryFinder(user_info_response['response'][0]['country'], db_connection_formal, json_formal, urllib_formal)
    user_info_response['response'][0]['city']=city_name
    user_info_response['response'][0]['country']=country_name
    if 'bdate' in user_info_response['response'][0]:
        bdatelist = user_info_response['response'][0]['bdate'].split('.')
        dateStr = str(bdatelist[2])+"-"+str(bdatelist[1])+"-"+str(bdatelist[0])+"T10:30:01+0000"
        user_info_response['response'][0]['bdate'] = datetime.datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S+0000")
    else:
        user_info_response['response'][0]['bdate'] = 0
    db_connection_formal.vk_user.remove({'uid':user_id_formal})
    db_connection_formal.vk_user.insert(user_info_response['response'][0])
    db_connection_formal.vk_user_daily.insert(user_info_response['response'][0])
def getNewWallPosts(user_id_formal, access_token_formal, db_connection_formal, json_formal, urllib_formal):
    last_date=''
    last_update = db_connection_formal.vk_wall_posts.find({'user_id':user_id_formal}, {'date':1, '_id':0}).sort([('date',-1)]).limit(1)
    for max_date in last_update:
        last_date = max_date['date']
    if last_date!='':
        max_timestamp =time.mktime(last_date.timetuple())
        iterator = 1
        offset = 0
        count = 100
        wall_response = urllib_formal.urlopen("https://api.vk.com/method/wall.get?owner_id="+user_id_formal+'&offset='+str(offset)+'&count='+str(count))
        wall_info_response = json_formal.loads(wall_response.read())
        while iterator!=0:
            for items in wall_info_response['response']:
                if type(items) is int:
                    indexof = wall_info_response['response'].index(items)
                elif items['date']> max_timestamp:
                    date_unformated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(items['date'])))
                    items['date'] = datetime.datetime.strptime(date_unformated ,"%Y-%m-%dT%H:%M:%SZ")
                    items['sentiment'] = ''
                    items['keywords'] = []
                    items['likes_crawled']='no'
                    items['comments_crawled']='no'
                    from_foll = db_connection_formal.vk_followers.find({'uid':int(items['from_id'])}, {'first_name':1, 'last_name':1, '_id':0}).count()
                    from_friend = db_connection_formal.vk_friends.find({'uid':int(items['from_id'])}, {'first_name':1, 'last_name':1, '_id':0}).count()
                    if from_foll!=0:
                        from_name = db_connection_formal.vk_followers.find({'uid':int(items['from_id'])}, {'first_name':1, 'last_name':1, '_id':0})
                        for name in from_name:
                            foll_name = name['first_name']+' '+name['last_name']
                    elif from_friend!=0:
                        from_name = db_connection_formal.vk_friends.find({'uid':int(items['from_id'])}, {'first_name':1, 'last_name':1, '_id':0})
                        for name in from_name:
                            foll_name = name['first_name']+' '+name['last_name']
                    else:
                        from_name = db_connection_formal.vk_user.find({'uid':int(items['from_id'])}, {'first_name':1, 'last_name':1, '_id':0})
                        for name in from_name:
                            foll_name = name['first_name']+' '+name['last_name']
                    items['from_name']=foll_name
                    items['reach']= items['likes']['count']+items['comments']['count']+items['reposts']['count']
                    items['liked'] = 0
                    items['user_id'] = int(user_id_formal)
                    db_connection_formal.vk_wall_posts.insert(items)
                else:
                    pass
            if (wall_info_response['response'][indexof]-offset) > count:
                offset = offset+iterator*count
                iterator=iterator+1
                wall_response = urllib_formal.urlopen("https://api.vk.com/method/wall.get?owner_id="+user_id_formal+'&offset='+str(offset)+'&count='+str(count))
                wall_info_response = json_formal.loads(user_info.read())
            else:
                iterator = 0
def wallPostUpdate(user_id_formal, access_token_formal, db_connection_formal, json_formal, urllib_formal):
    time_range = datetime.datetime.now()-datetime.timedelta(days=7)
    recent_post = db_connection_formal.vk_wall_posts.find({'user_id':user_id_formal, 'date':{'$gt':time_range}}, {'user_id':1, 'id':1, 'comments':1, 'likes':1, 'reposts':1})
    for each_post in recent_post:
        posts = str(each_post['user_id'])+str(each_post['id'])
        new_response = urllib_formal.urlopen("https://api.vk.com/method/wall.getById?posts="+posts)
        like_crawled = 'yes'
        comments_crawled = 'yes'
        if new_response['likes']['count'] !=each_post['likes']['count']:
            like_crawled='no'
        if new_response['comments']['count'] !=each_post['comments']['count']:
            comments_crawled = 'no'
        db_connection_formal.vk_wall_posts.update({'user_id':user_id_formal, 'id':each_post['id']}, {'$set':{'likes_crawled':like_crawled, 'comments_crawled':comments_crawled, 'likes.count':new_response['likes']['count'], 'comments.count':new_response['comments']['count'], 'reposts.count':new_response['reposts']['count']}})

def getWallComments(user_id_formal, access_token_formal, db_connection_formal, json_formal, urllib_formal):
    get_posts= db_connection_formal.vk_wall_posts.find({'user_id':str(user_id_formal), 'comments.count':{'$gt':0}, 'comments_crawled':'no'}, {'_id':0, 'id':1})
    for post_id in get_posts:
        all_comments = db_connection_formal.vk_wall_comments.find({'user_id':user_id_formal, 'post_id':post_id['id']}, {'cid':1, '_id':0})
        comment_existing = [comments['cid'] for comments in all_comments]
        id_list = []
        iterator = 1
        offset = 0
        count = 100
        comment_response = urllib_formal.urlopen("https://api.vk.com/method/wall.getComments?owner_id="+user_id_formal+'&post_id='+str(post_id['id'])+'&offset='+str(offset)+'&count='+str(count))
        comment_response_formated = json_formal.loads(comment_response.read())
        while iterator!=0:
            for items in comment_response_formated['response']:
                if type(items) is int:
                    indexof = comment_response_formated['response'].index(items)
                else:
                    if items['cid'] not in comment_existing:
                        items['post_id'] = post_id['id']
                        date_unformated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(items['date'])))
                        items['date'] = datetime.datetime.strptime(date_unformated ,"%Y-%m-%dT%H:%M:%SZ")
                        items['sentiment'] = ''
                        items['keywords'] = []
                        items['user_id'] = user_id_formal
                        db_connection_formal.vk_wall_comments.insert(items)
            if (comment_response_formated['response'][indexof]-offset) > count:
                offset = offset+iterator*count
                iterator=iterator+1
                comment_response = urllib_formal.urlopen("https://api.vk.com/method/wall.getComments?owner_id="+user_id_formal+'&post_id='+str(post_id['id'])+'&offset='+str(offset)+'&count='+str(count))
                comment_response_formated = json_formal.loads(comment_response.read())
            else:
                iterator = 0
def getWallLikes(user_id_formal, access_token_formal, db_connection_formal, json_formal, urllib_formal):
    get_posts= db_connection_formal.vk_wall_posts.find({'user_id':str(user_id_formal), 'likes.count':{'$gt':0}, 'likes_crawled':'no'}, {'_id':0, 'id':1})
    for post_id in get_posts:
        id_list = []
        iterator = 1
        offset = 0
        count = 100
        likers=[]
        url = "https://api.vk.com/method/likes.getList?type=post&owner_id="+user_id_formal+'&item_id='+str(post_id['id'])+'&offset='+str(offset)+'&count='+str(count)
        print url
        comment_response = urllib_formal.urlopen(url)
        comment_response_formated = json_formal.loads(comment_response.read())
        while iterator!=0:
            likers.extend(comment_response_formated['response']['users'])
            if comment_response_formated['response']['count'] > count:
                offset = offset+iterator*count
                iterator=iterator+1
                comment_response = urllib_formal.urlopen("https://api.vk.com/method/likes.getList?type=post&owner_id="+user_id_formal+'&item_id='+str(post_id['id'])+'&offset='+str(offset)+'&count='+str(count))
                comment_response_formated = json_formal.loads(comment_response.read())
            else:
                iterator = 0
        db_connection_formal.vk_wall_likes.update({'user_id':str(user_id_formal), 'post_id':post_id['id']}, {'$set':{'likers':likers}})
        if int(user_id_formal) in likers:
            db_connection_formal.vk_wall_posts.update({'user_id':int(user_id_formal), 'id': post_id['id']}, {'$set':{'liked':1}})
        else:
            db_connection_formal.vk_wall_posts.update({'user_id':int(user_id_formal), 'id': post_id['id']}, {'$set':{'liked':0}})
def dailyReachCalculator(user_id_formal, db_connection_formal):
    max_date=''
    date_response = db_connection_formal.vk_daily_reach.find({'user_id':user_id_formal}, {'date':1, '_id':0}).sort([('date',-1)]).limit(1)
    for date_max in date_response:
        max_date = date_max['date']
    if max_date!='':
        reach_response = db_connection_formal.vk_wall_posts.aggregate(
           [{"$match":{"user_id":str(user_id_formal), 'date':{'$gt':maxDate}}},
              {
                "$group" : {
                   "_id" : { "month": { "$month": "$date" }, "day": { "$dayOfMonth": "$date" }, "year": { "$year": "$date" } },
                   "reach":{"$sum":{"$add":["$comments.count", "$likes.count", "$reposts.count"]}}
                }
              }
           ]
        )
        foll_res = db_connection_formal.vk_user.find({'uid':int(user_id_formal)}, {'counters':1, '_id':0})
        for foll in foll_res:
            follower = foll['counters']['followers']+foll['counters']['friends']
        reach_json={
            "date":"",
            "user_id":"",
            "reach":""
        }
        reach_list = []
    else:
        reach_response = db_connection_formal.vk_wall_posts.aggregate(
           [{"$match":{"user_id":str(user_id_formal)}},
              {
                "$group" : {
                   "_id" : { "month": { "$month": "$date" }, "day": { "$dayOfMonth": "$date" }, "year": { "$year": "$date" } },
                   "reach":{"$sum":{"$add":["$comments.count", "$likes.count", "$reposts.count"]}}
                }
              }
           ]
        )
        foll_res = db_connection_formal.vk_user.find({'uid':int(user_id_formal)}, {'counters':1, '_id':0})
        for foll in foll_res:
            follower = foll['counters']['followers']+foll['counters']['friends']
        reach_json={
            "date":"",
            "user_id":"",
            "reach":""
        }
        reach_list = []
    for result in reach_response['result']:
        print reach_response, "=========================="
        dateStr = str(result["_id"]["year"])+"-"+str(result["_id"]["month"])+"-"+str(result["_id"]["day"])+"T10:30:01+0000"
        reach_json['date'] = datetime.datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S+0000")
        reach_json["reach"] = int((result["reach"]/float(follower))*100)
        reach_json["user_id"] = int(user_id_formal)
        reach_list.append(reach_json.copy())
    if len(reach_list)>0:
        db_connection_formal.vk_daily_reach.insert(reach_list)