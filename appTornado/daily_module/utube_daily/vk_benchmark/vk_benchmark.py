__author__ = 'sibiaanalytics'
from nltk.corpus import names
import random
from bson.objectid import ObjectId
import nltk
import time
from datetime import datetime
from datetime import date
import urllib
import json
import datetime as datetime2

def calculate_age(born):
    today = date.today()
    return today.year - born.year

def userFetch(db_connection_formal,user_id_formal, access_token_formal, brand_id_formal, master_page_id):
    profile_fields = "sex, bdate, city, country, photo_50, contacts, followers_count, status, counters"
    user_info = urllib.urlopen('https://api.vk.com/method/users.get?user_id='+user_id_formal+'&access_token='+access_token_formal+'&fields='+profile_fields)
    user_info_response = json.loads(user_info.read())
    city_name=''
    country_name = ''
    city_response = db_connection_formal.vk_city.find({'id':user_info_response['response'][0]['city']})
    for city in city_response:
        city_name = city['name']
    if city_name == '':
        city_name=cityFinder(user_info_response['response'][0]['city'], db_connection_formal)
    country_response = db_connection_formal.vk_country.find({'id':user_info_response['response'][0]['country']})
    for country in country_response:
        country_name = country['name']
    if country_name == '':
        country_name=countryFinder(user_info_response['response'][0]['country'], db_connection_formal)
    user_info_response['response'][0]['city']=city_name
    user_info_response['response'][0]['country']=country_name
    if 'bdate' in user_info_response['response'][0] and len(user_info_response['response'][0]['bdate'])==8:
        bdatelist = user_info_response['response'][0]['bdate'].split('.')
        dateStr = str(bdatelist[2])+"-"+str(bdatelist[1])+"-"+str(bdatelist[0])+"T10:30:01+0000"
        user_info_response['response'][0]['bdate'] = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S+0000")
    else:
        user_info_response['response'][0]['bdate'] = 0
    user_info_response['response'][0]['brand_id']=brand_id_formal
    user_info_response['response'][0]['master_page']=master_page_id
    db_connection_formal.vk_user_comp.remove({'uid':user_id_formal})
    db_connection_formal.vk_user_comp.insert(user_info_response['response'][0])
def cityFinder(city_id_formal, db_connection_formal):
    print city_id_formal, "==============>>>>"
    city_info= urllib.urlopen('https://api.vk.com/method/database.getCitiesById?city_ids='+str(city_id_formal))
    city_info_response = json.loads(city_info.read())
    city_json = {
        'id':0,
        'name':''
    }
    city_json['id'] = city_id_formal
    city_json['name'] = city_info_response['response'][0]['name']
    db_connection_formal.vk_city.insert(city_json)
    city = city_info_response['response'][0]['name']
    return city
def countryFinder(country_id_formal, db_connection_formal):
    country_info = urllib.urlopen('https://api.vk.com/method/database.getCountriesById?country_ids='+str(country_id_formal))
    country_info_response = json.loads(country_info.read())
    country_json = {
        'id':0,
        'name':''
    }
    country_json['id'] = country_id_formal
    country_json['name'] = country_info_response['response'][0]['name']
    db_connection_formal.vk_country.insert(country_json)
    country = country_info_response['response'][0]['name']
    return country

def getNewWallPosts(db_connection_formal,user_id_formal, access_token_formal, brand_id_formal, master_page_id):
    last_date=''
    last_update = db_connection_formal.vk_wall_posts_comp.find({'user_id':user_id_formal, 'master_page':str(master_page_id), 'brand_id':str(brand_id_formal)}, {'date':1, '_id':0}).sort([('date',-1)]).limit(1)
    for max_date in last_update:
        last_date = max_date['date']
    if last_date!='':
        max_timestamp =time.mktime(last_date.timetuple())
        iterator = 1
        offset = 0
        count = 100
        wall_response = urllib.urlopen("https://api.vk.com/method/wall.get?owner_id="+user_id_formal+'&offset='+str(offset)+'&count='+str(count))
        wall_info_response = json.loads(wall_response.read())
        while iterator!=0:
            for items in wall_info_response['response']:
                if type(items) is int:
                    indexof = wall_info_response['response'].index(items)
                elif items['date']> max_timestamp:
                    date_unformated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(items['date'])))
                    items['date'] = datetime2.datetime.strptime(date_unformated ,"%Y-%m-%dT%H:%M:%SZ")
                    items['sentiment'] = ''
                    items['keywords'] = []
                    items['likes_crawled']='no'
                    items['comments_crawled']='no'
                    from_foll = db_connection_formal.vk_followers_comp.find({'uid':int(items['from_id'])}, {'first_name':1, 'last_name':1, '_id':0}).count()
                    from_friend = db_connection_formal.vk_friends_comp.find({'uid':int(items['from_id'])}, {'first_name':1, 'last_name':1, '_id':0}).count()
                    if from_foll!=0:
                        from_name = db_connection_formal.vk_followers_comp.find({'uid':int(items['from_id'])}, {'first_name':1, 'last_name':1, '_id':0})
                        for name in from_name:
                            foll_name = name['first_name']+' '+name['last_name']
                    elif from_friend!=0:
                        from_name = db_connection_formal.vk_friends_comp.find({'uid':int(items['from_id'])}, {'first_name':1, 'last_name':1, '_id':0})
                        for name in from_name:
                            foll_name = name['first_name']+' '+name['last_name']
                    else:
                        from_name = db_connection_formal.vk_user_comp.find({'uid':int(items['from_id'])}, {'first_name':1, 'last_name':1, '_id':0})
                        for name in from_name:
                            foll_name = name['first_name']+' '+name['last_name']
                    items['from_name']=foll_name
                    items['reach']= items['likes']['count']+items['comments']['count']+items['reposts']['count']
                    items['liked'] = 0
                    items['user_id'] = int(user_id_formal)
                    db_connection_formal.vk_wall_posts_comp.insert(items)
                else:
                    pass
            if (wall_info_response['response'][indexof]-offset) > count:
                offset = offset+iterator*count
                iterator=iterator+1
                wall_response = urllib.urlopen("https://api.vk.com/method/wall.get?owner_id="+user_id_formal+'&offset='+str(offset)+'&count='+str(count))
                wall_info_response = json.loads(wall_response.read())
            else:
                iterator = 0
def wallPostUpdate(db_connection_formal,user_id_formal, access_token_formal, brand_id_formal, master_page_id):
    time_range = datetime2.datetime.now()-datetime2.timedelta(days=7)
    recent_post = db_connection_formal.vk_wall_posts_comp.find({'user_id':user_id_formal, 'master_page':str(master_page_id), 'brand_id':str(brand_id_formal), 'date':{'$gt':time_range}}, {'user_id':1, 'id':1, 'comments':1, 'likes':1, 'reposts':1})
    for each_post in recent_post:
        posts = str(each_post['user_id'])+str(each_post['id'])
        new_response = urllib.urlopen("https://api.vk.com/method/wall.getById?posts="+posts)
        like_crawled = 'yes'
        comments_crawled = 'yes'
        if new_response['likes']['count'] !=each_post['likes']['count']:
            like_crawled='no'
        if new_response['comments']['count'] !=each_post['comments']['count']:
            comments_crawled = 'no'
        db_connection_formal.vk_wall_posts_comp.update({'user_id':user_id_formal, 'id':each_post['id']}, {'$set':{'likes_crawled':like_crawled, 'comments_crawled':comments_crawled, 'likes.count':new_response['likes']['count'], 'comments.count':new_response['comments']['count'], 'reposts.count':new_response['reposts']['count']}})

def gender_features(word):
    return {'last_letter': word[-1]}
def genderClassifier(db_connection_formal,user_id_formal):
    labeled_names = ([(name, 'male') for name in names.words('male.txt')] + [(name, 'female') for name in names.words('female.txt')])
    random.shuffle(labeled_names)
    featuresets = [(gender_features(n), gender) for (n, gender) in labeled_names]
    classifier = nltk.NaiveBayesClassifier.train(featuresets)
    follower_response = db_connection_formal.vk_friends_comp.find({'user_id':int(user_id_formal), 'sex':0}, {'first_name':1, '_id':1})
    for follower in follower_response:
        try:
            first_name = follower['first_name']
            gender_str = classifier.classify(gender_features(first_name))
            if gender_str=='male':
                gender = 2
            else:
                gender = 1
        except:
            gender = 2
        db_connection_formal.vk_friends_comp.update({'_id':ObjectId(follower['_id'])}, {'$set':{'sex':gender}})
    follower_response = db_connection_formal.vk_followers_comp.find({'user_id':int(user_id_formal), 'sex':0}, {'first_name':1, '_id':1})
    for follower in follower_response:
        try:
            first_name = follower['first_name']
            gender_str = classifier.classify(gender_features(first_name))
            if gender_str=='male':
                gender = 2
            else:
                gender = 1
        except:
            gender = 2
        db_connection_formal.vk_followers_comp.update({'_id':ObjectId(follower['_id'])}, {'$set':{'sex':gender}})
def dailyReachCalculator(db_connection_formal,user_id_formal, brand_id_formal, master_page_id):
    reach_response = db_connection_formal.vk_wall_posts_comp.aggregate(
       [{"$match":{"user_id":int(user_id_formal)}},
          {
            "$group" : {
               "_id" : { "month": { "$month": "$date" }, "day": { "$dayOfMonth": "$date" }, "year": { "$year": "$date" } },
               "reach":{"$sum":{"$add":["$comments.count", "$likes.count", "$reposts.count"]}}
            }
          }
       ]
    )
    foll_res = db_connection_formal.vk_user_comp.find({'uid':int(user_id_formal)}, {'counters':1, '_id':0})
    for foll in foll_res:
        follower = foll['counters']['followers']+foll['counters']['friends']
    if follower==0:
        follower=1
    reach_json={
        "date":"",
        "user_id":"",
        "reach":"",
        "brand_id" : brand_id_formal,
        "master_page" : master_page_id
    }
    reach_list = []
    for result in reach_response['result']:
        print reach_response, "==========================",reach_response['result']
        dateStr = str(result["_id"]["year"])+"-"+str(result["_id"]["month"])+"-"+str(result["_id"]["day"])+"T10:30:01+0000"
        reach_json['date'] = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S+0000")
        reach_json["reach"] = int((result["reach"]/float(follower))*100)
        reach_json["user_id"] = int(user_id_formal)
        reach_list.append(reach_json.copy())
    print 'REACH_LIST================>',reach_list
    if len(reach_list) != 0:
        db_connection_formal.vk_daily_reach_comp.insert(reach_list)
    else:
        pass





