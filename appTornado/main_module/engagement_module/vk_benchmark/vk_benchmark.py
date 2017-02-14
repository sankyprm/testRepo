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
def userFollowers(db_connection_formal,user_id_formal, access_token_formal, brand_id_formal, master_page_id):
    profile_fields = "sex, bdate, city, country, photo_50, contacts, followers_count, status, counters"
    loop_counter=0
    iterator = 1
    offset = 0
    count = 1000
    user_info = urllib.urlopen('https://api.vk.com/method/users.getFollowers?user_id='+user_id_formal+'&access_token='+access_token_formal+'&fields='+profile_fields+'&offset='+str(offset)+'&count='+str(count))
    user_follower_response = json.loads(user_info.read())
    if user_follower_response.has_key('error'):
        db_connection_formal.vk_user_comp.remove({'uid':int(user_id_formal)})
        return 'error'
    else:
        while iterator!=0 and loop_counter<2:
            for items in user_follower_response['response']['items']:
                city_name=''
                country_name = ''
                try:
                    city_response = db_connection_formal.vk_city.find({'id':items['city']})
                    #print 'ITEMS=========>',items
                    for city in city_response:
                        city_name = city['name']
                        #print 'CITY_NAME=====>',city_name
                    if city_name == '':
                        #print 'ITEM_CITY=======>',items['city']
                        if items['city'] == 0:
                            json_un = {'id' : 0,
                                       'name' : 'Undefined'}
                            db_connection_formal.vk_city.insert(json_un)
                        else:
                            city_name=cityFinder(items['city'], db_connection_formal)
                    country_response = db_connection_formal.vk_country.find({'id':items['country']})
                    for country in country_response:
                        country_name = country['name']
                        #print 'COUNTRY_NAME=====>',country_name
                    if country_name == '':
                        #print 'items["country"]====================>',items['country']
                        if items['country'] == 0:
                            json_un = {'id' : 0,
                                       'name' : 'Undefined'}
                            db_connection_formal.vk_country.insert(json_un)
                        else:
                            country_name=countryFinder(items['country'], db_connection_formal)
                except:
                    city_name='Undefined'
                    country_name = 'Undefined'
                items['city'] = city_name
                items['country'] = country_name
                items['user_id'] = int(user_id_formal)
                items['brand_id'] = brand_id_formal
                items['master_page'] = master_page_id
                if 'bdate' in items and len(items['bdate'])==8:
                    bdatelist = items['bdate'].split('.')
                    dateStr = str(bdatelist[2])+"-"+str(bdatelist[1])+"-"+str(bdatelist[0])+"T10:30:01+0000"
                    items['bdate'] = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S+0000")
                    date_formated = date(int(bdatelist[2]), int(bdatelist[1]), int(bdatelist[0]))
                    age = calculate_age(date_formated)
                    items['age']= age
                else:
                    items['bdate'] = 0
                    items['age'] = 0
                db_connection_formal.vk_followers_comp.insert(items)
            if (user_follower_response['response']['count']-offset) > count:
                loop_counter+=1
                offset = offset+iterator*count
                iterator=iterator+1
                user_info = urllib.urlopen('https://api.vk.com/method/users.getFollowers?user_id='+user_id_formal+'&access_token='+access_token_formal+'&fields='+profile_fields+'&offset='+str(offset)+'&count='+str(count))
                user_follower_response = json.loads(user_info.read())
            else:
                iterator = 0
        return 1
def userFriends(db_connection_formal,user_id_formal, access_token_formal, brand_id_formal, master_page_id):
    profile_fields = "sex, bdate, city, country, photo_50, contacts, followers_count, status, counters"
    user_info = urllib.urlopen('https://api.vk.com/method/friends.get?user_id='+user_id_formal+'&access_token='+access_token_formal+'&fields='+profile_fields)
    user_follower_response = json.loads(user_info.read())
    if user_follower_response.has_key('error'):
        db_connection_formal.vk_user_comp.remove({'uid':int(user_id_formal)})
        db_connection_formal.vk_followers_comp.remove({'user_id':int(user_id_formal)})
        return 'error'
    else:
        for items in user_follower_response['response']:
            city_name=''
            country_name = ''
            try:
                if items['city']==0:
                    city_name='Undefined'
                else:
                    city_response = db_connection_formal.vk_city.find({'id':items['city']})
                    for city in city_response:
                        city_name = city['name']
                    if city_name == '':
                        city_name=cityFinder(items['city'], db_connection_formal)
                if items['country']==0:
                    country_name = 'Undefined'
                else:
                    country_response = db_connection_formal.vk_country.find({'id':items['country']})
                    for country in country_response:
                        country_name = country['name']
                    if country_name == '':
                        country_name=countryFinder(items['country'], db_connection_formal)
            except:
                city_name = 'Undefined'
                country_name = 'Undefined'
            items['city'] = city_name
            items['country'] = country_name
            items['user_id'] = int(user_id_formal)
            items['brand_id'] = brand_id_formal
            items['master_page'] = master_page_id
            if 'bdate' in items and len(items['bdate'])==8:
                bdatelist = items['bdate'].split('.')
                dateStr = str(bdatelist[2])+"-"+str(bdatelist[1])+"-"+str(bdatelist[0])+"T10:30:01+0000"
                items['bdate'] = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S+0000")
                date_formated = date(int(bdatelist[2]), int(bdatelist[1]), int(bdatelist[0]))
                age = calculate_age(date_formated)
                items['age']= age
            else:
                items['bdate'] = 0
                items['age']= 0
            db_connection_formal.vk_friends_comp.insert(items)
        return 1
def getWallPosts( db_connection_formal,user_id_formal, access_token_formal, brand_id_formal, master_page_id):
    loop_counter = 0
    iterator = 1
    offset = 0
    count = 100
    wall_response = urllib.urlopen("https://api.vk.com/method/wall.get?owner_id="+user_id_formal+'&offset='+str(offset)+'&count='+str(count))
    wall_info_response = json.loads(wall_response.read())
    if wall_info_response.has_key('error'):
        db_connection_formal.vk_user_comp.remove({'uid':int(user_id_formal)})
        db_connection_formal.vk_followers_comp.remove({'user_id':int(user_id_formal)})
        db_connection_formal.vk_friends_comp.remove({'user_id':int(user_id_formal)})
        return 'error'
    else:
        while iterator!=0 and loop_counter<10:
            for items in wall_info_response['response']:
                if type(items) is int:
                    indexof = wall_info_response['response'].index(items)
                else:
                    #print("ITERATOR====>",iterator)
                    #print('ITEMS IN GETWALLPOSTS===========================>',items)
                    #print('DATES====================>',type(items['date']))
                    if type(items['date']) is datetime:
                        date_unformated_timestamp = (items['date'] - datetime(1970, 1, 1, 0, 0)).total_seconds()
                        date_unformated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(date_unformated_timestamp)))
                    else:
                        date_unformated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(items['date'])))
                    #date_unformated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(items['date']))
                    items['date'] = datetime.strptime(date_unformated ,"%Y-%m-%dT%H:%M:%SZ")
                    items['sentiment'] = ''
                    items['keywords'] = []
                    items['likes_crawled']='yes'
                    items['comments_crawled']='yes'
                    try:
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
                    except:
                        foll_name=''
                    items['from_name']=foll_name
                    items['reach']= items['likes']['count']+items['comments']['count']+items['reposts']['count']
                    items['liked'] = 0
                    items['user_id'] = int(user_id_formal)
                    items['brand_id'] = brand_id_formal
                    items['master_page'] = master_page_id
                    item1 = items.copy()
                    db_connection_formal.vk_wall_posts_comp.insert(item1)
            if (wall_info_response['response'][indexof]-offset) > count:
                loop_counter+=1
                offset = offset+iterator*count
                iterator=iterator+1
                wall_response = urllib.urlopen("https://api.vk.com/method/wall.get?owner_id="+user_id_formal+'&offset='+str(offset)+'&count='+str(count))
                #print("ITERATOR====>",iterator)
                #print("WALL RESPONSE ITERATOR====>",wall_response)
                try:
                    for item in wall_response['response']:
                        #print('item===============================================================================================================================>',item)
                        if item == indexof:
                            pass
                        else:
                            #wall_info_response = json.loads(wall_response.read())
                            wall_info_response = json.loads(item.read())
                except:
                    pass
            else:
                iterator = 0
        return 1
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
def agegroupCalculator(db_connection_formal,user_id_formal, brand_id_formal, master_page_id):
    age_group_json = {
        "user_id":user_id_formal,
        "master_page":master_page_id,
        "brand_id":brand_id_formal,
        "age_group":{
        "M-13-17":0,
        "M-18-24":0,
        "M-25-34":0,
        "M-35-44":0,
        "M-45-64":0,
        "M-65+":0,
        "F-13-17":0,
        "F-18-24":0,
        "F-25-34":0,
        "F-35-44":0,
        "F-45-64":0,
        "F-65+":0
    }}
    follower_list = db_connection_formal.vk_followers_comp.find({'user_id':int(user_id_formal)}, {'age':1, 'sex':1, '_id':0})
    for foll in follower_list:
        if foll['age']>=0 and foll['age']<=17:
            if foll['sex']==1:
                age_group_json['age_group']['F-13-17']= age_group_json['age_group']['F-13-17']+1
            else:
                age_group_json['age_group']['M-13-17'] = age_group_json['age_group']['M-13-17']+1
        if foll['age']>=18 and foll['age']<=24:
            if foll['sex']==1:
                age_group_json['age_group']['F-18-24']= age_group_json['age_group']['F-18-24']+1
            else:
                age_group_json['age_group']['M-18-24'] = age_group_json['age_group']['M-18-24']+1
        if foll['age']>=25 and foll['age']<=34:
            if foll['sex']==1:
                age_group_json['age_group']['F-25-34']= age_group_json['age_group']['F-25-34']+1
            else:
                age_group_json['age_group']['M-25-34'] = age_group_json['age_group']['M-25-34']+1
        if foll['age']>=35 and foll['age']<=44:
            if foll['sex']==1:
                age_group_json['age_group']['F-35-44']= age_group_json['age_group']['F-35-44']+1
            else:
                age_group_json['age_group']['M-35-44'] = age_group_json['age_group']['M-35-44']+1
        if foll['age']>=35 and foll['age']<=44:
            if foll['sex']==1:
                age_group_json['age_group']['F-35-44']= age_group_json['age_group']['F-35-44']+1
            else:
                age_group_json['age_group']['M-35-44'] = age_group_json['age_group']['M-35-44']+1
        if foll['age']>=45 and foll['age']<=64:
            if foll['sex']==1:
                age_group_json['age_group']['F-45-64']= age_group_json['age_group']['F-45-64']+1
            else:
                age_group_json['age_group']['M-45-64'] = age_group_json['age_group']['M-45-64']+1
        if foll['age']>=65:
            if foll['sex']==1:
                age_group_json['age_group']['F-65+']= age_group_json['age_group']['F-65+']+1
            else:
                age_group_json['age_group']['M-65+'] = age_group_json['age_group']['M-65+']+1
    follower_list = db_connection_formal.vk_friends_comp.find({'user_id':int(user_id_formal)}, {'age':1, 'sex':1, '_id':0})
    for foll in follower_list:
        if foll['age']>=0 and foll['age']<=17:
            if foll['sex']==1:
                age_group_json['age_group']['F-13-17']= age_group_json['age_group']['F-13-17']+1
            else:
                age_group_json['age_group']['M-13-17'] = age_group_json['age_group']['M-13-17']+1
        if foll['age']>=18 and foll['age']<=24:
            if foll['sex']==1:
                age_group_json['age_group']['F-18-24']= age_group_json['age_group']['F-18-24']+1
            else:
                age_group_json['age_group']['M-18-24'] = age_group_json['age_group']['M-18-24']+1
        if foll['age']>=25 and foll['age']<=34:
            if foll['sex']==1:
                age_group_json['age_group']['F-25-34']= age_group_json['age_group']['F-25-34']+1
            else:
                age_group_json['age_group']['M-25-34'] = age_group_json['age_group']['M-25-34']+1
        if foll['age']>=35 and foll['age']<=44:
            if foll['sex']==1:
                age_group_json['age_group']['F-35-44']= age_group_json['age_group']['F-35-44']+1
            else:
                age_group_json['age_group']['M-35-44'] = age_group_json['age_group']['M-35-44']+1
        if foll['age']>=35 and foll['age']<=44:
            if foll['sex']==1:
                age_group_json['age_group']['F-35-44']= age_group_json['age_group']['F-35-44']+1
            else:
                age_group_json['age_group']['M-35-44'] = age_group_json['age_group']['M-35-44']+1
        if foll['age']>=45 and foll['age']<=64:
            if foll['sex']==1:
                age_group_json['age_group']['F-45-64']= age_group_json['age_group']['F-45-64']+1
            else:
                age_group_json['age_group']['M-45-64'] = age_group_json['age_group']['M-45-64']+1
        if foll['age']>=65:
            if foll['sex']==1:
                age_group_json['age_group']['F-65+']= age_group_json['age_group']['F-65+']+1
            else:
                age_group_json['age_group']['M-65+'] = age_group_json['age_group']['M-65+']+1
    db_connection_formal.vk_age_group_comp.insert(age_group_json)




