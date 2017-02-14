__author__ = 'sibiaanalytics'
from nltk.corpus import names
import random
from bson.objectid import ObjectId
import nltk
import time
from datetime import datetime
from datetime import date

def userFetch(user_id_formal, access_token_formal, db_connection_formal, json_formal, urllib_formal, brand_id):
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
    if 'bdate' in user_info_response['response'][0] and len(user_info_response['response'][0]['bdate'])==8:
        bdatelist = user_info_response['response'][0]['bdate'].split('.')
        dateStr = str(bdatelist[2])+"-"+str(bdatelist[1])+"-"+str(bdatelist[0])+"T10:30:01+0000"
        user_info_response['response'][0]['bdate'] = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S+0000")
    else:
        user_info_response['response'][0]['bdate'] = 0
    user_info_response['response'][0]['brand_id'] = [brand_id]
    db_connection_formal.vk_user.insert(user_info_response['response'][0])
    user_info_response['response'][0]["date"]=datetime.now()
    db_connection_formal.vk_user_daily.insert(user_info_response['response'][0])
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
        country_json = {
            'id':0,
            'name':''
        }
        country_json['id'] = country_id_formal
        country_json['name'] = 'Undefined'
        db_connection_formal.vk_country.insert(country_json)
        country = 'Undefined'
    return country
def userFollowers(user_id_formal, access_token_formal, db_connection_formal, json_formal, urllib_formal):
    profile_fields = "sex, bdate, city, country, photo_50, contacts, followers_count, status, counters"
    iterator = 1
    offset = 0
    count = 1000
    user_info = urllib_formal.urlopen('https://api.vk.com/method/users.getFollowers?user_id='+user_id_formal+'&access_token='+access_token_formal+'&fields='+profile_fields+'&offset='+str(offset)+'&count='+str(count))
    user_follower_response = json_formal.loads(user_info.read())
    while iterator!=0:
        for items in user_follower_response['response']['items']:
            city_name=''
            country_name = ''
            city_response = db_connection_formal.vk_city.find({'id':items['city']})
            for city in city_response:
                city_name = city['name']
            if city_name == '':
                city_name=cityFinder(items['city'], db_connection_formal, json_formal, urllib_formal)
            country_response = db_connection_formal.vk_country.find({'id':items['country']})
            for country in country_response:
                country_name = country['name']
            if country_name == '':
                country_name=countryFinder(items['country'], db_connection_formal, json_formal, urllib_formal)
            items['city'] = city_name
            items['country'] = country_name
            items['user_id'] = int(user_id_formal)
            if 'bdate' in items:
                bdatelist = items['bdate'].split('.')
                dateStr = str(bdatelist[2])+"-"+str(bdatelist[1])+"-"+str(bdatelist[0])+"T10:30:01+0000"
                items['bdate'] = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S+0000")
                date_formated = date(int(bdatelist[2]), int(bdatelist[1]), int(bdatelist[0]))
                age = calculate_age(date_formated)
                items['age']= age
            else:
                items['bdate'] = 0
                items['age'] = 0
            db_connection_formal.vk_followers.insert(items)
        if (user_follower_response['response']['count']-offset) > count:
            offset = offset+iterator*count
            iterator=iterator+1
            user_info = urllib_formal.urlopen('https://api.vk.com/method/users.getFollowers?user_id='+user_id_formal+'&access_token='+access_token_formal+'&fields='+profile_fields+'&offset='+str(offset)+'&count='+str(count))
            user_follower_response = json_formal.loads(user_info.read())
        else:
            iterator = 0
def userFriends(user_id_formal, access_token_formal, db_connection_formal, json_formal, urllib_formal):
    profile_fields = "sex, bdate, city, country, photo_50, contacts, followers_count, status, counters"
    user_info = urllib_formal.urlopen('https://api.vk.com/method/friends.get?user_id='+user_id_formal+'&access_token='+access_token_formal+'&fields='+profile_fields)
    user_follower_response = json_formal.loads(user_info.read())
    for items in user_follower_response['response']:
        city_name=''
        country_name = ''
        if items['city']==0:
            city_name='world'
        else:
            city_response = db_connection_formal.vk_city.find({'id':items['city']})
            for city in city_response:
                city_name = city['name']
            if city_name == '':
                city_name=cityFinder(items['city'], db_connection_formal, json_formal, urllib_formal)
        if items['country']==0:
            country_name = 'world'
        else:
            country_response = db_connection_formal.vk_country.find({'id':items['country']})
            for country in country_response:
                country_name = country['name']
            if country_name == '':
                country_name=countryFinder(items['country'], db_connection_formal, json_formal, urllib_formal)
        items['city'] = city_name
        items['country'] = country_name
        items['user_id'] = int(user_id_formal)
        if 'bdate' in items:
            bdatelist = items['bdate'].split('.')
            dateStr = str(bdatelist[2])+"-"+str(bdatelist[1])+"-"+str(bdatelist[0])+"T10:30:01+0000"
            items['bdate'] = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S+0000")
            date_formated = date(int(bdatelist[2]), int(bdatelist[1]), int(bdatelist[0]))
            age = calculate_age(date_formated)
            items['age']= age
        else:
            items['bdate'] = 0
            items['age']= 0
        db_connection_formal.vk_friends.insert(items)
def getWallPosts(user_id_formal, access_token_formal, db_connection_formal, json_formal, urllib_formal):
    iterator = 1
    offset = 0
    count = 100
    wall_response = urllib_formal.urlopen("https://api.vk.com/method/wall.get?owner_id="+user_id_formal+'&offset='+str(offset)+'&count='+str(count))
    wall_info_response = json_formal.loads(wall_response.read())
    while iterator!=0:
        for items in wall_info_response['response']:
            if type(items) is int:
                indexof = wall_info_response['response'].index(items)
            else:
                date_unformated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(items['date'])))
                items['date'] = datetime.strptime(date_unformated ,"%Y-%m-%dT%H:%M:%SZ")
                items['sentiment'] = ''
                items['keywords'] = []
                items['likes_crawled']='yes'
                items['comments_crawled']='yes'
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
        if (wall_info_response['response'][indexof]-offset) > count:
            offset = offset+iterator*count
            iterator=iterator+1
            wall_response = urllib_formal.urlopen("https://api.vk.com/method/wall.get?owner_id="+user_id_formal+'&offset='+str(offset)+'&count='+str(count))
            wall_info_response = json_formal.loads(user_info.read())
        else:
            iterator = 0
def getWallComments(user_id_formal, access_token_formal, db_connection_formal, json_formal, urllib_formal):

    get_posts= db_connection_formal.vk_wall_posts.find({'user_id':int(user_id_formal), 'comments.count':{'$gt':0}}, {'_id':0, 'id':1})
    for post_id in get_posts:
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
                    items['post_id'] = post_id['id']
                    date_unformated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(items['date'])))
                    items['date'] = datetime.strptime(date_unformated ,"%Y-%m-%dT%H:%M:%SZ")
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
    get_posts= db_connection_formal.vk_wall_posts.find({'user_id':int(user_id_formal), 'likes.count':{'$gt':0}}, {'_id':0, 'id':1})
    for post_id in get_posts:
        id_list = []
        iterator = 1
        offset = 0
        count = 100
        like_json = {
            'user_id':0,
            'post_id':0,
            'likers':[]
        }
        url = "https://api.vk.com/method/likes.getList?type=post&owner_id="+user_id_formal+'&item_id='+str(post_id['id'])+'&offset='+str(offset)+'&count='+str(count)
        print url
        comment_response = urllib_formal.urlopen(url)
        comment_response_formated = json_formal.loads(comment_response.read())
        while iterator!=0:
            like_json['user_id']= user_id_formal
            like_json['post_id'] = post_id['id']
            like_json['likers'].extend(comment_response_formated['response']['users'])
            if comment_response_formated['response']['count'] > count:
                offset = offset+iterator*count
                iterator=iterator+1
                comment_response = urllib_formal.urlopen("https://api.vk.com/method/likes.getList?type=post&owner_id="+user_id_formal+'&item_id='+str(post_id['id'])+'&offset='+str(offset)+'&count='+str(count))
                comment_response_formated = json_formal.loads(comment_response.read())
            else:
                iterator = 0
        db_connection_formal.vk_wall_likes.insert(like_json)
        if int(user_id_formal) in like_json['likers']:
            db_connection_formal.vk_wall_posts.update({'user_id':int(user_id_formal), 'id': post_id['id']}, {'$set':{'liked':1}})
        else:
            db_connection_formal.vk_wall_posts.update({'user_id':int(user_id_formal), 'id': post_id['id']}, {'$set':{'liked':0}})
def gender_features(word):
    return {'last_letter': word[-1]}
def genderClassifier(user_id_formal, db_connection_formal):
    labeled_names = ([(name, 'male') for name in names.words('male.txt')] + [(name, 'female') for name in names.words('female.txt')])
    random.shuffle(labeled_names)
    featuresets = [(gender_features(n), gender) for (n, gender) in labeled_names]
    classifier = nltk.NaiveBayesClassifier.train(featuresets)
    follower_response = db_connection_formal.vk_friends.find({'user_id':int(user_id_formal), 'sex':0}, {'first_name':1, '_id':1})
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
        db_connection_formal.vk_friends.update({'_id':ObjectId(follower['_id'])}, {'$set':{'sex':gender}})
    follower_response = db_connection_formal.vk_followers.find({'user_id':int(user_id_formal), 'sex':0}, {'first_name':1, '_id':1})
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
        db_connection_formal.vk_followers.update({'_id':ObjectId(follower['_id'])}, {'$set':{'sex':gender}})
def dailyReachCalculator(user_id_formal, db_connection_formal):
    reach_response = db_connection_formal.vk_wall_posts.aggregate(
       [{"$match":{"user_id":int(user_id_formal)}},
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
        reach_json['date'] = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S+0000")
        reach_json["reach"] = int((result["reach"]/float(follower))*100)
        reach_json["user_id"] = int(user_id_formal)
        reach_list.append(reach_json.copy())
    db_connection_formal.vk_daily_reach.insert(reach_list)
def agegroupCalculator(user_id_formal, db_connection_formal):
    age_group_json = {
        "user_id":user_id_formal,
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
    follower_list = db_connection_formal.vk_followers.find({'user_id':int(user_id_formal)}, {'age':1, 'sex':1, '_id':0})
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
    follower_list = db_connection_formal.vk_friends.find({'user_id':int(user_id_formal)}, {'age':1, 'sex':1, '_id':0})
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
    db_connection_formal.vk_age_group.insert(age_group_json)


def calculate_age(born):
    today = date.today()
    return today.year - born.year


