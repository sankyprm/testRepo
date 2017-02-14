__author__ = 'soumik'
from nltk.corpus import names
import random
from bson.objectid import ObjectId
import nltk
import time
from datetime import datetime
import datetime as datetime2
import urllib
import json
def userFetch(db_connection_formal,inst_user_id_formal,access_token_formal,brand_id_formal, master_page_id):
    print inst_user_id_formal, "====", access_token_formal
    user = urllib.urlopen('https://api.instagram.com/v1/users/'+inst_user_id_formal+'/?access_token='+access_token_formal)
    user_response = json.loads(user.read())
    print user_response
    user_json = user_response['data']
    user_json['ins_id'] = user_json.pop('id')
    user_json['brand_id'] = brand_id_formal
    user_json['master_page'] = master_page_id
    db_connection_formal.inst_user_comp.insert(user_json)
    return True
def recentMediaFetch(db_connection_formal,inst_user_id_formal, access_token_formal, brand_id_formal, master_page_id):
    next_url = 'https://api.instagram.com/v1/users/'+inst_user_id_formal+'/media/recent/?access_token='+access_token_formal
    counter = 0
    while next_url!='' and counter<8:
        recent_media =urllib.urlopen(next_url)
        recent_media_response = json.loads(recent_media.read())
        if len(recent_media_response['data'])!=0:
            recent_media_json = {
              "tags": [],
              "type": "image",
              "location": '',
              "comments_count": 0,
              "filter": "",
              "created_time": "0",
              "link": "",
              "likes_count": 0,
              "low_resolution": '',
              "thumbnail": '',
              "users_in_photo": [],
              "caption": '',
              "user_id":'',
              "video_url_standard":'',
              "video_url_low":'',
              "sentiment":'',
              "comments_crawled":'no',
              "likes_crawled":'no',
              "brand_id" : brand_id_formal,
              "master_page" : master_page_id
            }
            recent_media_list = []
            for media in recent_media_response['data']:
                recent_media_json['media_id']= media['id']
                recent_media_json['user_id'] = media['user']['id']
                try:
                    recent_media_json['caption'] = media['caption']['text']
                except:
                    recent_media_json['caption']=''
                try:
                    recent_media_json['users_in_photo'] = media['users_in_photo']
                except:
                    recent_media_json['users_in_photo']=''
                recent_media_json['thumbnail'] = media['images']['thumbnail']['url']
                recent_media_json['low_resolution'] = media['images']['low_resolution']['url']
                recent_media_json['standard_resolution'] = media['images']['standard_resolution']['url']
                recent_media_json['likes_count'] = media['likes']['count']
                recent_media_json['link'] = media['link']
                date_unformated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(media['created_time'])))
                recent_media_json['created_time'] = datetime.strptime(date_unformated ,"%Y-%m-%dT%H:%M:%SZ")
                try:
                    recent_media_json['filter'] = media['filter']
                except:
                    recent_media_json['filter'] = ''
                recent_media_json['comments_count'] = media['comments']['count']
                try:
                    recent_media_json['location'] = media['location']
                except:
                    recent_media_json['location'] = ''
                try:
                    recent_media_json['type'] = media['type']
                except:
                    recent_media_json['type']=''
                try:
                    recent_media_json["tags"] = media['tags']
                except:
                    recent_media_json["tags"]=''
                if str(media['type'])=='video':
                    recent_media_json['video_url_standard'] = media['videos']['standard_resolution']['url']
                    recent_media_json['video_url_low'] = media['videos']['low_resolution']['url']
                else:
                    recent_media_json['video_url_standard'] = ''
                    recent_media_json['video_url_low'] = ''
                recent_media_list.append(recent_media_json.copy())
            db_connection_formal.inst_media_comp.insert(recent_media_list)
        else:
            pass
        pagination = recent_media_response['pagination']
        if len(pagination)!=0:
            counter+=1
            next_url = pagination['next_url']
        else:
            next_url = ''
    return True
def recentMediaUpdate(db_connection_formal, inst_user_id_formal, access_token_formal, brand_id_formal, master_page_id):
    min_id=max_id=''
    time_range = datetime2.datetime.now()-datetime2.timedelta(days=5)
    max_id_response = db_connection_formal.inst_media_comp.find({'user_id':str(inst_user_id_formal),'master_page':str(master_page_id), 'created_time':{'$gt':time_range}}, {'media_id':1, '_id':0}).sort([('created_time',-1)]).limit(1)
    for max in max_id_response:
        max_id = max['media_id']
    min_id_response = db_connection_formal.inst_media_comp.find({'user_id':str(inst_user_id_formal),'master_page':str(master_page_id), 'created_time':{'$gt':time_range}}, {'media_id':1, '_id':0}).sort([('created_time',1)]).limit(1)
    for min in min_id_response:
        min_id = min['media_id']
    if min_id !='' and max_id!='':
        next_url = 'https://api.instagram.com/v1/users/'+inst_user_id_formal+'/media/recent/?access_token='+access_token_formal+'&min_id='+min_id+'&max_id='+max_id
        while next_url!='':
            recent_media =urllib.urlopen(next_url)
            recent_media_response = json.loads(recent_media.read())
            if len(recent_media_response['data'])!=0:
                for media in recent_media_response['data']:
                    cmnt_count = media['comments']['count']
                    likes_count = media['likes']['count']
                    db_response = db_connection_formal.inst_media_comp.find({'user_id':str(inst_user_id_formal), "media_id" :media['id']}, {'likes_count':1, 'comments_count':1, '_id':0})
                    for db_res in db_response:
                        cmnt = db_res['comments_count']
                        likes = db_res['likes_count']
                    comment_crawled = 'yes'
                    likes_crawled = 'yes'
                    if cmnt_count!=cmnt:
                        comment_crawled = 'no'
                    if likes_count!=likes:
                        likes_crawled = 'no'
                    db_connection_formal.inst_media_comp.update({'user_id':str(inst_user_id_formal), "media_id" :media['id']}, {'$set':{'comments_crawled':comment_crawled, 'likes_crawled':likes_crawled}})
            else:
                pass
            pagination = recent_media_response['pagination']
            if len(pagination)!=0:
                next_url = pagination['next_url']
            else:
                next_url = ''
    else:
        pass
    return True
def commentFetch( db_connection_formal,inst_user_id_formal,access_token_formal, media_id_list_formal, brand_id_formal, master_page_id):
    for ids in media_id_list_formal:
        comment_response = urllib.urlopen('https://api.instagram.com/v1/media/'+ids['media_id']+'/comments?access_token='+access_token_formal)
        comment_response_json = json.loads(comment_response.read())
        if len(comment_response_json['data'])!=0:
            comment_list = []
            for comments in comment_response_json['data']:
                comments['comment_id'] = comments.pop('id')
                comments['media_id'] = ids['media_id']
                comments['user_id'] = inst_user_id_formal
                comments['sentiment']= ''
                comments['keywords'] = ''
                comments['brand_id'] = brand_id_formal
                comments['master_page'] = master_page_id
                date_unformated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(comments['created_time'])))
                comments['created_time'] = datetime.strptime(date_unformated ,"%Y-%m-%dT%H:%M:%SZ")
                comment_list.append(comments.copy())
            db_connection_formal.inst_comments_comp.insert(comment_list)
        else:
            pass
        db_connection_formal.inst_media_comp.update({'user_id':str(inst_user_id_formal), "media_id" :ids['media_id']}, {'$set':{'comments_crawled':'yes'}})
    return True

def followedByFetch(db_connection_formal, inst_user_id_formal, access_token_formal, brand_id_formal, master_page_id):
    next_url = 'https://api.instagram.com/v1/users/'+inst_user_id_formal+'/followed-by?access_token='+access_token_formal
    counter = 0
    while next_url!= '' and counter<100:
        followed_by = urllib.urlopen(next_url)
        followed_by_response = json.loads(followed_by.read())
        if len(followed_by_response['data'])!=0:
            followed_by_list = []
            for follower in followed_by_response['data']:
                follower['follower_id'] = follower.pop('id')
                follower['follower_of'] = inst_user_id_formal
                follower['gender'] = ''
                follower['brand_id'] = brand_id_formal
                follower['master_page'] = master_page_id
                followed_by_list.append(follower.copy())
            db_connection_formal.inst_followedby_comp.insert(followed_by_list)
        else:
            pass
        if len(followed_by_response['pagination'])!=0:
            counter+=1
            next_url = followed_by_response['pagination']['next_url']
        else:
            next_url = ''
    return True
def dailyReachCalculator(db_connection_formal,inst_user_id_formal,brand_id_formal, master_page_id):
    reach_response = db_connection_formal.inst_media_comp.aggregate(
       [{"$match":{"user_id":inst_user_id_formal}},
          {
            "$group" : {
               "_id" : { "month": { "$month": "$created_time" }, "day": { "$dayOfMonth": "$created_time" }, "year": { "$year": "$created_time" } },
               "reach":{"$sum":{"$add":["$comments_count", "$likes_count"]}}
            }
          }
       ]
    )
    foll_res = db_connection_formal.inst_user_comp.find({'ins_id':inst_user_id_formal}, {'counts.followed_by':1, '_id':0})
    for foll in foll_res:
        follower = foll['counts']['followed_by']
    reach_json={
        "date":"",
        "user_id":"",
        "reach":"",
        "brand_id" : brand_id_formal,
        "master_page" : master_page_id
    }
    reach_list = []
    for result in reach_response['result']:
        dateStr = str(result["_id"]["year"])+"-"+str(result["_id"]["month"])+"-"+str(result["_id"]["day"])+"T10:30:01+0000"
        reach_json['date'] = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S+0000")
        reach_json["reach"] = int((result["reach"]/float(follower))*100)
        reach_json["user_id"] = inst_user_id_formal
        reach_list.append(reach_json.copy())
    db_connection_formal.inst_daily_reach_comp.insert(reach_list)

def gender_features(word):
    return {'last_letter': word[-1]}

def genderClassifier(db_connection_formal,inst_user_id_formal):
    labeled_names = ([(name, 'male') for name in names.words('male.txt')] + [(name, 'female') for name in names.words('female.txt')])
    random.shuffle(labeled_names)
    featuresets = [(gender_features(n), gender) for (n, gender) in labeled_names]
    classifier = nltk.NaiveBayesClassifier.train(featuresets)
    follower_response = db_connection_formal.inst_followedby_comp.find({'follower_of':inst_user_id_formal, 'gender':''}, {'full_name':1, '_id':1})
    for follower in follower_response:
        try:
            first_name = follower['full_name'].split(' ')
            gender = classifier.classify(gender_features(first_name[0]))
        except:
            gender = 'male'
        db_connection_formal.inst_followedby_comp.update({'_id':ObjectId(follower['_id'])}, {'$set':{'gender':gender}})

