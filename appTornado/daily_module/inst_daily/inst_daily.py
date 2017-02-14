__author__ = 'sanky_cse'
from nltk.corpus import names
import random
from bson.objectid import ObjectId
import nltk
import time
import datetime
def userFetch(inst_user_id_formal, db_connection_formal, access_token_formal, urllib_formal, json_formal):
    print inst_user_id_formal, "====", access_token_formal
    user = urllib_formal.urlopen('https://api.instagram.com/v1/users/'+inst_user_id_formal+'/?access_token='+access_token_formal)
    user_response = json_formal.loads(user.read())
    print user_response
    user_json = user_response['data']
    user_json['ins_id'] = user_json.pop('id')
    db_connection_formal.inst_user.remove({'ins_id':str(inst_user_id_formal)})
    db_connection_formal.inst_user.insert(user_json)
    user_json['date']=datetime.datetime.now()
    db_connection_formal.inst_user_daily.insert(user_json)
    return True
def recentMediaFetch(inst_user_id_formal, db_connection_formal, access_token_formal, urllib_formal, json_formal):
    min_id_response = db_connection_formal.inst_media.find({'user_id':str(inst_user_id_formal)}, {'media_id':1, '_id':0}).sort([('created_time',-1)]).limit(1)
    for min in min_id_response:
        min_id = min['media_id']
    next_url = 'https://api.instagram.com/v1/users/'+inst_user_id_formal+'/media/recent/?access_token='+access_token_formal+'&min_id='+min_id
    while next_url!='':
        recent_media =urllib_formal.urlopen(next_url)
        recent_media_response = json_formal.loads(recent_media.read())
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
              "likes_crawled":'no'
            }
            recent_media_list = []
            for media in recent_media_response['data']:
                recent_media_json['media_id']= media['id']
                recent_media_json['user_id'] = media['user']['id']
                recent_media_json['caption'] = media['caption']['text']
                recent_media_json['users_in_photo'] = media['users_in_photo']
                recent_media_json['thumbnail'] = media['images']['thumbnail']['url']
                recent_media_json['low_resolution'] = media['images']['low_resolution']['url']
                recent_media_json['standard_resolution'] = media['images']['standard_resolution']['url']
                recent_media_json['likes_count'] = media['likes']['count']
                recent_media_json['link'] = media['link']
                date_unformated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(media['created_time'])))
                recent_media_json['created_time'] = datetime.datetime.strptime(date_unformated ,"%Y-%m-%dT%H:%M:%SZ")
                recent_media_json['filter'] = media['filter']
                recent_media_json['comments_count'] = media['comments']['count']
                recent_media_json['location'] = media['location']
                recent_media_json['type'] = media['type']
                recent_media_json["tags"] = media['tags']
                if str(media['type'])=='video':
                    recent_media_json['video_url_standard'] = media['videos']['standard_resolution']['url']
                    recent_media_json['video_url_low'] = media['videos']['low_resolution']['url']
                else:
                    recent_media_json['video_url_standard'] = ''
                    recent_media_json['video_url_low'] = ''
                if media['id']!=min_id:
                    recent_media_list.append(recent_media_json.copy())
            if len(recent_media_list)>0:
                db_connection_formal.inst_media.insert(recent_media_list)
        else:
            pass
        pagination = recent_media_response['pagination']
        if len(pagination)!=0:
            next_url = pagination['next_url']
        else:
            next_url = ''
    return True
def recentMediaUpdate(inst_user_id_formal, db_connection_formal, access_token_formal, urllib_formal, json_formal):
    min_id=max_id=''
    time_range = datetime.datetime.now()-datetime.timedelta(days=5)
    max_id_response = db_connection_formal.inst_media.find({'user_id':str(inst_user_id_formal), 'created_time':{'$gt':time_range}}, {'media_id':1, '_id':0}).sort([('created_time',-1)]).limit(1)
    for max in max_id_response:
        max_id = max['media_id']
    min_id_response = db_connection_formal.inst_media.find({'user_id':str(inst_user_id_formal), 'created_time':{'$gt':time_range}}, {'media_id':1, '_id':0}).sort([('created_time',1)]).limit(1)
    for min in min_id_response:
        min_id = min['media_id']
    if min_id !='' and max_id!='':
        next_url = 'https://api.instagram.com/v1/users/'+inst_user_id_formal+'/media/recent/?access_token='+access_token_formal+'&min_id='+min_id+'&max_id='+max_id
        while next_url!='':
            recent_media =urllib_formal.urlopen(next_url)
            recent_media_response = json_formal.loads(recent_media.read())
            if len(recent_media_response['data'])!=0:
                for media in recent_media_response['data']:
                    cmnt_count = media['comments']['count']
                    likes_count = media['likes']['count']
                    db_response = db_connection_formal.inst_media.find({'user_id':str(inst_user_id_formal), "media_id" :media['id']}, {'likes_count':1, 'comments_count':1, '_id':0})
                    for db_res in db_response:
                        cmnt = db_res['comments_count']
                        likes = db_res['likes_count']
                    comment_crawled = 'yes'
                    likes_crawled = 'yes'
                    if cmnt_count!=cmnt:
                        comment_crawled = 'no'
                    if likes_count!=likes:
                        likes_crawled = 'no'
                    db_connection_formal.inst_media.update({'user_id':str(inst_user_id_formal), "media_id" :media['id']}, {'$set':{'comments_crawled':comment_crawled, 'likes_crawled':likes_crawled}})
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
def commentFetch(inst_user_id_formal, db_connection_formal, access_token_formal, urllib_formal, json_formal):
    db_res = db_connection_formal.inst_media.find({'user_id':str(inst_user_id_formal), 'comments_crawled':'no'}, {'media_id':1, '_id':0})
    for ids in db_res:
        comnt_list_res = db_connection_formal.inst_comments.find({'user_id':inst_user_id_formal,'media_id':ids['media_id']}, {'comment_id':1, '_id':0})
        comment_list_existing = []
        for comnt_list in comnt_list_res:
            comment_list_existing.append(comnt_list['comment_id'])
        comment_response = urllib_formal.urlopen('https://api.instagram.com/v1/media/'+ids['media_id']+'/comments?access_token='+access_token_formal)
        comment_response_json = json_formal.loads(comment_response.read())
        if len(comment_response_json['data'])!=0:
            comment_list = []
            for comments in comment_response_json['data']:
                comments['comment_id'] = comments.pop('id')
                comments['media_id'] = ids['media_id']
                comments['user_id'] = inst_user_id_formal
                comments['sentiment']= ''
                comments['keywords'] = ''
                date_unformated = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(comments['created_time'])))
                comments['created_time'] = datetime.datetime.strptime(date_unformated ,"%Y-%m-%dT%H:%M:%SZ")
                comments['comments_crawled']='yes'
                if comments['comment_id'] not in comment_list:
                    comment_list.append(comments.copy())
            db_connection_formal.inst_comments.insert(comment_list)
        else:
            pass
        db_connection_formal.inst_media.update({'user_id':str(inst_user_id_formal), "media_id" :media['id']}, {'$set':{'comments_crawled':'yes'}})
    return True
def likeFetch(inst_user_id_formal, db_connection_formal, access_token_formal, urllib_formal, json_formal):
    db_res = db_connection_formal.inst_media.find({'user_id':str(inst_user_id_formal), 'likes_crawled':'no'}, {'media_id':1, '_id':0})
    for ids in db_res:
        like_list_res=db_connection_formal.inst_media.find({'user_id':inst_user_id_formal,'media_id':ids['media_id']}, {'likers_id':1, '_id':0})
        like_list=[]
        for likers in like_list_res:
            like_list.append(likers['likers_id'])
        like_response = urllib_formal.urlopen('https://api.instagram.com/v1/media/'+ids['media_id']+'/likes?access_token='+access_token_formal)
        like_response_json = json_formal.loads(like_response.read())
        if len(like_response_json['data'])!=0:
            like_list = []
            for likes in like_response_json['data']:
                likes['likers_id'] = likes.pop('id')
                likes['media_id'] = ids['media_id']
                likes['user_id'] = inst_user_id_formal
                if likes['likers_id'] not in like_list:
                    like_list.append(likes.copy())
            db_connection_formal.inst_likes.insert(like_list)
        else:
            pass
        db_connection_formal.inst_media.update({'user_id':str(inst_user_id_formal), "media_id" :media['id']}, {'$set':{'likes_crawled':'yes'}})
    return  True
def dailyReachCalculator(inst_user_id_formal, db_connection_formal):
    max_date = db_connection_formal.inst_daily_reach.find({'user_id':inst_user_id_formal}, {'date':1, '_id':0}).sort([('date',-1)]).limit(1)
    for max in max_date:
        till_date = max['date']
    reach_response = db_connection_formal.inst_media.aggregate(
       [{"$match":{"user_id":inst_user_id_formal, 'date':{'$gt':till_date}}},
          {
            "$group" : {
               "_id" : { "month": { "$month": "$created_time" }, "day": { "$dayOfMonth": "$created_time" }, "year": { "$year": "$created_time" } },
               "reach":{"$sum":{"$add":["$comments_count", "$likes_count"]}}
            }
          }
       ]
    )
    foll_res = db_connection_formal.inst_user.find({'ins_id':inst_user_id_formal}, {'counts.followed_by':1, '_id':0})
    for foll in foll_res:
        follower = foll['counts']['followed_by']
    reach_json={
        "date":"",
        "user_id":"",
        "reach":""
    }
    reach_list = []
    for result in reach_response['result']:
        dateStr = str(result["_id"]["year"])+"-"+str(result["_id"]["month"])+"-"+str(result["_id"]["day"])+"T10:30:01+0000"
        reach_json['date'] = datetime.datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S+0000")
        reach_json["reach"] = int((result["reach"]/float(follower))*100)
        reach_json["user_id"] = inst_user_id_formal
        reach_list.append(reach_json.copy())
    if len(reach_list)>0:
        db_connection_formal.inst_daily_reach.insert(reach_list)
def gender_features(word):
    return {'last_letter': word[-1]}
def genderClassifier(inst_user_id_formal, db_connection_formal):
    labeled_names = ([(name, 'male') for name in names.words('male.txt')] + [(name, 'female') for name in names.words('female.txt')])
    random.shuffle(labeled_names)
    featuresets = [(gender_features(n), gender) for (n, gender) in labeled_names]
    classifier = nltk.NaiveBayesClassifier.train(featuresets)
    follower_response = db_connection_formal.inst_followedby.find({'follower_of':inst_user_id_formal, 'gender':''}, {'full_name':1, '_id':1})
    for follower in follower_response:
        try:
            first_name = follower['full_name'].split(' ')
            gender = classifier.classify(gender_features(first_name[0]))
        except:
            gender = 'male'
        db_connection_formal.inst_followedby.update({'_id':ObjectId(follower['_id'])}, {'$set':{'gender':gender}})