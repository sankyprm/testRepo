from datetime import datetime
import datetime as datetime2
import sys
from nltk.corpus import names
import random
import nltk

def userInfo(tw_user_id_formal, tw_handle_formal, db_connection_formal):
    #try:
    user_info_response = tw_handle_formal.show_user(user_id =tw_user_id_formal)
    user_info_response['user_id']=str(user_info_response.pop('id_str'))
    db_connection_formal.tw_users.remove({'user_id':str(tw_user_id_formal)})
    db_connection_formal.tw_users.insert(user_info_response)
    user_info_response['date']=datetime.now()
    db_connection_formal.tw_users_daily.insert(user_info_response)
    #except:
        #print "raise error"
    #return True
def userMentionTimeline(tw_user_id_formal, tw_handle_formal, db_connection_formal):
    max_id=''
    max_id_response = db_connection_formal.tw_mentions.find({'user_id':tw_user_id_formal}, {'mention_id':1, '_id':0}).sort([('created_at',-1)]).limit(1)
    for id in max_id_response:
        max_id = int(id['mention_id'])
    #try:
    if max_id != '':
        mention_response = tw_handle_formal.get_mentions_timeline(count=200, include_rts=1, since_id=max_id+1)
    else:
        mention_response = tw_handle_formal.get_mentions_timeline(count=200, include_rts=1)
    while len(mention_response)>0:
        mention_json = {
        "created_at": "",
        "text": "",
        "source": "",
        "in_reply_to_status_id":"",
        "user_id": "",
        "user_name": "",
        "user": {
          "id_str": "",
          "name": "",
          "screen_name": "",
          "location": "",
          "description": "",
          "followers_count": 0,
          "friends_count": 0,
          "listed_count": 0,
          "favourites_count": 0,
          "statuses_count": 0,
          "lang": "",
          "profile_image_url": "",
          },
        "geo": '',
        "coordinates": '',
        "place":'' ,
        "retweet_count": 0,
        "favorite_count": 0,
        "hashtags": [],
        "symbols": [],
        "urls": [],
        "sentiment": '',
        "keywords":[]
        }
        mentions_list = []
        import pprint
        pprint.pprint("====================================")
        pprint.pprint(mention_response)
        pprint.pprint("====================================")

        for mention in mention_response:
            import json

            from bson.objectid import ObjectId
            mention_json['_id']=ObjectId()
            mention_json['created_at']= datetime.strptime(mention['created_at'],"%a %b %d %H:%M:%S +0000 %Y")
            mention_json['mention_id']= mention['id_str']
            mention_json['text']= mention['text']
            mention_json['source']= mention['source']
            mention_json['in_reply_to_status_id'] = mention['in_reply_to_status_id_str']
            mention_json['user_id']= tw_user_id_formal
            mention_json['user_name']= mention['in_reply_to_screen_name']
            mention_json['user']['id_str']= mention['user']['id_str']
            mention_json['user']['name']= mention['user']['name']
            mention_json['user']['screen_name'] = mention['user']['screen_name']
            mention_json['user']['location'] = mention['user']['screen_name']
            mention_json['user']['description'] = mention['user']['description']
            mention_json['user']['followers_count'] = mention['user']['followers_count']
            mention_json['user']['friends_count'] = mention['user']['friends_count']
            mention_json['user']['listed_count'] = mention['user']['listed_count']
            mention_json['user']['favourites_count'] = mention['user']['favourites_count']
            mention_json['user']['statuses_count'] = mention['user']['statuses_count']
            mention_json['user']['lang'] =  mention['user']['lang']
            mention_json['user']['profile_image_url'] = mention['user']['profile_image_url']
            mention_json['geo'] = mention['geo']
            mention_json['coordinates'] = mention['coordinates']
            mention_json['place']= mention['place']
            mention_json['retweet_count'] = mention['retweet_count']
            mention_json['favorite_count'] = mention['favorite_count']
            mention_json['hashtags'] = mention['entities']['hashtags']
            mention_json['symbols']= mention['entities']['symbols']
            mention_json['urls'] = mention['entities']['urls']
            db_tw_mention = db_connection_formal.tw_mentions.insert(mention_json)
            #mentions_list.append(mention_json)
            if int(mention['id_str']) > max_id:
                max_id = int(mention['id_str'])
                print mention['id_str'], "\n"
        pprint.pprint(mentions_list)
        print max_id, "===",
        mention_response = tw_handle_formal.get_mentions_timeline(count=200, include_rts=1, since_id=(int(max_id)+1))
    #except:
        #print "raise error"
    return 'success mention'
def userTimeline(tw_user_id_formal, tw_handle_formal, db_connection_formal):
    max_id=''
    max_id_response = db_connection_formal.tw_tweets.find({'user_id':tw_user_id_formal}, {'tw_id':1, '_id':0}).sort([('created_at',-1)]).limit(1)
    for id in max_id_response:
        max_id = int(id['tw_id'])
    #try:
    if max_id !='':
        timeline_response = tw_handle_formal.get_user_timeline(count=200, include_rts=1, since_id=(max_id+1))
    else:
        timeline_response = tw_handle_formal.get_user_timeline(count=200, include_rts=1)
    while len(timeline_response)>0:
        tweet_json = {
            "created_at": "",
            "rt_crawled":'no',
            "favorited":'',
            "text": "",
            "source": "",
            "user_id": '',
            "user_name": '',
            "geo": '',
            "coordinates": '',
            "place": '',
            "contributors": '',
            "retweet_count": 0,
            "favorite_count": 0,
            "hashtags": [],
            "symbols": [],
            "urls": [],
            "user_mentions": [],
            "sentiment":'',
            "lang": ""
        }
        tweet_list = []
        for tweet in timeline_response:
            tweet_json['created_at']= datetime.strptime(tweet['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
            tweet_json['tw_id'] = tweet['id_str']
            tweet_json['text'] = tweet['text']
            tweet_json['favorited'] = tweet['favorited']
            tweet_json['source'] = tweet['source']
            tweet_json['user_id'] = tweet['user']['id_str']
            tweet_json['user_name'] = tweet['user']['name']
            tweet_json['geo'] = tweet['geo']
            tweet_json['coordinates'] = tweet['coordinates']
            tweet_json["place"] = tweet['place']
            tweet_json['contributors'] = tweet['contributors']
            tweet_json['retweet_count'] = tweet['retweet_count']
            tweet_json['favorite_count'] = tweet['favorite_count']
            tweet_json['hashtags'] = tweet['entities']['hashtags']
            tweet_json['symbols'] = tweet['entities']['symbols']
            tweet_json['urls'] = tweet['entities']['urls']
            tweet_json['user_mentions'] = tweet['entities']['user_mentions']
            tweet_json['lang'] = tweet['lang']
            tweet_list.append(tweet_json.copy())
            if max_id<int(tweet['id_str']):
                max_id = int(tweet['id_str'])
        db_tweets = db_connection_formal.tw_tweets.insert(tweet_list)
        print tweet_list
        timeline_response = tw_handle_formal.get_user_timeline(count=200, include_rts=1, since_id=(int(max_id)+1))
    #except:
        #print "raise error"
    return 'success tweet insertion'
def timelineUpdate(tw_user_id_formal, tw_handle_formal, db_connection_formal):
    min_id=max_id=''
    time_range = datetime.now()-datetime2.timedelta(days=31)
    max_id_response = db_connection_formal.tw_tweets.find({'user_id':str(tw_user_id_formal), 'created_at':{'$gt':time_range}}, {'tw_id':1, '_id':0}).sort([('created_at',-1)]).limit(1)
    for id in max_id_response:
        max_id = int(id['tw_id'])
    min_id_response = db_connection_formal.tw_tweets.find({'user_id':str(tw_user_id_formal), 'created_at':{'$gt':time_range}}, {'tw_id':1, '_id':0}).sort([('created_at',1)]).limit(1)
    for id_min in min_id_response:
        print min_id_response
        min_id = int(id_min['tw_id'])
    #try:
    if min_id!='' and max_id!='':
        timeline_response = tw_handle_formal.get_user_timeline(count=200, include_rts=1, since_id=(int(min_id)+1), max_id=(int(max_id)-1))
        while len(timeline_response)>0:
            for tweet in timeline_response:
                db_connection_formal.tw_tweets.update({'user_id':tw_user_id_formal, 'tw_id':tweet['id_str']}, {'$set':{'retweet_count':tweet['retweet_count'], 'favorite_count':tweet['favorite_count'], 'user_mentions':tweet['entities']['user_mentions'], 'favorited':tweet['favorited'], 'hashtags':tweet['entities']['hashtags']}})
                if min_id<int(tweet['id_str']):
                    min_id = int(tweet['id_str'])
                rt_count_response = db_connection_formal.tw_tweets.find({'user_id':tw_user_id_formal, 'tw_id':tweet['id_str']}, {'retweet_count':1, '_id':0})
                for rt in rt_count_response:
                    rt_count = rt['retweet_count']
                if rt_count!=tweet['retweet_count']:
                    db_connection_formal.tw_tweets.update({'user_id':tw_user_id_formal, 'tw_id':tweet['id_str']}, {'$set':{'rt_crawled':'no'}})
            timeline_response = tw_handle_formal.get_user_timeline(count=200, include_rts=1, since_id=(int(min_id)+1), max_id=(int(max_id)-1))
    #except:
        #print "raise error"
    return 'success tweet insertion'
def retweetsFetch(tw_user_id_formal, tw_handle_formal, db_connection_formal):
    db_tw_tweets = db_connection_formal.tw_tweets
    tweet_from_db = db_tw_tweets.find({"user_id": tw_user_id_formal, "retweet_count": {"$gt":0}, "rt_crawled":'no'}, {'tw_id':1})
    db_tw_retweets = db_connection_formal.tw_retweets
    for tweets in tweet_from_db:
        print "\n================problem is here======================\n", tweets
        #try:
        rt_existing = db_connection_formal.tw_retweets.find({'tw_id':tweets['tw_id']}, {'rt_id':1, '_id':0})
        rt_existing_list = []
        for rt in rt_existing:
            rt_existing_list.append(rt['rt_id'])
        retweet_response = tw_handle_formal.get_retweets(id=tweets['tw_id'])
        print retweet_response, "===================\n"
        if len(retweet_response)>0:
            retweet_json = {
                "user_id":'',
                "created_at": "",
                "source": "",
                "user": {
                  "id": "",
                  "name": "",
                  "location": "",
                  "description": "",
                  "followers_count": 0,
                  "friends_count": 0,
                  "listed_count": 0,
                  "favourites_count": 0,
                  "verified": "",
                  "statuses_count": 0,
                  "lang": "",
                  "profile_image_url": "",
                  "following": "",
                  "follow_request_sent": "",
                },
                "geo": "",
                "coordinates": "",
                "place": "",
            }
            retweet_list = []
            for retweet in retweet_response:
                retweet_json['created_at'] = retweet['created_at']
                retweet_json['rt_id'] = retweet['id_str']
                retweet_json['source'] = retweet['source']
                retweet_json['user']['id'] = retweet['user']['id_str']
                retweet_json['user']['name'] = retweet['user']['name']
                retweet_json['user']['location'] = retweet['user']['location']
                retweet_json['user']['description'] = retweet['user']['description']
                retweet_json['user']['followers_count'] = retweet['user']['followers_count']
                retweet_json['user']['friends_count'] = retweet['user']['friends_count']
                retweet_json['user']['listed_count'] = retweet['user']['listed_count']
                retweet_json['user']['favourites_count'] = retweet['user']['favourites_count']
                retweet_json['user']['verified'] = retweet['user']['verified']
                retweet_json['user']['statuses_count'] = retweet['user']['statuses_count']
                retweet_json['user']['lang'] = retweet['user']['lang']
                retweet_json['user']['profile_image_url'] = retweet['user']['profile_image_url']
                retweet_json['user']['following'] = retweet['user']['following']
                retweet_json['user']['follow_request_sent'] = retweet['user']['follow_request_sent']
                retweet_json['geo'] = retweet['geo']
                retweet_json['coordinates'] = retweet['coordinates']
                retweet_json['place'] = retweet['place']
                retweet_json['tw_id']=tweets['tw_id']
                retweet_json['user_id'] = tw_user_id_formal
                if retweet['id_str'] not in rt_existing_list:
                    retweet_list.append(retweet_json.copy())
            db_tw_retweets.insert(retweet_list)
        else:
            pass
        db_tw_tweets.update({"user_id": tw_user_id_formal, 'tw_id':tweets['tw_id']}, {'$set':{'rt_crawled':'yes'}})
        #except:
            #print "raise error"
    return 'successfully inserted retweets'
def followersFetch(tw_user_id_formal, tw_handle_formal, db_connection_formal):
    #try:
    followersIds_response = tw_handle_formal.get_followers_ids(id=tw_user_id_formal, count=5000)
    print followersIds_response
    follwersIds_json = {
        "id_list":[]
    }
    next_cursor = 1
    while next_cursor!=0:
        follwersIds_json['user_id']= tw_user_id_formal
        follwersIds_json['id_list']=followersIds_response['ids']
        check_exist = db_connection_formal.tw_follower_list.find({"user_id":str(tw_user_id_formal)}).count()
        if check_exist != 0:
            db_connection_formal.tw_follower_list.update({"user_id": str(tw_user_id_formal)}, {"$addToSet":{"id_list": {"$each": follwersIds_json['id_list']}}})
            print 'update\n', follwersIds_json
        else:
            db_connection_formal.tw_follower_list.insert(follwersIds_json)
            print 'insert', follwersIds_json
        next_cursor = followersIds_response['next_cursor']
        followersIds_response = tw_handle_formal.get_followers_ids(id=tw_user_id_formal, count=5000, cursor=next_cursor)
    #except:
        #print "raise error"
    return "success follwers fetch"
def followerDetails(tw_user_id_formal, tw_handle_formal, db_connection_formal):
    fans_existing_final = []
    fan_list_final = []
    fan_list_response = db_connection_formal.tw_follower_list.find({'user_id':tw_user_id_formal}, {'_id':0, 'id_list':1})
    for fan_list in fan_list_response:
        fan_list_final = fan_list['id_list']
    existing_fans_response = db_connection_formal.tw_followers.find({'celeb':tw_user_id_formal}, {'user_id':1})
    for fans_existing in existing_fans_response:
        fans_existing_final.append(fans_existing['user_id'])
    index = 0
    fan_list_final_new_only = list(set(fan_list_final).difference(set(fans_existing_final)))
    while index<len(fan_list_final_new_only):
        fanString = ",".join(map(str, fan_list_final[index:index+100]) )
        #try:
        fan_response = tw_handle_formal.lookup_user(user_id=fanString)
        fanfor_db_insert = []
        for eachfan in fan_response:
            eachfan['user_id'] = eachfan.pop('id_str')
            eachfan['celeb'] = tw_user_id_formal
            eachfan['gender'] = ''
            fanfor_db_insert.append(eachfan.copy())
        db_connection_formal.tw_followers.insert(fanfor_db_insert)
        index = index+100
        #except:
            #print "raise error"
def dailyReachCalculator(tw_user_id_formal, db_connection_formal):
    try:
        max_date_res = db_connection_formal.tw_daily_reach.find({"user_id":tw_user_id_formal}, {'date':1, '_id':0}).sort([('date',-1)]).limit(1)
        for dateObj in max_date_res:
            maxDate = dateObj['date']
        reach_response = db_connection_formal.tw_tweets.aggregate(
           [{"$match":{"user_id":tw_user_id_formal, 'created_at':{'$gt':maxDate}}},
              {
                "$group" : {
                   "_id" : { "month": { "$month": "$created_at" }, "day": { "$dayOfMonth": "$created_at" }, "year": { "$year": "$created_at" } },
                   "reach":{"$sum":{"$add":["$favorite_count", "$retweet_count"]}}
                }
              }
           ]
        )
        reach_json={
            "date":"",
            "user_id":"",
            "reach":""
        }
        reach_list = []
        for result in reach_response['result']:
            dateStr = str(result["_id"]["year"])+"-"+str(result["_id"]["month"])+"-"+str(result["_id"]["day"])+"T10:30:01+0000"
            reach_json['date'] = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S+0000")
            reach_json["reach"] = result["reach"]
            reach_json["user_id"] = tw_user_id_formal
            reach_list.append(reach_json.copy())
        if len(reach_list)>0:
            db_connection_formal.tw_daily_reach.insert(reach_list)
    except:
        reach_json={
            "date":datetime.now(),
            "user_id":tw_user_id_formal,
            "reach":0
        }
        db_connection_formal.tw_daily_reach.insert(reach_json)
def gender_features(word):
    return {'last_letter': word[-1]}
def genderClassifier(tw_user_id_formal, db_connection_formal):
    labeled_names = ([(name, 'male') for name in names.words('male.txt')] + [(name, 'female') for name in names.words('female.txt')])
    random.shuffle(labeled_names)
    featuresets = [(gender_features(n), gender) for (n, gender) in labeled_names]
    classifier = nltk.NaiveBayesClassifier.train(featuresets)
    follower_response = db_connection_formal.tw_followers.find({'celeb':tw_user_id_formal, 'gender':''}, {'name':1, 'user_id':1})
    for follower in follower_response:
        gender = classifier.classify(gender_features(follower['name']))
        db_connection_formal.tw_followers.update({'user_id':follower['user_id']}, {'$set':{'gender':gender}})



