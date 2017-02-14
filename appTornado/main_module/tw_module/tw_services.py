__author__ = 'sibiaanalytics'
from datetime import datetime
import sys
from nltk.corpus import names
import random
import nltk
import urllib
import json

def userMentionTimeline(tw_user_id_formal, tw_handle_formal, db_connection_formal):
    try:
        mention_response = tw_handle_formal.get_mentions_timeline(count=200, include_rts=1)
        minId = 9999999999999999999
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
                if mention['id'] < minId:
                    minId = int(mention['id'])
                    print mention['id'], "\n"
            pprint.pprint(mentions_list)
            print minId, "===",
            mention_response = tw_handle_formal.get_mentions_timeline(count=200, include_rts=1, max_id=(minId-1))
    except:
        print "raise error"
    return 'success mention'
def userTimeline(tw_user_id_formal, tw_handle_formal, db_connection_formal):
    try:
        timeline_response = tw_handle_formal.get_user_timeline(count=200, include_rts=1)
        min_id = 9999999999999999999
        while len(timeline_response)>0:
            tweet_json = {
                "created_at": "",
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
                try:
                    tweet_json['media'] = tweet['entities']['media']
                except:
                    tweet_json['media'] = []
                tweet_json['lang'] = tweet['lang']
                tweet_list.append(tweet_json.copy())
                if min_id>int(tweet['id']):
                    min_id = int(tweet['id'])
            db_tweets = db_connection_formal.tw_tweets.insert(tweet_list)
            print tweet_list
            timeline_response = tw_handle_formal.get_user_timeline(count=200, include_rts=1, max_id=(min_id-1))
    except:
        print "raise error"
    return 'success tweet insertion'
def retweetsFetch(tw_user_id_formal, tw_handle_formal, db_connection_formal):
    db_tw_tweets = db_connection_formal.tw_tweets
    tweet_from_db = db_tw_tweets.find({"user_id": tw_user_id_formal, "retweet_count": {"$gt":0}}, {'tw_id':1})
    db_tw_retweets = db_connection_formal.tw_retweets
    for tweets in tweet_from_db:
        print "\n================problem is here in retweets ======================\n", tweets
        #try:
        retweet_response = tw_handle_formal.get_retweets(id=tweets['tw_id'])
        print retweet_response, "===================\n"
        if len(retweet_response)>0:
            retweet_json = {
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
                retweet_json['created_at'] = datetime.strptime(retweet['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
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
                retweet_list.append(retweet_json.copy())
            db_tw_retweets.insert(retweet_list)
        else:
            pass
        '''except:
            print "raise error" '''
    return 'successfully inserted retweets'
def trendsFetch(tw_user_id_formal, tw_handle_formal, db_connection_formal):
    print("tw_user_id_formal ====",tw_user_id_formal)
    print("tw_handle_formal ====",tw_handle_formal)
    print("db_connection_formal ====",db_connection_formal)
    db_tw_users = db_connection_formal.tw_users
    location_user = db_tw_users.find({"tw_id":tw_user_id_formal}, {"location":1, "_id":0})
    db_tw_woeid = db_connection_formal.tw_woeid
    for location in location_user:
        if location['location']=='':
            location_formated = str(location['location']).title()
        else:
            location_formated = ''
        woeid_location = db_tw_woeid.find({"name": location_formated}, {"woeid":1})
        #for woeid in woeid_location:
    trends_response = tw_handle_formal.get_place_trends(id=1)
    if len(trends_response)>0:
        db_connection_formal.tw_trends.insert(trends_response)
    else:
        db_connection_formal.tw_trends.insert(tw_handle_formal.get_place_trends(id=1))
def followersFetch(tw_user_id_formal, tw_handle_formal, db_connection_formal):
    try:
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
    except:
        print "raise error"
    return "success follwers fetch"
def userInfo(tw_user_id_formal, tw_handle_formal, db_connection_formal, brand_id):
    try:
        user_info_response = tw_handle_formal.show_user(user_id =tw_user_id_formal)
        user_info_response['user_id']=str(user_info_response.pop('id_str'))
        user_info_response['brand_id'] = [brand_id]
        db_connection_formal.tw_users.insert(user_info_response)
        user_info_response['date']=datetime.now()
        db_connection_formal.tw_users_daily.insert(user_info_response)
    except:
        print "raise error"
    return True
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
        print "fanstring======>>>>>>", fanString
        fan_response = tw_handle_formal.lookup_user(user_id=fanString)
        fanfor_db_insert = []
        for eachfan in fan_response:
            if eachfan['location'] is not None:
                eachfan['location'] = locationFetch(eachfan['location'])
            else:
                eachfan['location']='Not Defined'
            eachfan['user_id'] = eachfan.pop('id_str')
            eachfan['celeb'] = tw_user_id_formal
            eachfan['gender'] = ''
            fanfor_db_insert.append(eachfan.copy())
        db_connection_formal.tw_followers.insert(fanfor_db_insert)
        index = index+100
        #except:
        #    print "raise error"
def dailyReachCalculator(tw_user_id_formal, db_connection_formal):
    reach_response = db_connection_formal.tw_tweets.aggregate(
       [{"$match":{"user_id":tw_user_id_formal}},
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
    else:
        pass
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
def locationFetch(location_name):
    location='Not Defined'
    try:
        location_res = urllib.urlopen("http://maps.googleapis.com/maps/api/geocode/json?address="+location_name+"&sensor=false")
        location_res_formated = json.loads(location_res.read())
        for loc in location_res_formated['results']:
            for location_comp in loc["address_components"]:
                if "country" in location_comp['types']:
                    location = location_comp['long_name']
    except:
        pass
    return location














