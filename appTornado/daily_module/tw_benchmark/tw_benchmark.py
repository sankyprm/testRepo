__author__ = 'soumik'
from datetime import datetime
import sys
from nltk.corpus import names
import random
import nltk
import datetime as datetime2

def userTimeline(db_connection_formal,tw_user_id_formal, tw_handle_formal, brand_id_formal, master_page_id):
    try:
        timeline_response = tw_handle_formal.get_user_timeline(user_id=tw_user_id_formal,count=200, include_rts=1,)
        print timeline_response
        min_id = 9999999999999999999
        counter = 0
        while counter < 7 and len(timeline_response)!=0:
            tweet_json = {
                "brand_id":brand_id_formal,
                "master_page":master_page_id,
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
                "lang": "",
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
                if min_id>int(tweet['id_str']):
                    min_id = int(tweet['id_str'])
            print 'min_id===========>>',min_id
            db_connection_formal.tw_tweets_comp.insert(tweet_list)
            counter += 1
            timeline_response = tw_handle_formal.get_user_timeline(user_id=tw_user_id_formal,count=200, include_rts=1, max_id=(min_id-1))
            print timeline_response
    except:
        print "raise error"
    return 'success tweet insertion'

def timelineUpdate( db_connection_formal,tw_user_id_formal, tw_handle_formal, brand_id_formal, master_page_id):
    min_id=max_id=''
    time_range = datetime.now()-datetime2.timedelta(days=31)
    max_id_response = db_connection_formal.tw_tweets_comp.find({'user_id':str(tw_user_id_formal), 'master_page':str(master_page_id),'created_at':{'$gt':time_range}}, {'tw_id':1, '_id':0}).sort([('created_at',-1)]).limit(1)
    for id in max_id_response:
        max_id = int(id['tw_id'])
    min_id_response = db_connection_formal.tw_tweets_comp.find({'user_id':str(tw_user_id_formal), 'master_page':str(master_page_id), 'created_at':{'$gt':time_range}}, {'tw_id':1, '_id':0}).sort([('created_at',1)]).limit(1)
    for id_min in min_id_response:
        print min_id_response
        min_id = int(id_min['tw_id'])
    try:
        if min_id!='' and max_id!='':
            timeline_response = tw_handle_formal.get_user_timeline(count=200, include_rts=1, since_id=(min_id+1), max_id=(max_id-1))
            while len(timeline_response)>0:
                for tweet in timeline_response:
                    db_connection_formal.tw_tweets_comp.update({'user_id':tw_user_id_formal, 'tw_id':tweet['id_str']}, {'$set':{'retweet_count':tweet['retweet_count'], 'favorite_count':tweet['favorite_count'], 'user_mentions':tweet['entities']['user_mentions'], 'favorited':tweet['favorited'], 'hashtags':tweet['entities']['hashtags']}})
                    if min_id<int(tweet['id_str']):
                        min_id = int(tweet['id_str'])
                    rt_count_response = db_connection_formal.tw_tweets_comp.find({'user_id':tw_user_id_formal, 'tw_id':tweet['id_str']}, {'retweet_count':1, '_id':0})
                    for rt in rt_count_response:
                        rt_count = rt['retweet_count']
                    if rt_count!=tweet['retweet_count']:
                        db_connection_formal.tw_tweets_comp.update({'user_id':tw_user_id_formal, 'tw_id':tweet['id_str']}, {'$set':{'rt_crawled':'no'}})
                timeline_response = tw_handle_formal.get_user_timeline(count=200, include_rts=1, since_id=(min_id+1), max_id=(max_id-1))
    except:
        print "raise error"
    return 'success tweet insertion'
def followersFetch(db_connection_formal, tw_user_id_formal, tw_handle_formal, brand_id_formal, master_page_id):
    try:
        follwersIds_json = {}
        followersIds_response = tw_handle_formal.get_followers_ids(id=tw_user_id_formal, count=5000)
        follwersIds_json['user_id']= tw_user_id_formal
        follwersIds_json['id_list']=followersIds_response['ids']
        follwersIds_json['brand_id']=brand_id_formal
        follwersIds_json['master_page'] = master_page_id
        check_exist = db_connection_formal.tw_follower_list_comp.find({"user_id":str(tw_user_id_formal)}).count()
        if check_exist != 0:
            db_connection_formal.tw_follower_list.update({"user_id": str(tw_user_id_formal)}, {"$addToSet":{"id_list": {"$each": follwersIds_json['id_list']}}})
            #print 'update\n', follwersIds_json
        else:
            db_connection_formal.tw_follower_list_comp.insert(follwersIds_json)
            #print 'insert', follwersIds_json
    except:
        print "raise error"
    return "success follwers fetch"

def userInfo(db_connection_formal, tw_user_id_formal, tw_handle_formal, brand_id_formal, master_page_id):
    try:
        user_info_response = tw_handle_formal.show_user(user_id =tw_user_id_formal)
        user_info_response['user_id']=str(user_info_response.pop('id_str'))
        user_info_response['brand_id']=brand_id_formal
        user_info_response['master_page'] = master_page_id
        user_info_response['fetch_completed']='incomplete'
        db_connection_formal.tw_users_comp.insert(user_info_response)
    except:
        print "raise error"
    return True

def followerDetails( db_connection_formal, tw_user_id_formal, tw_handle_formal, brand_id_formal, master_page_id):
    fans_existing_final = []
    fan_list_final = []
    fan_list_response = db_connection_formal.tw_follower_list_comp.find({'user_id':tw_user_id_formal}, {'_id':0, 'id_list':1})
    for fan_list in fan_list_response:
        fan_list_final.extend(fan_list['id_list'])
    existing_fans_response = db_connection_formal.tw_followers_comp.find({'celeb':tw_user_id_formal}, {'user_id':1})
    for fans_existing in existing_fans_response:
        fans_existing_final.append(fans_existing['user_id'])
    index = 0
    fan_list_final_new_only = list(set(fan_list_final).difference(set(fans_existing_final)))
    while index<len(fan_list_final_new_only):
        fanString = ",".join(map(str, fan_list_final[index:index+100]) )
        try:
            fan_response = tw_handle_formal.lookup_user(user_id=fanString)
            fanfor_db_insert = []
            for eachfan in fan_response:
                eachfan['user_id'] = eachfan.pop('id_str')
                eachfan['celeb'] = tw_user_id_formal
                eachfan['gender'] = ''
                eachfan['brand_id'] = brand_id_formal
                eachfan['master_page'] = master_page_id
                fanfor_db_insert.append(eachfan.copy())
            db_connection_formal.tw_followers_comp.insert(fanfor_db_insert)
            index = index+100
        except:
            print "raise error"

def dailyReachCalculator( db_connection_formal,tw_user_id_formal, brand_id_formal, master_page_id):
    reach_response = db_connection_formal.tw_tweets_comp.aggregate(
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
        "reach":"",
        "brand_id" : brand_id_formal,
        "master_page" : master_page_id
    }
    reach_list = []
    for result in reach_response['result']:
        dateStr = str(result["_id"]["year"])+"-"+str(result["_id"]["month"])+"-"+str(result["_id"]["day"])+"T10:30:01+0000"
        reach_json['date'] = datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S+0000")
        reach_json["reach"] = result["reach"]
        reach_json["user_id"] = tw_user_id_formal
        reach_list.append(reach_json.copy())
    db_connection_formal.tw_daily_reach_comp.insert(reach_list)


def gender_features(word):
    return {'last_letter': word[-1]}

def genderClassifier( db_connection_formal,tw_user_id_formal):
    labeled_names = ([(name, 'male') for name in names.words('male.txt')] + [(name, 'female') for name in names.words('female.txt')])
    random.shuffle(labeled_names)
    featuresets = [(gender_features(n), gender) for (n, gender) in labeled_names]
    classifier = nltk.NaiveBayesClassifier.train(featuresets)
    follower_response = db_connection_formal.tw_followers_comp.find({'celeb':tw_user_id_formal, 'gender':''}, {'name':1, 'user_id':1})
    for follower in follower_response:
        gender = classifier.classify(gender_features(follower['name']))
        db_connection_formal.tw_followers_comp.update({'user_id':follower['user_id']}, {'$set':{'gender':gender}})