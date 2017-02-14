import time
from urlparse import urlparse, parse_qs
import datetime
import urllib, json

def userAccountsInfo(db_connection_formal, account_id_formal, fb_handle_formal):
    #fb_handle = facebook.GraphAPI(access_token_formal)
    user_info = fb_handle_formal.get_object(account_id_formal)
    final_user_info = {
              "about": "",
              "bio": "",
              "category": "",
              "category_id":"",
              "checkins": 0,
              "cover": {},
              "is_community_page": '',
              "likes": 0,
              "link": "",
              "location": {
                "latitude": 0,
                "longitude": 0
              },
              "name": "",
              "personal_info": "",
              "personal_interests": "",
              "talking_about_count": 0,
              "username": "",
              "website": "",
              "were_here_count": 0,
              "total_photos":0,
              "total_album":0,
    }
    final_key_list = ['id', 'about', 'bio', 'category', 'checkins', 'cover', 'is_community_page', 'likes', 'new_like_count', 'location', 'name', 'personal_info', 'personal_interests', 'talking_about_count', 'username', 'website', 'were_here_count', 'total_photos', 'total_album']
    for key, value in user_info.iteritems():
        if key in final_key_list:
            if key == 'id':
                final_user_info['page_'+key]=str(value)
            else:
                final_user_info[key]=value
        else:
            continue
    db_connection_formal.fb_accounts.remove({'page_id':account_id_formal})
    db_connection_formal.fb_accounts.insert(final_user_info)
    final_user_info['date']=datetime.datetime.now()
    db_connection_formal.fb_accounts_daily.insert(final_user_info)
    #db_connection_formal.brands.update({'associated_accounts.fb_accounts.page_id':str(account_id_formal)}, {'$set':{'associated_accounts.fb_accounts.avatar':str(user_info['cover']['source'])}})
    return "account fetched successfully"
def pagePosts(account_id_formal, db_connection_formal, fb_handle_formal, access_token_formal):
    import json
    print account_id_formal
    result_date = db_connection_formal.fb_post_details.find({'page_id':str(account_id_formal)}, {'created_time':1, '_id':0}).sort([('created_time',-1)]).limit(1)
    for date in result_date:
        max_date = date['created_time']
    #print '=====>>>>' ,max_date
    since_time=time.mktime(max_date.timetuple())
    post_edge = str(account_id_formal+'/posts')
    page_posts = fb_handle_formal.get_object(post_edge, since=int(since_time+1.0), limit=100)
    while since_time < int(time.time()) and len(page_posts['data'])>0:
        post_json = {
                'post_id': '',
                'from_id': '',
                'page_id':'',
                'from_name' : '',
                'message': '',
                'story': '',
                'picture': '',
                'link': '',
                'liked':0,
                'name': '',
                'icon': '',
                'type': '',
                'status_type': '',
                'created_time':'',
                'updated_time': '',
                'likes':0,
                'comments':0,
                'share':0,
                'sentiment':'',
                'comments_crawled':'no',
                'likers_crawled':'no'
            }
        post_arr = []

        for post in page_posts['data']:
            post_json['post_id'] = str(post['id'])
            post_json['page_id'] = str(account_id_formal)
            post_json['from_id']= str(post['from']['id'])
            post_json['from_name']= post['from']['name'].encode('utf-8')
            if 'caption' in post:
                post_json['message']= post['caption'].encode('utf-8')
            elif 'description' in post:
                post_json['message']= post['description'].encode('utf-8')
            elif 'message' in post:
                post_json['message']=post['message']
            else:
                post_json['message']=''
            try:
                post_json['story'] = str(post['story'])
            except:
                post_json['story'] = ''
            try:
                post_json['picture']= str(post['picture'])
            except:
                post_json['picture']= ''
            try:
                post_json['link'] =  str(post['link'])
            except:
                post_json['link'] =  ''
            try:
                post_json['name'] = str(post['name'])
            except:
                post_json['name'] = ''
            try:
                post_json['icon'] =  str(post['icon'])
            except:
                post_json['icon'] = ''
            try:
                post_json['type'] = str(post['type'])

            except:
                post_json['type'] = ''

            try:
                post_json['status_type'] = str(post['status_type'])
                if(post['status_type']=='mobile_status_update'):
                    pp=fb_handle_formal.get_object(str(post['id'])+'/')
            except:
                post_json['status_type'] = ''
            post_json['created_time'] = datetime.datetime.strptime(post['created_time'], "%Y-%m-%dT%H:%M:%S+0000")
            post_json['updated_time'] = datetime.datetime.strptime(post['updated_time'], "%Y-%m-%dT%H:%M:%S+0000")
            try:
                likes_share_cmnt_res = urllib.urlopen("https://graph.facebook.com/v2.3/"+str(post['id'])+"?fields=shares,likes.summary(true),comments.summary(true)&access_token="+access_token_formal)
                likes_share_cmnt = json.loads(likes_share_cmnt_res.read())
                post_json['share']=likes_share_cmnt['shares']['count']
                post_json['likes']=likes_share_cmnt['likes']['summary']['total_count']
                post_json['comments']=likes_share_cmnt['comments']['summary']['total_count']
            except:
                post_json['likes']=0
                post_json['comments']=0
                post_json['share']=0
            post_json['sentiment']=''
            post_json['comments_crawled'] = 'no'
            post_json['likers_crawled'] = 'no'
            post_arr.append(post_json.copy())
        db_fb_posts = db_connection_formal.fb_post_details
        db_fb_posts.insert(post_arr)
        parsed_url = urlparse(page_posts['paging']['previous'])
        parsed_query_str = parse_qs(parsed_url.query, keep_blank_values=True)
        since_time = int(parsed_query_str['since'][0])
        page_posts = fb_handle_formal.get_object(post_edge, limit=100, since=since_time)

    return 'success post'
def postUpdate(account_id_formal, db_connection_formal, fb_handle_formal, access_token_formal):
    like_change_list = []
    comment_change_list = []
    time_range = datetime.datetime.now()-datetime.timedelta(days=31)
    posts_result = db_connection_formal.fb_post_details.find({'page_id':account_id_formal, 'created_time':{'$gt':time_range}}, {"post_id":1, 'likes':1, 'comments':1, 'share':1,  "_id":0})
    for posts in enumerate(posts_result):
        try:
            print "posts======>>>>>>>", posts
            likes_share_cmnt_res = urllib.urlopen("https://graph.facebook.com/v2.3/"+str(posts[1]['post_id'])+"?fields=shares,likes.summary(true),comments.summary(true)&access_token="+access_token_formal)
            likes_share_cmnt = json.loads(likes_share_cmnt_res.read())
            print "posts======>>>>>>>", posts
            print "likes_share_cmnt======>>>>>>>", likes_share_cmnt
            like_count=likes_share_cmnt['likes']['summary']['total_count']
            comments_count=likes_share_cmnt['comments']['summary']['total_count']
            if like_count > posts[1]['likes']:
                like_change_list.append(posts[1]['post_id'])
                db_connection_formal.fb_post_details.update({'post_id':str(posts[1]['post_id'])}, {'$set':{'likers_crawled':'no'}})
            if comments_count > posts[1]['comments']:
                comment_change_list.append(posts[1]['post_id'])
                db_connection_formal.fb_post_details.update({'post_id':str(posts[1]['post_id'])}, {'$set':{'comments_crawled':'no'}})
            if like_count > posts[1]['likes'] or comments_count > posts[1]['comments']:
                db_connection_formal.fb_post_details.update({'post_id':str(posts[1]['post_id'])}, {'$set':{'likes':like_count, 'comments':comments_count}})
        except:
            pass
def commentFetch(account_id_formal, db_connection_formal, fb_handle_formal):
    db_post_coll = db_connection_formal.fb_post_details
    db_comment_coll = db_connection_formal.fb_comments
    posts_result = db_post_coll.find({"page_id": account_id_formal, "comments":{"$gt": 0 }, 'comments_crawled':'no'}, {"post_id":1, "_id":0})
    for post in enumerate(posts_result):
        comment_idList = []
        comments_db_response = db_connection_formal.fb_comments.find({"page_id": account_id_formal, 'post_id':str(post[1]['post_id'])}, {'comment_id':1, '_id':0})
        for id in comments_db_response:
            comment_idList.append(id['comment_id'])
        comment_response = fb_handle_formal.get_object(str(post[1]['post_id']+"/comments"), limit=50)
        print comment_response
        after_id = 'ff'
        while comment_response['data']>0 and after_id!='':
            comment_list = []
            comment_json = {
                          "post_id": "",
                          "from": {
                                         "id": "",
                                        "name": ""
                                         },
                          "message": "",
                          "created_time": "",
                          "like_count": 0,
                          "page_id":'',
                          "sentiment":'',
                          "keywords":[],
                          "comment_id":""
                            }
            for comment in comment_response['data']:
                if comment['id'] not in comment_idList:
                    comment_json['page_id'] = str(account_id_formal)
                    comment_json['post_id'] = str(post[1]['post_id'])
                    comment_json['from']['id'] = comment['from']['id']
                    comment_json['from']['name'] = comment['from']['name']
                    comment_json['message'] = comment['message']
                    comment_json['created_time'] = datetime.datetime.strptime(comment['created_time'], "%Y-%m-%dT%H:%M:%S+0000")
                    comment_json['like_count'] = comment['like_count']
                    comment_json['comment_id'] = comment['id']
                    comment_list.append(comment_json.copy())
            if 'next' in comment_response['paging']:
                after_id = comment_response['paging']['cursors']['after']
                comment_response = fb_handle_formal.get_object(str(post[1]['post_id']+"/comments"), limit=50, after=after_id )
            else:
                after_id=''
        if len(comment_list)>0:
            db_comment_coll.insert(comment_list)
        db_connection_formal.fb_post_details.update({'post_id':str(post[1]['post_id'])}, {'$set':{'comments_crawled':'yes'}})
    return('successful')
def like_fetch(account_id_formal, db_connection_formal, fb_handle_formal):
    db_post_coll = db_connection_formal.fb_post_details
    posts_result = db_post_coll.find({"from_id": account_id_formal, "likes":{"$gt": 0 }, 'likers_crawled':'no'}, {"post_id":1, "_id":0})
    for post in enumerate(posts_result):
        like_response = fb_handle_formal.get_object(str(post[1]['post_id']+"/likes"), limit=50)
        after_id = 'ff'
        like_json = {
                          "page_id": str(account_id_formal),
                          "post_id": str(post[1]['post_id']),
                          "likers":[]
                            }
        while like_response['data']>0 and after_id!='':
            for like in like_response['data']:
                like_json['likers'].append(like['id'])
            if 'next' in like_response['paging']:
                after_id = like_response['paging']['cursors']['after']
                comment_response = fb_handle_formal.get_object(str(post[1]['post_id']+"/likes"), limit=50, after=after_id )
            else:
                after_id=''
        db_connection_formal.fb_likes.insert(like_json)
        if str(account_id_formal) in like_json['likers']:
            db_connection_formal.fb_post_details.update({'post_id':str(post[1]['post_id'])}, {'$set':{'liked':1}})
        else:
            db_connection_formal.fb_post_details.update({'post_id':str(post[1]['post_id'])}, {'$set':{'liked':0}})
        db_connection_formal.fb_post_details.update({'post_id':str(post[1]['post_id'])}, {'$set':{'likers_crawled':'yes'}})
    return('successful')
def insight_fetch(account_id_formal, db_connection_formal, fb_handle_formal):
    db_insight = db_connection_formal.fb_page_insights
    max_date_response = db_insight.find({'page_id':str(account_id_formal)}, {'date':1, '_id':0}).sort([('date',-1)]).limit(1)
    for dates in max_date_response:
        max_date = dates['date']
    since_time=time.mktime(max_date.timetuple())
    until_time = since_time+86400
    insight_response = fb_handle_formal.get_object(str(account_id_formal+'/insights'), since=since_time, until=until_time)
    insight_json = {
                    "page_id":account_id_formal,
                    "date": "",
                    "page_fan_adds_unique": 0,
                    "page_fan_adds_by_paid_non_paid_unique":{"unpaid":0,
                                        "paid":0,
                                        "total":0
                                            },
                    "page_fan_removes_unique":0,

                    "page_story_adds":0,
                    "page_story_adds_by_story_type":0,

                    "reach":{
                        "page_impressions_by_age_gender_unique":0,
                        "page_impressions_by_country_unique":0,
                        "page_impressions_by_city_unique":0,
                        },
                    "talking_about":{
                        "page_story_adds_by_age_gender_unique":0,
                        "page_story_adds_by_country_unique":0,
                        "page_story_adds_by_city_unique":0
                            },
                    "page_impression":{
                        "total":0,
                        "paid":0,
                        "organic":0,
                        "viral":0
                            },
                    "page_post_impression":{
                        "total":0,
                        "paid":0,
                        "organic":0,
                        "viral":0
                                },
                    "page_negetive_feedback":0,
                    "page_positive_feedback_by_type":{
                                "rsvp": 0,
                                "link": 0,
                                "like": 0,
                                "comment": 0,
                                "claim": 0,
                                "answer": 0
                                    },
                    "page_fans":0,
                    "page_fan_city":0,
                    "page_fan_country":0,
                    "page_fans_gender_age":0
                }
    flag = 0
    while flag!=1:
        insight_json['date']=datetime.datetime.strptime(insight_response['data'][0]['values'][0]['end_time'], "%Y-%m-%dT%H:%M:%S+0000")
        insights_list= []
        insight_keys1 = ['page_fan_adds_unique', 'page_fan_adds_by_paid_non_paid_unique', 'page_fan_removes_unique', 'page_story_adds', 'page_story_adds_by_story_type',
                         'page_impressions_by_country_unique', 'page_impressions_by_city_unique',
                         'page_story_adds_by_country_unique', 'page_story_adds_by_city_unique',
                        'page_impressions', 'page_impressions_paid_unique', 'page_impressions_organic_unique', 'page_impressions_viral_unique',
                        'page_posts_impressions', 'page_posts_impressions_paid_unique', 'page_posts_impressions_organic_unique', 'page_posts_impressions_viral_unique',
                        'page_negetive_feedback', 'page_positive_feedback_by_type', 'page_fans']
        insight_keys3 = ['page_fans_country']
        insight_keys2 = ['page_fans_gender_age']
        for insight in insight_response['data']:
            if insight['name']in insight_keys1:
                if len(insight['values']) < 1:
                    insight_json[insight['name']]=0
                else:
                    insight_json[insight['name']]=insight['values'][0]['value']
            elif insight['name'] in insight_keys2:
                if len(insight['values'])!=1:
                    for values in insight['values']:
                        if len(values['value'])!=0:
                            for key, val in values['value'].iteritems():
                                new_key = key.replace('.', '-')
                                values['value'][new_key]=values['value'].pop(key)
                        else:
                            values['value'] = {
                                                "F-25-34" : 0,
                                                "M-25-34" : 0,
                                                "M-65+" : 0,
                                                "M-13-17" : 0,
                                                "F-18-24" : 0,
                                                "M-18-24" : 0,
                                                "M-35-44" : 0,
                                                "F-35-44" : 0,
                                                "F-45-54":0,
                                                "F-55-64":0,
                                                "M-45-54":0,
                                                "M-55-64":0
                                            }
                    insight_json[insight['name']]=values['value']
                else:
                    for values in insight['values']:
                        if len(values['value'])!=0:
                            for key, val in values['value'].iteritems():
                                new_key = key.replace('.', '-')
                                values['value'][new_key]=values['value'].pop(key)
                        else:
                            values['value'] = { "F-25-34" : 0,
                                                "M-25-34" : 0,
                                                "M-65+" : 0,
                                                "M-13-17" : 0,
                                                "F-18-24" : 0,
                                                "M-18-24" : 0,
                                                "M-35-44" : 0,
                                                "F-35-44" : 0,
                                                "F-45-54":0,
                                                "F-55-64":0,
                                                "M-45-54":0,
                                                "M-55-64":0
                                            }
                        insight_json[insight['name']]=values['value']
            elif insight['name'] in insight_keys3:
                if len(insight['values']) != 3:
                    insight_json[insight['name']]=countryMap(insight['values'][0]['value'], db_connection_formal)
                else:
                    insight_json[insight['name']]=countryMap(insight['values'][0]['value'], db_connection_formal)
        insights_list.append(insight_json.copy())
        flag = 1
    db_insight.insert(insights_list)
    return 'successfull'
def countryMap(country_code, db_connection_formal):
    try:
        print('country_code=============>',country_code)
        countries_res = db_connection_formal.country_code.find({}, {'_id':0})
        for countries in countries_res:
            code_list=countries
        country_dict = {y:x for x,y in code_list.iteritems()}
        print "country_dict======>>>>>", country_dict
        for code in country_code.keys():
            name =country_dict[code]
            country_code[name]=country_code.pop(code)
        return country_code
    except:
        return {}

