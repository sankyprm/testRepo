import time
from urlparse import urlparse, parse_qs
import datetime
def userAccountsInfo(db_connection_formal, account_id_formal, fb_handle_formal, brand_id_formal, fb_account_id_formal):
    #fb_handle = facebook.GraphAPI(access_token_formal)
    user_info = fb_handle_formal.get_object(account_id_formal)
    final_user_info = {
              "brand_id":brand_id_formal,
              "master_page":fb_account_id_formal,
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
    db_fb_accounts = db_connection_formal.fb_accounts_comp
    db_fb_accounts.remove({'page_id':account_id_formal})
    db_fb_accounts.insert(final_user_info)
    final_user_info['date']=datetime.datetime.now()
    db_connection_formal.fb_accounts_comp_daily.insert(final_user_info)
    #db_connection_formal.brands.update({'associated_accounts.fb_accounts.page_id':str(account_id_formal)}, {'$set':{'associated_accounts.fb_accounts.avatar':str(user_info['cover']['source'])}})
    return "account fetched successfully"
def pagePosts(account_id_formal, db_connection_formal, fb_handle_formal, brand_id_formal, fb_account_id_formal):
    post_id_list = []
    max_date=''
    import json
    print account_id_formal
    result_date = db_connection_formal.fb_post_details_comp.find({'page_id':str(account_id_formal), 'master_page':str(fb_account_id_formal)}, {'created_time':1, '_id':0}).sort([('created_time',-1)]).limit(1)
    for date in result_date:
        max_date = date['created_time']
    #print '=====>>>>' ,max_date
    since_time=time.mktime(max_date.timetuple())
    post_edge = str(account_id_formal+'/posts')
    if max_date!='':
        page_posts = fb_handle_formal.get_object(post_edge, since=int(since_time+1.0), limit=100)
    else:
        page_posts = fb_handle_formal.get_object(post_edge, limit=100)
    while since_time < int(time.time()) and len(page_posts['data'])>0:
        post_json = {
                "brand_id":brand_id_formal,
                "master_page":fb_account_id_formal,
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
            post_id_list.append(str(post['id']))
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
            likes_share_cmnt = fb_handle_formal.fql("SELECT like_info.like_count, share_info.share_count, comment_info.comment_count FROM stream WHERE post_id ='"+str(post['id']+"'"))
            for valuess in likes_share_cmnt:
                post_json['likes']=valuess['like_info']['like_count']
                post_json['comments']=valuess['comment_info']['comment_count']
                post_json['share']=valuess['share_info']['share_count']
            post_json['sentiment']=''
            post_json['comments_crawled'] = 'no'
            post_json['likers_crawled'] = 'no'
            post_arr.append(post_json.copy())
        db_fb_posts = db_connection_formal.fb_post_details_comp
        db_fb_posts.insert(post_arr)
        parsed_url = urlparse(page_posts['paging']['previous'])
        parsed_query_str = parse_qs(parsed_url.query, keep_blank_values=True)
        since_time = int(parsed_query_str['since'][0])
        page_posts = fb_handle_formal.get_object(post_edge, limit=100, since=since_time)

    return post_id_list
def postUpdate(account_id_formal, db_connection_formal, fb_handle_formal, brand_id_formal, fb_account_id_formal):
    like_change_list = []
    comment_change_list = []
    time_range = datetime.datetime.now()-datetime.timedelta(days=31)
    posts_result = db_connection_formal.fb_post_details_comp.find({'page_id':account_id_formal, 'master_page':str(fb_account_id_formal), 'created_time':{'$gt':time_range}}, {"post_id":1, 'likes':1, 'comments':1, 'share':1,  "_id":0})
    for posts in enumerate(posts_result):
        likes_share_cmnt = fb_handle_formal.fql("SELECT like_info.like_count, share_info.share_count, comment_info.comment_count FROM stream WHERE post_id ='"+str(posts[1]['post_id']+"'"))
        for valuess in likes_share_cmnt:
            like_count=valuess['like_info']['like_count']
            comments_count=valuess['comment_info']['comment_count']
            share_count=valuess['share_info']['share_count']
        if like_count > posts[1]['likes']:
            like_change_list.append(posts[1]['post_id'])
            db_connection_formal.fb_post_details_comp.update({'post_id':str(posts[1]['post_id'])}, {'$set':{'likers_crawled':'no'}})
        if comments_count > posts[1]['comments']:
            comment_change_list.append(posts[1]['post_id'])
            db_connection_formal.fb_post_details_comp.update({'post_id':str(posts[1]['post_id'])}, {'$set':{'comments_crawled':'no'}})
        if like_count > posts[1]['likes'] or comments_count > posts[1]['comments'] or share_count > posts[1]['share']:
            db_connection_formal.fb_post_details_comp.update({'post_id':str(posts[1]['post_id'])}, {'$set':{'likes':like_count, 'comments':comments_count, 'share':share_count}})
def comments_fetch(account_id_formal, db_connection_formal, fb_handle_formal, post_list_formal, brand_id_formal, fb_account_id_formal):
    for post in post_list_formal:
        comment_response = fb_handle_formal.get_object(str(post+"/comments"), limit=50)
        after_id = 'ff'
        while len(comment_response['data'])>0 and after_id!='':
            print "comment response===>>>>",len(comment_response)
            comment_list = []
            comment_json = {
                            "brand_id":brand_id_formal,
                          "master_page":fb_account_id_formal,
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
                comment_json['page_id'] = str(account_id_formal)
                comment_json['post_id'] = str(post[1]['post_id'])
                comment_json['from']['id'] = comment['from']['id']
                comment_json['from']['name'] = comment['from']['name']
                comment_json['message'] = comment['message']
                comment_json['created_time'] = datetime.datetime.strptime(comment['created_time'], "%Y-%m-%dT%H:%M:%S+0000")
                comment_json['like_count'] = comment['like_count']
                comment_json['comment_id'] = comment['id']
                comment_list.append(comment_json.copy())
            if len(comment_list) != 0:
                db_comment_coll.insert(comment_list)
                after_id=''
            else:
                pass
        db_connection_formal.fb_post_details_comp.update({'post_id':str(post[1]['post_id'])}, {'$set':{'comments_crawled':'yes'}})
    return('successful')
def insight_fetch(account_id_formal, db_connection_formal, fb_handle_formal, brand_id_formal, fb_account_id_formal):
    db_insight = db_connection_formal.fb_page_insights_comp
    max_date_response = db_insight.find({'page_id':str(account_id_formal), 'master_page':str(fb_account_id_formal)}, {'date':1, '_id':0}).sort([('date',-1)]).limit(1)
    for dates in max_date_response:
        max_date = dates['date']
    since_time=time.mktime(max_date.timetuple())
    until_time = since_time+86400
    db_insight = db_connection_formal.fb_page_insights_comp
    insight_response = fb_handle_formal.get_object(str(account_id_formal+'/insights'), since=since_time, until=until_time)
    #print(insight_response)
    #print('Hi I am here')
    #exit()
    since_time=int(time.time())
    while (since_time)>int(time.time()-33*24*60*60):
        print 'since time===>>>>', since_time
        insight_json = {
                        "brand_id":brand_id_formal,
                        "master_page":fb_account_id_formal,
                        "page_id":account_id_formal,
                        "date": "",
                        "page_fan_country":0
                    }
        insight_date1=insight_json.copy()
        insight_date2=insight_json.copy()
        if len(insight_response['data'])>0:
            print len(insight_response['data'])
            flag = 0
            while flag!=1:
                insight_date1['date']=datetime.datetime.strptime(insight_response['data'][0]['values'][0]['end_time'], "%Y-%m-%dT%H:%M:%S+0000")
                insight_date2['date']= datetime.datetime.strptime(insight_response['data'][0]['values'][1]['end_time'], "%Y-%m-%dT%H:%M:%S+0000")
                insights_list= []
                insight_keys3 = ['page_fans_country']
                for insight in insight_response['data']:
                    if len(insight['values']) != 3:
                        #print("insight['values'][0]['value']========================>",type(insight['values'][0]['value']))
                        if isinstance(insight['values'][0]['value'], dict):
                            insight_date1[insight['name']]=countryMap(insight['values'][0]['value'], db_connection_formal)
                            insight_date2[insight['name']]=countryMap(insight['values'][0]['value'], db_connection_formal)
                            insight_json[insight['name']]=countryMap(insight['values'][0]['value'], db_connection_formal)
                        else:
                            insight_date1[insight['name']]= dict()
                            insight_date2[insight['name']]= dict()
                            insight_json[insight['name']]=dict()

                    else:
                        #print("insight['values'][0]['value']========================>",type(insight['values'][0]['value']))
                        if isinstance(insight['values'][0]['value'], dict):
                            insight_date1[insight['name']]=countryMap(insight['values'][0]['value'], db_connection_formal)
                            insight_date2[insight['name']]=countryMap(insight['values'][1]['value'], db_connection_formal)
                            insight_json[insight['name']]=countryMap(insight['values'][2]['value'], db_connection_formal)
                        else:
                            insight_date1[insight['name']]= dict()
                            insight_date2[insight['name']]= dict()
                            insight_json[insight['name']]=dict()
                insights_list.append(insight_date1.copy())
                insights_list.append(insight_date2.copy())
                flag = 1
            db_insight.insert(insights_list)
        since_until = urlparse(insight_response['paging']['previous'])
        since_until_list = parse_qs(since_until.query, keep_blank_values=True)
        since_time= int(since_until_list['since'][0])
        until_time = int(since_until_list['until'][0])
        insight_response = fb_handle_formal.get_object(str(account_id_formal+'/insights'), since=since_time, until=until_time)
    return 'successfull'
def countryMap(country_code, db_connection_formal):
    #print('country_code=============>',country_code)
    countries_res = db_connection_formal.country_code.find({}, {'_id':0})
    for countries in countries_res:
        code_list=countries
    country_dict = {y:x for x,y in code_list.iteritems()}
    for code in country_code.keys():
        name =country_dict[code]
        country_code[name]=country_code.pop(code)
    return country_code