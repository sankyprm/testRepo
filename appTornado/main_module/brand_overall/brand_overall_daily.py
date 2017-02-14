__author__ = 'SIBIAAnalytics'
from bson.code import Code
from bson.objectid import ObjectId
import datetime
from twython import Twython
def find_closest_trend(db_connection_formal, brand_id_formal):
    tw_handle = Twython('inBAC5z1EiCt8k6z32dqw', 's8IK3HT8cWReb2YUSV8kTQvf3qANXwKZhCZORw5XAVg', '177512438-v68R6ZeW9dTZB6PzZgSZSvjnIfAI4rscDioxIcfs', 'oVw6W90eXspQrlAbib6QivHKJ2nJ6nf8dPOK7DxVic')
    current_time_less_one_hr = datetime.datetime.now()-datetime.timedelta(hours=1)
    location_info_res = db_connection_formal.brands.find({'_id':ObjectId(brand_id_formal)}, {'coordinates':1})
    for loc in location_info_res:
        lat= (loc['coordinates']['latitude'])
        long = (loc['coordinates']['longitude'])
    if lat=='NA' or long=='NA':
        whoid = 1
    else:
        whoid_res = tw_handle.get_closest_trends(lat=float(lat), long=float(long))
        print "who id ==================================>>>>>>>>>>>", whoid_res
        print "lat long=======================================>>>>>", float(lat),float(long)
        whoid = whoid_res[0]['woeid']

    created_time = None
    check_tw_trends_res = db_connection_formal.tw_trends.find({'brand_id':brand_id_formal})
    for check_res in check_tw_trends_res:
        created_time = check_res['created_at']
    if created_time==None or created_time<current_time_less_one_hr:
        trends_response = tw_handle.get_place_trends(id=whoid)
        print trends_response
        trends_response[0]['created_at'] = datetime.datetime.strptime(trends_response[0]['created_at'],"%Y-%m-%dT%H:%M:%SZ")
        trends_response[0]['brand_id'] = brand_id_formal
        db_connection_formal.tw_trends.insert(trends_response[0])
    else:
        pass

def createBrandJson(db_connection_formal, brand_id_formal):
    no_of_brand = db_connection_formal.brands.find({'_id':ObjectId(brand_id_formal)}, {}).count()
    json_list = []
    brand_counter = 0
    while brand_counter<no_of_brand:
        json_format = {
	'brand_id':'',
	'total_pages':'',
	'total_feedback':'',
	'total_follower':'',
	'total_posts':'',
	'reach_chart':{
		'date':[],
		'fb_reach':[],
		'tw_reach':[],
		'inst_reach':[],
		'vk_reach':[]
	},
	'sentiment':{
		'pos':0,
		'neg':0,
		'neutral':0
			},
	'posts':{
		'fb':[],
		'tw':[],
		'inst':[],
		'in':[],
		'utube':[],
		'vk':[],
		'gp':[]
	},
	'pages':{
		'fb':[],
		'tw':[],
		'inst':[],
		'in':[],
		'utube':[],
		'vk':[],
		'gp':[]
	}
}
        json_list.append(json_format.copy())
        brand_counter+=1
    return json_list
def totalPages(db_connection_formal,brand_overall_json_formal, brand_id_formal):
    page_ids = {
        "brand_id":'',
        "fb_page_ids":[],
        "tw_page_ids":[],
        "inst_page_ids":[],
        "utube_page_ids":[],
        "in_page_ids":[],
        "vk_page_ids":[],
        "gp_page_ids":[]
    }
    page_ids_list=[]
    no_of_pages = db_connection_formal.brands.find({'_id':ObjectId(brand_id_formal)}, {'_id':1, 'associated_accounts':1})
    page_counter = 0
    for pages in no_of_pages:
        print 'pages+>>>>>>>>>',pages
        brand_overall_json_formal[page_counter]['total_pages'] = len(pages['associated_accounts']['fb_accounts'])+len(pages['associated_accounts']['google_accounts'])+\
            len(pages['associated_accounts']['in_accounts'])+len(pages['associated_accounts']['ins_accounts'])+len(pages['associated_accounts']['pin_accounts'])+\
            len(pages['associated_accounts']['qq_accounts'])+len(pages['associated_accounts']['ren_accounts'])+len(pages['associated_accounts']['tw_accounts'])+\
            len(pages['associated_accounts']['utube_accounts'])+len(pages['associated_accounts']['weibo_accounts'])+len(pages['associated_accounts']['vk_accounts'])
        brand_overall_json_formal[page_counter]['brand_id'] = page_ids['brand_id']=pages['_id']
        page_ids['fb_page_ids']= [each_page['page_id'] for each_page in pages['associated_accounts']['fb_accounts']]
        page_ids['in_page_ids']= [each_page['page_id'] for each_page in pages['associated_accounts']['in_accounts']]
        page_ids['tw_page_ids']= [each_page['page_id'] for each_page in pages['associated_accounts']['tw_accounts']]
        page_ids['inst_page_ids']= [each_page['page_id'] for each_page in pages['associated_accounts']['ins_accounts']]
        page_ids['utube_page_ids']= [each_page['page_id'] for each_page in pages['associated_accounts']['utube_accounts']]
        page_ids['vk_page_ids']= [each_page['page_id'] for each_page in pages['associated_accounts']['vk_accounts']]
        page_ids['gp_page_ids']= [each_page['page_id'] for each_page in pages['associated_accounts']['google_accounts']]
        page_ids_list.append(page_ids.copy())
        page_counter+=1
    return [brand_overall_json_formal, page_ids_list]
def totalFeedback(db_connection_formal, brand_overall_json_formal, page_ids_list_formal):
    page_counter = 0
    for pages in page_ids_list_formal:
        feedback_count = 0
        feedback_count = feedback_count+db_connection_formal.fb_comments.find({'page_id':{'$in':pages['fb_page_ids']}}).count()
        fb_likes = db_connection_formal.fb_likes.find({'page_id':{'$in':pages['fb_page_ids']}})
        for likers in fb_likes:
            feedback_count=feedback_count+len(likers["likers"])
        feedback_count = feedback_count+db_connection_formal.tw_mentions.find({'user_id':{'$in':pages['tw_page_ids']}}).count()
        feedback_count = feedback_count+db_connection_formal.tw_retweets.find({'user_id':{'$in':pages['tw_page_ids']}}).count()
        feedback_count = feedback_count+db_connection_formal.inst_likes.find({'user_id':{'$in':pages['inst_page_ids']}}).count()
        feedback_count = feedback_count+db_connection_formal.inst_comments.find({'user_id':{'$in':pages['inst_page_ids']}}).count()
        feedback_count = feedback_count+db_connection_formal.li_comments.find({'id':{'$in':pages['in_page_ids']}}).count()
        feedback_count = feedback_count+db_connection_formal.vk_wall_comments.find({'user_id':{'$in':pages['vk_page_ids']}}).count()
        vk_likes = db_connection_formal.vk_wall_likes.find({'user_id':{'$in':pages['vk_page_ids']}})
        for likers in vk_likes:
            feedback_count=feedback_count+len(likers['likers'])
        feedback_count = feedback_count+db_connection_formal.utube_video_comments.find({'yt_channelId':{'$in':pages['utube_page_ids']}}).count()
        brand_overall_json_formal[page_counter]['total_feedback']=feedback_count
        page_counter+=1
    return 1
def totalFollower(db_connection_formal, brand_overall_json_formal, page_ids_list_formal):
    page_counter = 0
    for pages in page_ids_list_formal:
        follower_count = 0
        fb_like_res=db_connection_formal.fb_accounts.find({'page_id':{'$in':pages['fb_page_ids']}})
        for likes in fb_like_res:
            follower_count=follower_count+likes['likes']
        tw_like_res=db_connection_formal.tw_users.find({'id':{'$in':map(int, pages['tw_page_ids'])}})
        for likes in tw_like_res:
            follower_count=follower_count+likes['followers_count']
        inst_like_res=db_connection_formal.inst_user.find({'ins_id':{'$in':pages['inst_page_ids']}})
        for likes in inst_like_res:
            follower_count=follower_count+likes['counts']['followed_by']
        in_like_res=db_connection_formal.li_basic_info.find({'id':{'$in':map(int, pages['in_page_ids'])}})
        for likes in in_like_res:
            follower_count=follower_count+likes['numFollowers']
        vk_like_res=db_connection_formal.vk_user.find({'uid':{'$in':map(int, pages['vk_page_ids'])}})
        for likes in vk_like_res:
            follower_count=follower_count+likes['counters']['followers']
        brand_overall_json_formal[page_counter]['total_follower']=follower_count
        page_counter+=1
    return 1
def totalPosts(db_connection_formal, brand_overall_json_formal, page_ids_list_formal):
    page_counter = 0
    for pages in page_ids_list_formal:
        total_posts= 0
        total_posts = total_posts+db_connection_formal.fb_post_details.find({'page_id':{'$in':pages['fb_page_ids']}}).count()
        total_posts = total_posts+db_connection_formal.tw_tweets.find({'user_id':{'$in':pages['tw_page_ids']}}).count()
        total_posts = total_posts+db_connection_formal.inst_media.find({'user_id':{'$in':pages['inst_page_ids']}}).count()
        total_posts = total_posts+db_connection_formal.vk_wall_posts.find({'user_id':{'$in':map(int, pages['vk_page_ids'])}}).count()
        total_posts = total_posts+db_connection_formal.li_shares.find({'updateContent.company.id':{'$in':map(int, pages['in_page_ids'])}}).count()
        total_posts = total_posts+db_connection_formal.utube_channel_videos.find({'channelId':{'$in':pages['utube_page_ids']}}).count()
        brand_overall_json_formal[page_counter]['total_posts']=total_posts
        page_counter+=1
    return 1
def topPosts(db_connection_formal, brand_overall_json_formal, page_ids_list_formal):
    page_counter = 0
    for pages in page_ids_list_formal:
        #===================>fb
        fb_posts = db_connection_formal.fb_post_details.find({'page_id':{'$in':pages['fb_page_ids']}}).sort([('likes',-1), ('comments',-1)]).limit(5)
        fb_post_list = []
        for posts in fb_posts:
            fb_post_list.append(posts.copy())
        brand_overall_json_formal[page_counter]['posts']['fb']=fb_post_list
        #print "pages==========>>>",brand_overall_json_formal[page_counter]['posts']['fb']
        #==========================>tw
        tw_posts = db_connection_formal.tw_tweets.find({'user_id':{'$in':pages['tw_page_ids']}}).sort([('retweet_count',-1), ('favorite_count',-1)]).limit(5)
        tw_post_list = []
        for posts in tw_posts:
            tw_post_list.append(posts.copy())
        brand_overall_json_formal[page_counter]['posts']['tw']=tw_post_list
        #===========================>inst
        inst_posts = db_connection_formal.inst_media.find({'user_id':{'$in':pages['inst_page_ids']}}).sort([('likes_count',-1), ('comments_count',-1)]).limit(5)
        inst_post_list = []
        for posts in inst_posts:
            inst_post_list.append(posts.copy())
        brand_overall_json_formal[page_counter]['posts']['inst']=inst_post_list
        #=============================>LI
        in_posts = db_connection_formal.li_shares.find({'updateContent.company.id':{'$in':map(int, pages['in_page_ids'])}}).sort([('numLikes',-1)]).limit(5)
        in_posts_list = []
        for posts in in_posts:
            in_posts_list.append(posts.copy())
        brand_overall_json_formal[page_counter]['posts']['in']=in_posts_list
        #===============================>vk
        vk_posts = db_connection_formal.vk_wall_posts.find({'user_id':{'$in':map(int, pages['vk_page_ids'])}}).sort([('likes.count',-1), ('comments.count',-1)]).limit(5)
        vk_posts_list = []
        for posts in vk_posts:
            vk_posts_list.append(posts.copy())
        brand_overall_json_formal[page_counter]['posts']['vk']=vk_posts_list
        #===============================>utube
        utube_posts = db_connection_formal.utube_channel_videos.find({'channelId':{'$in':pages['utube_page_ids']}}).sort([('rank',-1)]).limit(5)
        utube_posts_list = []
        for posts in utube_posts:
            utube_posts_list.append(posts.copy())
        brand_overall_json_formal[page_counter]['posts']['utube']=utube_posts_list
        page_counter+=1
    return 1
def sentimentScore(db_connection_formal, brand_overall_json_formal, page_ids_list_formal):
    page_counter = 0
    for pages in page_ids_list_formal:
        pos_count = 0
        neg_count= 0
        neutral_count = 0
        fb_senti_res = db_connection_formal.fb_post_details.aggregate([{'$match':{'page_id':{'$in':pages['fb_page_ids']}}},{'$group':{
            '_id':'$sentiment',
            'count':{'$sum':1}
        }}])
        pos_count, neg_count, neutral_count=calculateScore(pos_count, neg_count, neutral_count, fb_senti_res)
        tw_senti_res = db_connection_formal.tw_tweets.aggregate([{'$match':{'page_id':{'$in':pages['tw_page_ids']}}},{'$group':{
            '_id':'$sentiment',
            'count':{'$sum':1}
        }}])
        pos_count, neg_count, neutral_count=calculateScore(pos_count, neg_count, neutral_count, tw_senti_res)
        inst_senti_res = db_connection_formal.inst_media.aggregate([{'$match':{'page_id':{'$in':pages['inst_page_ids']}}},{'$group':{
            '_id':'$sentiment',
            'count':{'$sum':1}
        }}])
        pos_count, neg_count, neutral_count=calculateScore(pos_count, neg_count, neutral_count, inst_senti_res)
        vk_senti_res = db_connection_formal.vk_wall_posts.aggregate([{'$match':{'page_id':{'$in':pages['vk_page_ids']}}},{'$group':{
            '_id':'$sentiment',
            'count':{'$sum':1}
        }}])
        pos_count, neg_count, neutral_count=calculateScore(pos_count, neg_count, neutral_count, vk_senti_res)
        in_senti_res = db_connection_formal.li_shares.aggregate([{'$match':{'page_id':{'$in':pages['in_page_ids']}}},{'$group':{
            '_id':'$sentiment',
            'count':{'$sum':1}
        }}])
        pos_count, neg_count, neutral_count=calculateScore(pos_count, neg_count, neutral_count, in_senti_res)
        utube_senti_res = db_connection_formal.utube_channel_videos.aggregate([{'$match':{'page_id':{'$in':pages['utube_page_ids']}}},{'$group':{
            '_id':'$sentiment',
            'count':{'$sum':1}
        }}])
        pos_count, neg_count, neutral_count=calculateScore(pos_count, neg_count, neutral_count, utube_senti_res)
        brand_overall_json_formal[page_counter]['sentiment']['pos']=pos_count
        brand_overall_json_formal[page_counter]['sentiment']['neg']=neg_count
        brand_overall_json_formal[page_counter]['sentiment']['neutral']=neutral_count
        page_counter+=1
def calculateScore(pc, nc, nuc, db_res):
    for res in db_res['result']:
        if res['_id']=='neg':
            nc = nc+res['count']
        if res['_id']=='pos':
            pc = pc +res['count']
        if res['_id']=='neutral' or res['_id']=='':
            nuc = nuc+res['count']
    return [pc, nc, nuc]
def reachPlatform(db_connection_formal, brand_overall_json_formal, page_ids_list_formal):
    last_date_upto_fetch = datetime.datetime.today()- datetime.timedelta(days=30, hours=0, minutes=0, seconds=0, microseconds=0)
    base =datetime.datetime.today()
    date_list = [(base - datetime.timedelta(days=x, hours=0, minutes=0, seconds=0, microseconds=0)).replace(hour=0, minute=0, second=0, microsecond=0) for x in range(0, 31)]
    page_counter = 0
    for pages in page_ids_list_formal:
        fb_reach_res = db_connection_formal.fb_page_insights.aggregate([{'$match':{'page_id':{'$in':pages['fb_page_ids']}, 'date':{'$gt':last_date_upto_fetch}}},{'$group':{
            '_id':'$date',
            'reach':{'$sum':'$page_posts_impressions'}
        }}])
        brand_overall_json_formal[page_counter]['reach_chart']['date']=date_list
        brand_overall_json_formal[page_counter]['reach_chart']['fb_reach']=calculateReachList(date_list, fb_reach_res)
        tw_reach_res = db_connection_formal.tw_daily_reach.aggregate([{'$match':{'user_id':{'$in':pages['tw_page_ids']}, 'date':{'$gt':last_date_upto_fetch}}},{'$group':{
            '_id':'$date',
            'reach':{'$sum':'$reach'}
        }}])
        tw = calculateReachList(date_list, tw_reach_res)
        brand_overall_json_formal[page_counter]['reach_chart']['tw_reach']=tw
        #print tw, '=================>tw reach'
        inst_reach_res = db_connection_formal.inst_daily_reach.aggregate([{'$match':{'user_id':{'$in':pages['inst_page_ids']}, 'date':{'$gt':last_date_upto_fetch}}},{'$group':{
            '_id':'$date',
            'reach':{'$sum':'$reach'}
        }}])
        inst = calculateReachList(date_list, inst_reach_res)
        #print inst, '====================inst reach'
        brand_overall_json_formal[page_counter]['reach_chart']['inst_reach']=inst
        vk_reach_res = db_connection_formal.vk_daily_reach.aggregate([{'$match':{'user_id':{'$in':pages['vk_page_ids']}, 'date':{'$gt':last_date_upto_fetch}}},{'$group':{
            '_id':'$date',
            'reach':{'$sum':'$reach'}
        }}])
        vk = calculateReachList(date_list, vk_reach_res)
        #print vk , '================vk reach'
        brand_overall_json_formal[page_counter]['reach_chart']['vk_reach']=vk
        page_counter+=1
def calculateReachList(date_arr_formal, db_res_formal):
    reachArr = [0]*len(date_arr_formal)
    for res in db_res_formal['result']:
        #print "=====================> reach---",db_res_formal
        date_formated = (res['_id']).replace(hour=0, minute=0, second=0, microsecond=0)
        try:
            position = date_arr_formal.index(date_formated)
            reachArr[position]=res['reach']
        except:
            pass
    return reachArr
def pageInfo(db_connection_formal, brand_overall_json_formal, page_ids_list_formal):
    page_counter = 0
    for pages in page_ids_list_formal:
        #============================>fb
        info_list = []
        for ids in pages['fb_page_ids']:
            fb_res_count = db_connection_formal.fb_accounts.find({'page_id':ids}, {'likes':1, 'talking_about_count':1, 'new_like_count':1, '_id':0, 'name':1, 'cover':1}).count()
            fb_res = db_connection_formal.fb_accounts.find({'page_id':ids}, {'likes':1, 'talking_about_count':1, 'new_like_count':1, '_id':0, 'name':1, 'cover':1})
            page_fb_info = {}
            if fb_res_count!=0:
                for res in fb_res:
                    page_fb_info = {
                        'page_id':ids,
                        'avatar':res['cover'],
                        'page_name':res['name'],
                        'likes':res['likes'],
                        'talking_about_count':res['talking_about_count'],
                        'new_like_count':res['new_like_count'],
                        'status':'fetched'
                    }
            else:
                access_token_result = db_connection_formal.brands.find({"associated_accounts.fb_accounts.page_id":ids}, {"associated_accounts.fb_accounts.token":1,"associated_accounts.fb_accounts.page_id":1, "_id": 0, "associated_accounts.fb_accounts.name":1})
                token = ''
                token_secret = ''
                #print(access_token_result[0]['associated_accounts']['tw_accounts'])
                for accounts in access_token_result[0]['associated_accounts']['fb_accounts']:
                    if accounts['page_id']==ids:
                        access_token = accounts['token']
                        name=accounts['name']
                        print(name)
                page_fb_info = {
                        'page_id':ids,
                        'avatar':{"source":"http://sociabyte.com/theme/images/social-facebook-icon.png"},
                        'page_name':name,
                        'likes':"NA",
                        'talking_about_count':"NA",
                        'new_like_count':"NA",
                        'status':'notfetched'
                    }
            info_list.append(page_fb_info.copy())
            print "fb pages ==========>>>>>", pages['fb_page_ids']
            print "info list==========>>>>>", info_list
        brand_overall_json_formal[page_counter]['pages']['fb']=info_list
        #==============================>tw
        info_list = []
        for ids in pages['tw_page_ids']:
            tw_res_count = db_connection_formal.tw_users.find({'user_id':ids}, {'followers_count':1, 'statuses_count':1, 'favourites_count':1, '_id':0, 'name':1, 'profile_image_url':1}).count()
            tw_res = db_connection_formal.tw_users.find({'user_id':ids}, {'followers_count':1, 'statuses_count':1, 'favourites_count':1, '_id':0, 'name':1, 'profile_image_url':1})
            page_tw_info = {}
            if tw_res_count!=0:
                for res in tw_res:
                    page_tw_info = {
                        'page_id':ids,
                        'avatar':res['profile_image_url'],
                        'page_name':res['name'],
                        'followers_count':res['followers_count'],
                        'statuses_count':res['statuses_count'],
                        'favourites_count':res['favourites_count'],
                        'status':'fetched'
                    }
                info_list.append(page_tw_info.copy())
            else:
                access_token_result = db_connection_formal.brands.find({"associated_accounts.tw_accounts.page_id":ids}, {"associated_accounts.tw_accounts.token":1,"associated_accounts.tw_accounts.page_id":1, "_id": 0, "associated_accounts.tw_accounts.tokenSecret":1,"associated_accounts.tw_accounts.name":1})

                token = ''
                token_secret = ''
                #print(access_token_result[0]['associated_accounts']['tw_accounts'])
                for accounts in access_token_result[0]['associated_accounts']['tw_accounts']:
                    if accounts['page_id']==ids:
                        token = accounts['token']
                        token_secret = accounts['tokenSecret']
                        name=accounts['name']
                page_tw_info = {
                        'page_id':ids,
                        'avatar':"http://sociabyte.com/theme/images/Twitter-icon.png",
                        'page_name':name,
                        'followers_count':"NA",
                        'statuses_count':"NA",
                        'favourites_count':"NA",
                        'status':'notfetched'
                    }
                info_list.append(page_tw_info.copy())
        brand_overall_json_formal[page_counter]['pages']['tw']=info_list
        #==============================>inst
        info_list = []
        for ids in pages['inst_page_ids']:
            inst_res_count = db_connection_formal.inst_user.find({'ins_id':ids}, {'counts':1, '_id':0, 'username':1, 'profile_picture':1}).count()
            inst_res = db_connection_formal.inst_user.find({'ins_id':ids}, {'counts':1, '_id':0, 'username':1, 'profile_picture':1})
            page_inst_info={}
            if inst_res_count!=0:
                for res in inst_res:
                    page_inst_info = {
                        'page_id':ids,
                        'avatar':res['profile_picture'],
                        'page_name':res['username'],
                        'followers_count':res['counts']['followed_by'],
                        'statuses_count':res['counts']['media'],
                        'follows':res['counts']['follows'],
                        'status':'fetched'
                    }
                info_list.append(page_inst_info.copy())
            else:
                access_token_result = db_connection_formal.brands.find({"associated_accounts.ins_accounts.page_id":ids}, {"associated_accounts.ins_accounts.token":1,"associated_accounts.ins_accounts.page_id":1, "_id": 0, "associated_accounts.ins_accounts.name":1})

                token = ''
                token_secret = ''
                #print(access_token_result[0]['associated_accounts']['tw_accounts'])
                for accounts in access_token_result[0]['associated_accounts']['ins_accounts']:
                    if accounts['page_id']==ids:
                        token = accounts['token']
                        name=accounts['name']
                page_inst_info = {
                        'page_id':ids,
                        'avatar':"http://sociabyte.com/theme/images/Instagram_Icon.png",
                        'page_name':name,
                        'followers_count':"NA",
                        'statuses_count':"NA",
                        'follows':"NA",
                        'status':'notfetched'
                    }
                info_list.append(page_inst_info.copy())
        brand_overall_json_formal[page_counter]['pages']['inst']=info_list
        #================================>in
        info_list = []
        for ids in pages['in_page_ids']:
            in_res_count = db_connection_formal.li_basic_info.find({'id':int(ids)}).count()
            in_res = db_connection_formal.li_basic_info.find({'id':int(ids)})
            page_in_info={}
            if in_res_count!=0:
                for res in in_res:
                    if res.has_key('employeeCountRange'):
                        ecr = res['employeeCountRange']
                    else:
                        ecr = 0
                    page_in_info = {
                        'page_id':ids,
                        'avatar':res['logoUrl'],
                        'page_name':res['name'],
                        'followers_count':res['numFollowers'],
                        'employeeCountRange':ecr,
                        'status':'fetched'
                    }
                info_list.append(page_in_info.copy())
            else:
                access_token_result = db_connection_formal.brands.find({"associated_accounts.in_accounts.page_id":ids}, {"associated_accounts.in_accounts.token":1,"associated_accounts.in_accounts.page_id":1, "_id": 0, "associated_accounts.in_accounts.name":1})

                token = ''
                token_secret = ''
                #print(access_token_result[0]['associated_accounts']['tw_accounts'])
                for accounts in access_token_result[0]['associated_accounts']['in_accounts']:
                    if accounts['page_id']==ids:
                        token = accounts['token']
                        name=accounts['name']
                page_in_info = {
                        'page_id':ids,
                        'avatar':"http://sociabyte.com/theme/images/linkedin-icon.png",
                        'page_name':name,
                        'followers_count':"NA",
                        'employeeCountRange':"NA",
                        'status':'notfetched'
                    }
                info_list.append(page_in_info.copy())
        brand_overall_json_formal[page_counter]['pages']['in']=info_list
        #================================vk
        info_list = []
        for ids in pages['vk_page_ids']:
            vk_res_count = db_connection_formal.vk_user.find({'uid':int(ids)}, {'followers_count':1, '_id':0, 'photo_50':1, 'first_name':1, 'last_name':1, 'counters':1}).count()
            vk_res = db_connection_formal.vk_user.find({'uid':int(ids)}, {'followers_count':1, '_id':0, 'photo_50':1, 'first_name':1, 'last_name':1, 'counters':1})
            page_in_info={}
            if vk_res_count!=0:
                for res in vk_res:
                    page_in_info = {
                        'page_id':ids,
                        'avatar':res['photo_50'],
                        'page_name':res['first_name']+res['last_name'],
                        'followers_count':res['followers_count'],
                        'counters':res['counters'],
                        'status':'fetched'
                    }
                info_list.append(page_in_info.copy())
            else:
                access_token_result = db_connection_formal.brands.find({"associated_accounts.vk_accounts.page_id":ids}, {"associated_accounts.vk_accounts.token":1,"associated_accounts.vk_accounts.page_id":1, "_id": 0, "associated_accounts.vk_accounts.name":1})

                token = ''
                token_secret = ''
                #print(access_token_result[0]['associated_accounts']['tw_accounts'])
                for accounts in access_token_result[0]['associated_accounts']['vk_accounts']:
                    if accounts['page_id']==ids:
                        token = accounts['token']
                        name=accounts['name']
                page_in_info = {
                        'page_id':ids,
                        'avatar':"http://sociabyte.com/theme/images/Vk.png",
                        'page_name':name,
                        'followers_count':"NA",
                        'counters':"NA",
                        'status':'notfetched'
                    }
                info_list.append(page_in_info.copy())
        brand_overall_json_formal[page_counter]['pages']['vk']=info_list
        #===============================utube
        info_list = []
        for ids in pages['utube_page_ids']:
            utube_res_count = db_connection_formal.utube_basic_details.find({'channel_id':ids}).count()
            utube_res = db_connection_formal.utube_basic_details.aggregate([{
                '$match':{'channel_id':ids}
            },{
                                        "$group": {
                                            "_id": ids,
                                            "likeCount": {
                                                "$sum": "$value.likes"
                                            },
                                            "viewCount": {
                                                "$sum": "$value.views"
                                            },
                                            "shareCount": {
                                                "$sum": "$value.shares"
                                            }
                                        }
                         }
                                                                            ])


            page_in_info={}
            if utube_res_count!=0:
                for res in utube_res['result']:
                    page_in_info = {
                        'page_id':ids,
                        'likeCount':res['likeCount'],
                        'viewCount':res['viewCount'],
                        'shareCount':res['shareCount'],
                        'status':'fetched'
                    }

                utube_basics = db_connection_formal.brands.find({'associated_accounts.utube_accounts.page_id':str(ids), '_id':ObjectId(pages['brand_id'])}, {'associated_accounts.utube_accounts.avatar':1, 'associated_accounts.utube_accounts.name':1, '_id':0, 'associated_accounts.utube_accounts.page_id':1,})
                counter = 0
                for acc in utube_basics:
                    print acc, "===="
                    for acc_unit in acc['associated_accounts']['utube_accounts']:
                        if acc['associated_accounts']['utube_accounts'][counter]['page_id']==str(ids):
                            page_in_info['avatar'] = acc['associated_accounts']['utube_accounts'][counter]['avatar']
                            page_in_info['page_name'] = acc['associated_accounts']['utube_accounts'][counter]['name']
                        else:
                            pass
                        counter = counter+1
                info_list.append(page_in_info.copy())
            else:
                page_in_info = {
                        'page_id':ids,
                        'likeCount':"NA",
                        'viewCount':"NA",
                        'shareCount':"NA",
                        'status':'notfetched'
                    }

                utube_basics = db_connection_formal.brands.find({'associated_accounts.utube_accounts.page_id':str(ids), '_id':ObjectId(pages['brand_id'])}, {'associated_accounts.utube_accounts.avatar':1, 'associated_accounts.utube_accounts.name':1, '_id':0, 'associated_accounts.utube_accounts.page_id':1,})
                counter = 0
                for acc in utube_basics:
                    print acc, "===="
                    for acc_unit in acc['associated_accounts']['utube_accounts']:
                        if acc['associated_accounts']['utube_accounts'][counter]['page_id']==str(ids):
                            page_in_info['avatar'] = "http://sociabyte.com/theme/images/YouTube-icon.png"
                            page_in_info['page_name'] = acc['associated_accounts']['utube_accounts'][counter]['name']
                        else:
                            pass
                        counter = counter+1
                info_list.append(page_in_info.copy())
        brand_overall_json_formal[page_counter]['pages']['utube']=info_list
        #=============================================> utube details from brand

        page_counter+=1
def calcualteCrisis(db_connection_formal, page_ids_list_formal, user_id, uname, media):
    if user_id=="NR":
        pass
    else:
        import urllib
        import json
        crisis_json = {
            "date":datetime.datetime.now(),
            "brand_id":"",
            "fb":[],
            "tw":[],
            "inst":[],
            "in":[],
            "vk":[],
            "utube":[],
            "projects":[]
        }
        page_counter = 0
        for pages in page_ids_list_formal:
            crisis_json['brand_id']=pages['brand_id']
            crisis_words = []
            crisis_words_response = db_connection_formal.brands.find({'_id':ObjectId(pages['brand_id'])}, {'_id':0, 'crisis_keywords':1})
            for res in crisis_words_response:
                crisis_words_case_insensitive = res['crisis_keywords']
            for words in crisis_words_case_insensitive:
                crisis_words.append((words.lower()).strip())
            if media=="FB":
                for page_ids in pages['fb_page_ids']:
                    fb_posts=db_connection_formal.fb_post_details.find({'page_id':page_ids}).count()
                    fb_post = db_connection_formal.fb_post_details.find({'page_id':page_ids})
                    fb_post_count=0
                    for posts in fb_post:
                        if posts['from_id']==page_ids:
                            msg = posts['message'].lower()
                            msg_list = msg.split(' ')
                            commn = list(set(msg_list).intersection(crisis_words))
                            if len(commn)!=0:
                                fb_post_count+=1
                    fb_comnt = db_connection_formal.fb_comments.find({'page_id':page_ids}).count()
                    fb_comnts = db_connection_formal.fb_comments.find({'page_id':page_ids})
                    fb_comnt_count=0
                    for posts in fb_comnts:
                        if posts['from']['id']==page_ids:
                            msg = posts['message'].lower()
                            msg_list = msg.split(' ')
                            commn = list(set(msg_list).intersection(crisis_words))
                            if len(commn)!=0:
                                fb_comnt_count+=1
                    if (fb_comnt+fb_posts)>0:
                        crisis_percentage = (float(fb_post_count+fb_comnt_count)/float(fb_comnt+fb_posts))*100
                        print "crisis percentage======>>>>>>>>>>>", crisis_percentage, ((fb_post_count+fb_comnt_count)/(fb_comnt+fb_posts))
                        if crisis_percentage>70:
                            insert_json = {
                        "to" : user_id,
                        "text" : "crisis occur for the facebook page named as "+uname,
                        "action_url" : "http://sociabyte.com/notifications",
                        "status" : "unseen",
                        "timestamp":datetime.datetime.now()
                    }
                            id_file_db = db_connection_formal.notifications.insert(insert_json)
                            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
                            urllib.urlopen(url_download)
                    else:
                        crisis_percentage = 0
                    fb_json = {'page_id':page_ids, 'crisis_percentage':crisis_percentage}
                    crisis_json['fb'].append(fb_json)
            #print "pages==========>>>",brand_overall_json_formal[page_counter]['posts']['fb']
            #==========================>tw
            if media=="TW":
                for page_ids in pages['tw_page_ids']:
                    tw_posts = db_connection_formal.tw_mentions.find({'user_id':page_ids}).count()
                    tw_post = db_connection_formal.tw_mentions.find({'user_id':page_ids})
                    tw_posts_count = 0
                    for tweets in tw_post:
                        if tweets['user']['id_str']==page_ids:
                            msg = tweets['text'].lower()
                            msg_list = msg.split(' ')
                            commn = list(set(msg_list).intersection(crisis_words))
                            if len(commn)!=0:
                                tw_posts_count+=1
                    if tw_posts>0:
                        crisis_percentage=(float(tw_posts_count)/float(tw_posts))*100
                        if crisis_percentage>70:
                            insert_json = {
                        "to" : user_id,
                        "text" : "crisis occur for the twitter page named as "+uname,
                        "action_url" : "http://sociabyte.com/notifications",
                        "status" : "unseen",
                        "timestamp":datetime.datetime.now()
                    }
                            id_file_db = db_connection_formal.notifications.insert(insert_json)
                            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
                            urllib.urlopen(url_download)
                    else:
                        crisis_percentage = 0
                    tw_json = {
                        'page_id':page_ids,
                        'crisis_percentage':crisis_percentage
                    }
                    crisis_json['tw'].append(tw_json)
            #===========================>inst
            if media=="INST":
                for page_ids in pages['inst_page_ids']:
                    inst_cmnts = db_connection_formal.inst_comments.find({'user_id':page_ids}).count()
                    inst_cmnt = db_connection_formal.inst_comments.find({'user_id':page_ids})
                    inst_cmnts_count=0
                    for cmnts in inst_cmnt:
                        if cmnts['from']['id']==page_ids:
                            msg = cmnts['text'].lower()
                            msg_list = msg.split(' ')
                            commn = list(set(msg_list).intersection(crisis_words))
                            if len(commn)!=0:
                                inst_cmnts_count+=1
                    if inst_cmnts>0:
                        crisis_percentage = (float(inst_cmnts_count)/float(inst_cmnts))*100
                        if crisis_percentage>70:
                            insert_json = {
                        "to" : user_id,
                        "text" : "crisis occur for the instagram page named as "+uname,
                        "action_url" : "http://sociabyte.com/notifications",
                        "status" : "unseen",
                        "timestamp":datetime.datetime.now()
                    }
                            id_file_db = db_connection_formal.notifications.insert(insert_json)
                            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
                            urllib.urlopen(url_download)
                    else:
                        crisis_percentage = 0
                    instagram_json = {'page_id':page_ids, 'crisis_percentage':crisis_percentage}
                    crisis_json['inst'].append(instagram_json)
            #=============================>LI
            if media=="IN":
                for page_ids in pages['in_page_ids']:
                    in_comments = db_connection_formal.li_comments.find({'id':page_ids}).count()
                    in_comment = db_connection_formal.li_comments.find({'id':page_ids})
                    in_comments_count=0
                    for shares in in_comment:
                        try:
                            if shares['person']['id']==page_ids:
                                msg = shares['comment'].lower()
                                msg_list = msg.split(' ')
                                commn = list(set(msg_list).intersection(crisis_words))
                                if len(commn)!=0:
                                    in_comments_count+=1
                        except:
                            pass
                    if in_comments>0:
                        crisis_percentage = (float(in_comments_count)/float(in_comments))*100
                        if crisis_percentage>70:
                            insert_json = {
                        "to" : user_id,
                        "text" : "crisis occur for the linked in page named as "+uname,
                        "action_url" : "http://sociabyte.com/notifications",
                        "status" : "unseen",
                        "timestamp":datetime.datetime.now()
                    }
                            id_file_db = db_connection_formal.notifications.insert(insert_json)
                            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
                            urllib.urlopen(url_download)
                    else:
                        crisis_percentage = 0
                    in_json = {'page_id':page_ids, 'crisis_percentage':crisis_percentage}
                    crisis_json['in'].append(in_json)
            #===============================>vk
            if media=="VK":
                print "pages['vk_page_ids']---->>>>>", pages['vk_page_ids']
                for page_ids in pages['vk_page_ids']:
                    vk_comments = db_connection_formal.vk_wall_comments.find({'user_id':page_ids}).count()
                    vk_comment = db_connection_formal.vk_wall_comments.find({'user_id':page_ids})
                    vk_comments_count =0
                    for cmmnts in vk_comment:
                        #from_response =urllib.urlopen('https://api.vk.com/method/users.get?user_id='+str(cmmnts['from_id'])+'&fields=photo')
                        #from_response_formated = json.loads(from_response.read())
                        if cmmnts['from_id']==page_ids:
                            msg = cmmnts['text'].lower()
                            msg_list = msg.split(' ')
                            commn = list(set(msg_list).intersection(crisis_words))
                            if len(commn)!=0:
                                vk_comments_count+=1
                    if vk_comments>0:
                        crisis_percentage = (float(vk_comments_count)/float(vk_comments))*100
                        if crisis_percentage>70:
                            insert_json = {
                        "to" : user_id,
                        "text" : "crisis occur for the vkontakte page named as "+uname,
                        "action_url" : "http://sociabyte.com/notifications",
                        "status" : "unseen",
                        "timestamp":datetime.datetime.now()
                    }
                            id_file_db = db_connection_formal.notifications.insert(insert_json)
                            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
                            urllib.urlopen(url_download)
                    else:
                        crisis_percentage = 0
                    vk_json = {'page_id':page_ids, 'crisis_percentage': crisis_percentage}
                    crisis_json['vk'].append(vk_json)
                print "crisis done vk"
            #===============================>utube
            if media=="UTUBE":
                for page_ids in pages['utube_page_ids']:
                    utube_posts = db_connection_formal.utube_video_comments.find({'yt_channelId':page_ids}).count()
                    utube_post = db_connection_formal.utube_video_comments.find({'yt_channelId':page_ids})
                    utube_posts_count=0
                    for post_utube in utube_post:
                        msg = post_utube['content'].lower()
                        msg_list = msg.split(' ')
                        commn = list(set(msg_list).intersection(crisis_words))
                        if len(commn)!=0:
                            utube_posts_count+=1
                    if utube_posts>0:
                        crisis_percentage = (float(utube_posts_count)/float(utube_posts))*100
                        if crisis_percentage>70:
                            insert_json = {
                        "to" : user_id,
                        "text" : "crisis occur for the youtube channel named as "+uname,
                        "action_url" : "http://sociabyte.com/notifications",
                        "status" : "unseen",
                        "timestamp":datetime.datetime.now()
                    }
                            id_file_db = db_connection_formal.notifications.insert(insert_json)
                            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
                            urllib.urlopen(url_download)
                    else:
                        crisis_percentage = 0
                    utube_json = {'page_id':page_ids, 'crisis_percentage':crisis_percentage}
                    crisis_json['utube'].append(utube_json)
            #================================================projects
            if media=="PRO":
                project_list = []
                projects = db_connection_formal.brands.find({'_id':ObjectId(pages['brand_id'])}, {'_id':0, 'projects':1})
                for project in projects:
                    project_list = project['projects']
                for projct in project_list:
                    article_count = db_connection_formal.rss_response.find({'project_id':projct}).count()
                    crisis_article = db_connection_formal.rss_response.find({'project_id':projct, 'keywords':{'$in':crisis_words}}).count()
                    if article_count>0:
                        crisis_percentage = (float(crisis_article)/float(article_count))*100
                        if crisis_percentage>70:
                            insert_json = {
                        "to" : user_id,
                        "text" : "crisis occur for the project named as "+uname,
                        "action_url" : "http://sociabyte.com/notifications",
                        "status" : "unseen",
                        "timestamp":datetime.datetime.now()
                    }
                            id_file_db = db_connection_formal.notifications.insert(insert_json)
                            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
                            urllib.urlopen(url_download)
                    else:
                        crisis_percentage = 0
                    project_json = {'project_id':projct, 'crisis_percentage':crisis_percentage}
                    crisis_json['projects'].append(project_json)
            page_counter+=1
            db_connection_formal.brand_crisis.remove({'brand_id':pages['brand_id']})
            db_connection_formal.brand_crisis.insert(crisis_json)
def detectServiceReq(db_connection_formal, page_ids_list_formal, media, brand_id):
    import urllib
    import json
    page_counter = 0
    for pages in page_ids_list_formal:
        crisis_words = []
        crisis_words_response = db_connection_formal.brands.find({'_id':ObjectId(pages['brand_id'])}, {'_id':0, 'service_requests':1})
        for res in crisis_words_response:
            crisis_words_case_insensitive = res['service_requests']
        for words in crisis_words_case_insensitive:
            crisis_words.append(words.lower())
        print "crisis_words==================>>>",crisis_words
        fb_posts = []
        fb_cmmnt = []
        if media=="FB":
            for page_ids in pages['fb_page_ids']:
                print "fb_page_ids=========>>>", page_ids
                fb_post_count = db_connection_formal.fb_post_details.find({'page_id':page_ids})
                page_details = db_connection_formal.fb_accounts.find({'page_id':page_ids}, {'_id':0})
                for page_detail in page_details:
                    page_name = page_detail['username']
                for posts in fb_post_count:
                    print "posts===========>>>>", posts
                    if posts['from_id']!=str(page_ids):
                        msg = posts['message'].lower()
                        print "massge==============>>>", msg
                        msg_list = msg.split(' ')
                        commn = list(set(msg_list).intersection(crisis_words))
                        if len(commn)!=0:
                            service_request_json = {
                                "brand_id":'',
                                "page_id":'',
                                "platform":'',
                                "message":'',
                                "from_id":'',
                                "from_name":'',
                                "profile_image":'',
                                'date':'',
                                'status':'notserviced',
                                'page_name':page_name,
                                'type':'',
                                'assinged_person_id':'',
                                'assinged_person_name':''
                            }
                            service_request_json["brand_id"]=pages['brand_id']
                            service_request_json["page_id"]=page_ids
                            service_request_json["platform"]='fb'
                            service_request_json["message"]=posts['message']
                            service_request_json["from_id"]=posts['from_id']
                            service_request_json["from_name"]=posts['from_name']
                            service_request_json["profile_image"]='http://graph.facebook.com/'+posts['from_id']+'/picture'
                            service_request_json['date']=posts['created_time']
                            service_request_json['type'] = 'post'
                            db_connection_formal.service_request.save(service_request_json.copy())
                fb_comnt_count = db_connection_formal.fb_comments.find({'page_id':page_ids})
                for posts in fb_comnt_count:
                    if posts['from']['id']!=page_ids:
                        msg = posts['message'].lower()
                        msg_list = msg.split(' ')
                        commn = list(set(msg_list).intersection(crisis_words))
                        if len(commn)!=0:
                            service_request_json = {
                                "brand_id":'',
                                "page_id":'',
                                "platform":'',
                                "message":'',
                                "from_id":'',
                                "from_name":'',
                                "profile_image":'',
                                'date':'',
                                'status':'notserviced',
                                'page_name':page_name,
                                'type':'',
                                'assinged_person_id':'',
                                'assinged_person_name':''
                            }
                            service_request_json["brand_id"]=pages['brand_id']
                            service_request_json["page_id"]=page_ids
                            service_request_json["platform"]='fb'
                            service_request_json["message"]=posts['message']
                            service_request_json["from_id"]=posts['from']['id']
                            service_request_json["from_name"]=posts['from']['name']
                            service_request_json["profile_image"]='http://graph.facebook.com/'+posts['from']['id']+'/picture'
                            service_request_json['date']=posts['created_time']
                            service_request_json['type'] = 'comment'
                            db_connection_formal.service_request.save(service_request_json.copy())

        #print "pages==========>>>",brand_overall_json_formal[page_counter]['posts']['fb']
        #==========================>tw
        tw_tweets = []
        if media=="TW":
            for page_ids in pages['tw_page_ids']:
                tw_posts_count = db_connection_formal.tw_mentions.find({'user_id':page_ids})
                page_details = db_connection_formal.tw_users.find({'user_id':page_ids}, {'_id':0})
                for page_detail in page_details:
                    page_name = page_detail['screen_name']
                for tweets in tw_posts_count:
                    if tweets['user']['id_str']!=page_ids:
                        msg = tweets['text'].lower()
                        msg_list = msg.split(' ')
                        commn = list(set(msg_list).intersection(crisis_words))
                        if len(commn)!=0:
                            service_request_json = {
                                "brand_id":'',
                                "page_id":'',
                                "platform":'',
                                "message":'',
                                "from_id":'',
                                "from_name":'',
                                "profile_image":'',
                                'date':'',
                                'status':'notserviced',
                                'page_name':page_name,
                                'type':'',
                                'assinged_person_id':'',
                                'assinged_person_name':''
                            }
                            service_request_json["brand_id"]=str(pages['brand_id'])
                            service_request_json["page_id"]=page_ids
                            service_request_json["platform"]='tw'
                            service_request_json["message"]=tweets['text']
                            service_request_json["from_id"]=tweets['user']['id_str']
                            service_request_json["from_name"]=tweets['user']['name']
                            service_request_json["profile_image"]=tweets['user']['profile_image_url']
                            service_request_json['date']=tweets['created_at']
                            service_request_json['type'] = 'post'
                            db_connection_formal.service_request.save(service_request_json.copy())

        #===========================>inst
        if media=="INST":
            inst_posts = []
            for page_ids in pages['inst_page_ids']:
                inst_cmnts_count = db_connection_formal.inst_comments.find({'user_id':page_ids})
                page_details = db_connection_formal.tw_users.find({'ins_id':page_ids}, {'_id':0})
                for page_detail in page_details:
                    page_name = page_detail['username']
                for cmnts in inst_cmnts_count:
                    if cmnts['from']['id']!=page_ids:
                        msg = cmnts['text'].lower()
                        msg_list = msg.split(' ')
                        commn = list(set(msg_list).intersection(crisis_words))
                        if len(commn)!=0:
                            inst_json={
                                "brand_id":pages['brand_id'],
                                "page_id":page_ids,
                                "platform":'inst',
                                "message":cmnts['text'],
                                "from_id":cmnts['from']['id'],
                                "from_name":cmnts['from']['username'],
                                "profile_image":cmnts['from']['profile_picture'],
                                'date':cmnts['created_time'],
                                'status':'notserviced',
                                'page_name':page_name,
                                'type':'post',
                                'assinged_person_id':'',
                                'assinged_person_name':''
                            }
                            db_connection_formal.service_request.save(inst_json.copy())

        #=============================>LI
        if media=="IN":
            li_posts = []
            for page_ids in pages['in_page_ids']:
                in_comments_count = db_connection_formal.li_comments.find({'id':page_ids})
                page_details = db_connection_formal.tw_users.find({'id':int(page_ids)}, {'_id':0})
                for page_detail in page_details:
                    page_name = page_detail['name']
                for shares in in_comments_count:
                    try:
                        if shares['person']['id']!=page_ids:
                            msg = shares['comment'].lower()
                            msg_list = msg.split(' ')
                            commn = list(set(msg_list).intersection(crisis_words))
                            if len(commn)!=0:
                                in_json={
                                    "brand_id":pages['brand_id'],
                                    "page_id":page_ids,
                                    "platform":'in',
                                    "message":shares['comment'],
                                    "from_id":shares['person']['id'],
                                    "from_name":shares['from']['username'],
                                    "profile_image":shares['from']['profile_picture'],
                                    'date':shares['timestamp'],
                                    'status':'notserviced',
                                    'page_name':page_name,
                                    'type':'post',
                                'assinged_person_id':'',
                                'assinged_person_name':''
                                }
                                db_connection_formal.service_request.save(in_json.copy())
                    except:
                        pass
        #===============================>vk
        if media=="VK":
            vk_posts = []
            for page_ids in pages['vk_page_ids']:
                vk_comments_count = db_connection_formal.vk_wall_comments.find({'user_id':page_ids})
                page_details = db_connection_formal.tw_users.find({'uid':int(page_ids)}, {'_id':0})
                for page_detail in page_details:
                    page_name = page_detail['first_name']+ ' '+page_detail['last_name']
                for cmmnts in vk_comments_count:
                    from_response =urllib.urlopen('https://api.vk.com/method/users.get?user_id='+str(cmmnts['from_id'])+'&fields=photo')
                    from_response_formated = json.loads(from_response.read())
                    if cmmnts['from_id']!=page_ids:
                        msg = cmmnts['text'].lower()
                        msg_list = msg.split(' ')
                        commn = list(set(msg_list).intersection(crisis_words))
                        if len(commn)!=0:
                            vk_json={
                                "brand_id":pages['brand_id'],
                                "page_id":page_ids,
                                "platform":'vk',
                                "message":cmmnts['text'],
                                "from_id":cmmnts['from_id'],
                                "from_name":from_response_formated['response'][0]['first_name']+from_response_formated['response'][0]['last_name'],
                                "profile_image":from_response_formated['response'][0]['photo'],
                                'date':cmmnts['date'],
                                'status':'notserviced',
                                'page_name':page_name,
                                'type':'post',
                                'assinged_person_id':'',
                                'assinged_person_name':''
                            }
                            db_connection_formal.service_request.save(vk_json.copy())
        #===============================>utube
        if media=="UTUBE":
            utube_posts = []
            for page_ids in pages['utube_page_ids']:
                utube_posts_count = db_connection_formal.utube_video_comments.find({'yt_channelId':page_ids})
                access_token_response = db_connection_formal.brands.find({'associated_accounts.utube_accounts.page_id':str(page_ids), '_id':ObjectId(brand_id)}, {'associated_accounts.utube_accounts.name':1,  '_id':0})
                for acc in access_token_response:
                    try:
                        print 'acc======>>', acc
                        page_name = acc['associated_accounts']['utube_accounts'][0]['name']
                    except:
                        pass

                for post_utube in utube_posts_count:
                    msg = post_utube['content'].lower()
                    msg_list = msg.split(' ')
                    commn = list(set(msg_list).intersection(crisis_words))
                    if len(commn)!=0:
                        utube_json={
                            "brand_id":pages['brand_id'],
                            "page_id":page_ids,
                            "platform":'utube',
                            "message":post_utube['content'],
                            "from_id":post_utube['yt_googlePlusUserId'],
                            "from_name":post_utube['author']['name'],
                            "profile_image":'https://plus.google.com/'+post_utube['yt_googlePlusUserId'],
                            'date':post_utube['published'],
                                'status':'notserviced',
                                'page_name':page_name,
                                'type':'post',
                                'assinged_person_id':'',
                                'assinged_person_name':''
                        }
                        db_connection_formal.service_request.save(utube_json.copy())
        page_counter+=1
    print "page counter ", page_counter
def executorOverall(db_conn_formal, brand_id, checker, user_id, uname, social_media):
    db_connection = db_conn_formal.sociabyte
    brand_overall_json = createBrandJson(db_connection, brand_id)
    brand_overall_json_final, page_id_list = totalPages(db_connection, brand_overall_json, brand_id)
    if checker=='brand':
        print "in baler brand"
        find_closest_trend(db_connection, brand_id)
        totalFeedback(db_connection, brand_overall_json_final, page_id_list)
        totalFollower(db_connection, brand_overall_json_final, page_id_list)
        totalPosts(db_connection, brand_overall_json_final, page_id_list)
        #print "brand_overall_json_final====================>", brand_overall_json_final
        topPosts(db_connection, brand_overall_json_final, page_id_list)###############################from this onwards check for error
        sentimentScore(db_connection, brand_overall_json_final, page_id_list)
        reachPlatform(db_connection, brand_overall_json_final, page_id_list)
        pageInfo(db_connection, brand_overall_json_final, page_id_list)
        db_connection.brand_overall.remove({'brand_id' : ObjectId(brand_id)})
        print "Brand Overall===+", brand_overall_json_final
        db_connection.brand_overall.insert(brand_overall_json_final)
        calcualteCrisis(db_connection, page_id_list, user_id, uname, social_media)
        detectServiceReq(db_connection, page_id_list,social_media, brand_id)
    else:
        print "in baler except"
        calcualteCrisis(db_connection, page_id_list, user_id, uname, social_media)
        detectServiceReq(db_connection, page_id_list, social_media, brand_id)
    #print "brand_overall_json_final====================>", brand_overall_json_final







        