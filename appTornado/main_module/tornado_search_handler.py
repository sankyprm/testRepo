import tornado.web
import tornado.gen
import tornado.httpclient
import json
from twython import Twython
import zmq
import sys
import urllib
import urllib2
import datetime
import feedparser
import pymongo
from pymongo import MongoClient,DESCENDING
from bson.objectid import ObjectId
import time
import itertools
from multiprocessing import Pool
import facebook
from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError
from tornado.httpclient import AsyncHTTPClient
from gplus_module.gplus_services import Gplus_Services as GPLUS
import global_settings

from engagement import EngagementHandler
from scheduling import SchedulingHandler

db_conn = global_settings.db_conn
final_feed_list = []
last_element = []
final_tweet_list = []
final_vk_list = []
final_facebook_list = []


class ProjectDataFetchTemp(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self):
        result = yield tornado.gen.Task(self.search_key)
        self.finish()

    def search_key(self, callback):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Credentials", "true")
        self.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        self.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")
        import ast
        import project_module.project_services_temp as project_services_temp
        category = ast.literal_eval(self.get_argument('category'))
        sources = ast.literal_eval(self.get_argument('sources'))
        countries = ast.literal_eval(self.get_argument('countries'))
        project_id = str(self.get_argument('project_id'))
        account_type =str(self.get_argument('account_type'))
        brand_id = str(self.get_argument('brandId'))
        #print (category), '===', '====', (sources), '====', (countries), '====', (brand_id)
        list_for_google = []
        arg_list_final1 = []
        arg_list_final2 = []
        for pair in category:
            cat = pair['name']
            query = pair['query']
            label = pair ['labels']  #array
            languages = pair['languages'] #array
            temp_list = [cat, query, label, languages, project_id]
            list_for_google.append(temp_list)
            arg_list1 = [query, project_id, account_type, cat, languages, label]
            arg_list2 = [query, project_id, account_type, cat, countries, sources, languages, label]
            arg_list_final1.append(arg_list1)
            arg_list_final2.append(arg_list2)
        project_json_temp = {
            'project_id':project_id,
            'key':category,
            'account_type':account_type,
            'city':["kolkata","delhi"],  #needs to be replaced by actual city
            'country':countries,#and country name passed through the query parameter
            'status':'inactive'
        }
        db_conn.sociabyte.project_temp.insert(project_json_temp)
        for arg_val in list_for_google:
            try:
                project_services_temp.FeedUrlFetch(arg_val)
            except:
                pass
        """try:
            process_pool = Pool(processes=4)
            process_pool.map(project_services_temp.dataFeed, arg_list_final1)  # other sources should be fetched like this
            process_pool.close()
            process_pool.join()
        except:
            pass"""
        # code to fetch the rss feed link
        #for temp_val in arg_list_final2:
        #    project_services_temp.dataFeedRss(temp_val)
        try:
            process_pool = Pool(processes=4)
            process_pool.map(project_services_temp.dataFeedRss, arg_list_final2)  # other sources should be fetched like this
            process_pool.close()
            process_pool.join()
        except:
            pass
        access_token_response = db_conn.sociabyte.brands.find({'_id':ObjectId(brand_id)}, {"associated_accounts.tw_accounts.token":1, "associated_accounts.tw_accounts.tokenSecret":1, "_id":0})
        counter = 0
        token = ''
        for acc in access_token_response:
            for acc_unit in acc['associated_accounts']['tw_accounts']:
                token = acc['associated_accounts']['tw_accounts'][counter]['token']
                token_secret = acc['associated_accounts']['tw_accounts'][counter]['tokenSecret']
                counter = counter+1
        if token!='':
            tw_handle = Twython('pMBYng3YBKIyXoJaERynDMMqG', 'cRTbD3zxZcVMyI5bKsHHclmxzpxIDrmG93vTXQ0sezAdN3WDRu', token, token_secret)
        else:
            tw_handle = Twython('inBAC5z1EiCt8k6z32dqw', 's8IK3HT8cWReb2YUSV8kTQvf3qANXwKZhCZORw5XAVg', '177512438-v68R6ZeW9dTZB6PzZgSZSvjnIfAI4rscDioxIcfs', 'oVw6W90eXspQrlAbib6QivHKJ2nJ6nf8dPOK7DxVic')
        for tw_arg in list_for_google:
            try:
                project_services_temp.fetchTwitterFeeds(tw_arg, tw_handle) #place the keys properly
            except:
                pass
            try:
                project_services_temp.fetchVkFeeds(tw_arg) #place the keys properly
            except:
                pass
        sendDict = {
            'acc_id':project_id,
            'code':'project'
        }
        sendDict = json.dumps(sendDict)
        port = '5558'
        context = zmq.Context()
        print ("Connecting to server...")
        socket = context.socket(zmq.PUB)
        socket.bind("tcp://*:%s" % port)
        print ("Sending request ")
        a=0
        while a<2:
            socket.send_multipart(['',sendDict])
            print "send successfull"
            a+=1
            time.sleep(1)
        #  Get the reply.
        socket.close()
        checker = 1
        while checker:
            check_status = db_conn.sociabyte.project_temp.find({'project_id':project_id}, {'status':1, '_id':0})
            for status in check_status:
                final_status = status['status']
            if final_status == 'active':
                send_dictionary = project_services_temp.doSomeCalculation(project_id, db_conn)
                dumped_Json = json.dumps(send_dictionary)
                self.write(dumped_Json)
                checker=0
            if final_status == 'inactive':
                time.sleep(1)
                continue
        db_conn.sociabyte.project_temp.remove({'project_id':str(project_id)})
        db_conn.sociabyte.feed_link_temp.remove({'project_id':str(project_id)})
        db_conn.sociabyte.rss_response_temp.remove({'project_id':str(project_id)})
        callback()
class ProjectDataFetchParmanent(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        self.projectId = yield tornado.gen.Task(self.getId)
        self.set_status(200)
        self.finish()
        result = yield tornado.gen.Task(self.search_key_final)
    def getId(self, callback):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Credentials", "true")
        self.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        self.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")
        project_id = self.get_argument('project_id')
        callback(project_id)
    def search_key_final(self, callback):
        import project_module.project_services_parm as project_services_parm
        from brand_overall import brand_overall_daily as brand_overall
        project_id = self.projectId
        project_details_response = db_conn.sociabyte.projects.find({'_id':ObjectId(project_id)}, {'_id':0})
        for project in project_details_response:
            #print project
            category = project['category']
            sources_existing = project['existing_sources']['sources']
            sources_new = project['sources']
            social_media = project['social_media']
            countries = project['country']
            brand_id = project['owner_brands']
            user_id = project['user_id']
            project_name = project['project_name']
            mentions_limit = project['mentions_limit']
            mention_fetched = project['mentions_fetched']
        #print "sources=================>>>>",sources_existing
        remaining_article = mentions_limit - mention_fetched
        # newly added
        feed_dict = {
        "0" : "http://www.marketwatch.com/rss/video/wsj.asp?value=wsj-subsection&query=Worth+It",
        "1" : "http://www.marketwatch.com/rss/video/wsj.asp?value=wsj-subsection&query=Digits",
        "2" : "http://feeds.wsjonline.com/wsj/video/personal-finance/feed",
        "3" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-subsection&query=Taxes",
        "4" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-package&query=A-Heds&count=30",
        "5" : "http://feeds.wsjonline.com/wsj/video/arts-and-entertainment/feed",
        "6" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-playlist&query=WSJ%20Cafe",
        "7" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-subsection&query=WSJ%20Magazine",
        "8" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-subsection&query=Opinion+Journal&fields=all&count=30",
        "9" : "http://feeds.wsjonline.com/wsj/video/real-estate/feed",
        "10" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-package&query=Wendy%20Bounds",
        "11" : "http://feeds.wsjonline.com/wsj/video/small-business/feed",
        "12" : "http://feeds.wsjonline.com/wsj/video/journal-reports/feed",
        "13" : "http://feeds.wsjonline.com/wsj/video/special_reports",
        "14" : "http://feeds.wsjonline.com/wsj/podcast_tech_diary",
        "15" : "http://www.wsj.com/xml/rss/3_7143.xml",
        "16" : "http://feeds.wsjonline.com/wsj/podcast_the_middle_seat",
        "17" : "http://www.marketwatch.com/podcast/Market%20Update",
        "18" : "http://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
        "19" : "http://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
        "20" : "http://www.reuters.com/news/world",
        "21" : "http://www.reuters.com/finance",
        "22" : "http://www.reuters.com/news/technology",
        "23" : "http://www.reuters.com/finance/deals",
        "24" : "http://feeds.foxnews.com/foxnews/world",
        "25" : "http://feeds.foxnews.com/foxnews/scitech",
        "26" : "http://feeds.foxnews.com/foxnews/business",
        "27" : "https://www.reddit.com/r/worldnews/",
        "28" : "https://www.reddit.com/r/business/",
        "29" : "https://www.reddit.com/r/small_business/",
        "30" : "http://rss.cnn.com/rss/edition_americas.rss",
        "31" : "http://rss.cnn.com/rss/money_news_international.rss",
        "32" : "http://rss.cnn.com/rss/edition_technology.rss",
        "33" : "http://www.wsj.com/xml/rss/3_7041.xml",
        "34" : "http://www.wsj.com/xml/rss/3_7085.xml",
        "35" : "http://www.wsj.com/xml/rss/3_7014.xml",
        "36" : "http://www.wsj.com/xml/rss/3_7031.xml",
        "37" : "http://www.wsj.com/xml/rss/3_7455.xml",
        "38" : "http://www.wsj.com/xml/rss/3_7201.xml",
        "39" : "http://blogs.wsj.com/bankruptcy/feed/",
        "40" : "http://blogs.wsj.com/capitaljournal/feed/",
        "41" : "http://blogs.wsj.com/chinarealtime/feed/",
        "42" : "http://blogs.wsj.com/corruption-currents/feed/",
        "43" : "http://feeds.wsjonline.com/wsj/dailyfix/feed/",
        "44" : "http://feeds.wsjonline.com/wsj/deals/feed",
        "45" : "http://feeds.wsjonline.com/wsj/developments/feed",
        "46" : "http://feeds.wsjonline.com/wsj/biztech/feed",
        "47" : "http://blogs.wsj.com/dispatch/feed/",
        "48" : "http://blogs.wsj.com/drivers-seat/feed/",
        "49" : "http://blogs.wsj.com/exchange/feed/",
        "50" : "http://blogs.wsj.com/wealth-manager/feed/",
        "51" : "http://blogs.wsj.com/management/feed/",
        "52" : "http://blogs.wsj.com/health/feed/",
        "53" : "http://blogs.wsj.com/speakeasy/category/style/feed/",
        "54" : "http://blogs.wsj.com/hong-kong/feed/",
        "55" : "http://blogs.wsj.com/ideas-market/feed/",
        "56" : "http://blogs.wsj.com/indiarealtime/feed/",
        "57" : "http://blogs.wsj.com/japanrealtime/feed/",
        "58" : "http://blogs.wsj.com/juggle/feed/",
        "59" : "http://blogs.wsj.com/korearealtime/feed/",
        "60" : "http://blogs.wsj.com/law/feed/",
        "61" : "http://blogs.wsj.com/marketbeat/feed/",
        "62" : "http://blogs.wsj.com/metropolis/feed/",
        "63" : "http://blogs.wsj.com/middleseat/feed/",
        "64" : "http://mobilefeeds.wsj.com/xml/feed/v2/3_8461.rss",
        "65" : "http://blogs.wsj.com/emergingeurope/feed/",
        "66" : "http://feeds.wsjonline.com/wsj/numbersguy/feed",
        "67" : "http://blogs.wsj.com/wine/feed/",
        "68" : "http://blogs.wsj.com/photojournal/feed/",
        "69" : "http://blogs.wsj.com/privateequity/feed/",
        "70" : "http://blogs.wsj.com/brussels/feed/",
        "71" : "http://feeds.wsjonline.com/wsj/economics/feed",
        "72" : "http://blogs.wsj.com/scene/feed/",
        "73" : "http://blogs.wsj.com/simonnixon/feed/",
        "74" : "http://feeds2.feedburner.com/wsj/speakeasy/feed/",
        "75" : "http://blogs.wsj.com/venturecapital/feed/",
        "76" : "http://blogs.wsj.com/washwire/feed/",
        "77" : "http://blogs.wsj.com/wealth/feed/",
        "78" : "http://blogs.wsj.com/puzzle/feed/",
        "79" : "http://feeds.wsjonline.com/wsj/video/most-popular/feed",
        "80" : "http://feeds.wsjonline.com/wsj/video/most-popular-this-week/feed",
        "81" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-section&query=The%20News%20Hub&count=30",
        "82" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-subsection&query=AM%20Report%20%26%20PM%20Report&count=30",
        "83" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-subsection&query=Hot%20Stocks&count=30",
        "84" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-subsection&query=News%20Hub%20Extra&count=30",
        "85" : "http://feeds.wsjonline.com/wsj/video/news/feed",
        "86" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-subsection&query=Page+One&fields=all&count=30",
        "87" : "http://feeds.wsjonline.com/wsj/video/politics/feed",
        "88" : "http://feeds.wsjonline.com/wsj/video/world/feed",
        "89" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-package&query=The%20Middle%20East",
        "90" : "http://feeds.wsjonline.com/wsj/video/business/feed",
        "91" : "http://feeds.wsjonline.com/wsj/video/economy/feed",
        "92" : "http://feeds.wsjonline.com/wsj/video/health/feed",
        "93" : "http://feeds.wsjonline.com/wsj/video/law/feed",
        "94" : "http://feeds.wsjonline.com/wsj/video/media-and-marketing/feed",
        "95" : "http://feeds.wsjonline.com/wsj/video/environmental-capital/feed/",
        "96" : "http://feeds.wsjonline.com/wsj/video/management/feed",
        "97" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-subsection&query=Viewpoints",
        "98" : "http://feeds.wsjonline.com/wsj/video/business-insight/feed",
        "99" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-section&query=Leadership",
        "100" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-section&query=The+Big+Interview",
        "101" : "http://feeds.wsjonline.com/wsj/video/markets/feed",
        "102" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-subsection&query=Markets+Hub&fields=all&count=30",
        "103" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-subsection&query=Heard%20on%20the%20Street",
        "104" : "http://feeds.wsjonline.com/wsj/video/tech/feed",
        "105" : "http://feeds.wsjonline.com/wsj/video/andy-jordan/feed",
        "106" : "http://www.marketwatch.com/rss/video/wsj.asp?type=wsj-package&query=Science",
        "107" : "http://feeds.wsjonline.com/wsj/video/funds/feed",
        "108" : "http://feeds.wsjonline.com/wsj/video/life-and-style/feed",
        "109" : "http://feeds.wsjonline.com/wsj/video/autos/feed",
        "110" : "http://feeds.wsjonline.com/wsj/video/books/feed",
        "111" : "http://feeds.wsjonline.com/wsj/video/fashion/feed",
        "112" : "http://feeds.wsjonline.com/wsj/video/food-and-drink/feed",
        "113" : "http://feeds.wsjonline.com/wsj/video/sports/feed",
        "114" : "http://feeds.wsjonline.com/wsj/video/travel/feed",
        "115" : "http://feeds.wsjonline.com/wsj/video/opinion/feed",
        "116" : "http://feeds.wsjonline.com/wsj/video/careers/feed",
        "117" : "http://www.marketwatch.com/mw2/rss/video/wsj.asp?type=wsj-package&query=Weekend+Conversations&count=30",
        "118" : "http://www.marketwatch.com/mw2/mediarss/wsj/copyflow.asp?type=wsj-package&query=New+York&count=30",
        "119" : "http://www.marketwatch.com/mw2/mediarss/wsj/copyflow.asp?type=wsj-package&query=Bay+Area&count=30",
        "120" : "http://feeds.feedburner.com/wsj/podcast_the_sports_retort/",
        "121" : "http://feeds.feedburner.com/YourMoneyNowTheSmallBusinessReport",
        "122" : "http://www.marketwatch.com/mw2/feeds/podcast/podcast.asp?count=10&doctype=116&column=The+Wall+Street+Journal+Tech+Talk",
        "123" : "http://www.marketwatch.com/feeds/podcast/podcast.asp?count=10&doctype=116&column=The+Wall+Street+Journal+This+Morning",
        "124" : "http://feeds.wsjonline.com/wsj/podcast_your_money_matters",
        "125" : "http://feeds.wsjonline.com/wsj/podcast_wall_street_journal_whats_news",
        "126" : "http://feeds.marketwatch.com/marketwatch/podcasts/marketwatchmorningstocktalk",
        "127" : "http://feeds.feedburner.com/marketwatch/podcasts/MarketWatchNewsBreak",
        "128" : "http://feeds.marketwatch.com/marketwatch/podcasts/moneymarketsmore",
        "129" : "http://feeds.wsjonline.com/wsj/podcast_the_news_hub",
        "130" : "http://feeds.wsjonline.com/wsj/podcast_watching_your_wallet",
        "131" : "http://feeds.barrons.info/barrons/podcasts/thisweekinbarrons",
        "132" : "http://www.marketwatch.com/podcast/Wall%20Street%20Journal%20Editors%27%20Picks",
        "133" : "http://rss.nytimes.com/services/xml/rss/nyt/SmallBusiness.xml",
        "134" : "http://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
        "135" : "http://feeds.huffingtonpost.com/c/35496/f/677045/index.rss",
        "136" : "http://feeds.huffingtonpost.com/c/35496/f/677497/index.rss",
        "137" : "http://feeds.huffingtonpost.com/c/35496/f/677555/index.rss",
        "138" : "http://feeds.huffingtonpost.com/c/35496/f/677102/index.rss",
        "139" : "http://feeds.huffingtonpost.com/c/35496/f/677554/index.rss",
        "140" : "http://feeds.huffingtonpost.com/c/35496/f/677612/index.rss",
        "141" : "http://feeds.huffingtonpost.com/c/35496/f/677048/index.rss",
        "142" : "http://feeds.huffingtonpost.com/c/35496/f/677500/index.rss",
        "143" : "http://feeds.huffingtonpost.com/c/35496/f/677558/index.rss",
        "144" : "http://feeds.huffingtonpost.com/c/35496/f/677090/index.rss",
        "145" : "http://feeds.huffingtonpost.com/c/35496/f/677542/index.rss",
        "146" : "http://feeds.huffingtonpost.com/c/35496/f/677600/index.rss",
        "147" : "http://feeds.huffingtonpost.com/c/35496/f/677097/index.rss",
        "148" : "http://feeds.huffingtonpost.com/c/35496/f/677549/index.rss",
        "149" : "http://feeds.huffingtonpost.com/c/35496/f/677607/index.rss",
        "150" : "http://www.prnewswire.com/news-releases/business-technology-latest-news/"
        }
        l = ['http://timesofindia.indiatimes.com/rss.cms','http://timesofindia.indiatimes.com/rssfeedstopstories.cms','http://timesofindia.indiatimes.com/rssfeeds/296589292.cms','http://timesofindia.indiatimes.com/rssfeeds/7098551.cms','http://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms','http://timesofindia.indiatimes.com/rssfeeds/1898055.cms','http://timesofindia.indiatimes.com/rssfeeds/4719161.cms','http://timesofindia.indiatimes.com/rssfeeds/4719148.cms','http://timesofindia.indiatimes.com/rssfeeds/3908999.cms','http://timesofindia.indiatimes.com/rssfeeds/-2128672765.cms','http://timesofindia.indiatimes.com/rssfeeds/2647163.cms','http://timesofindia.indiatimes.com/rssfeeds/5880659.cms','http://timesofindia.indiatimes.com/rssfeeds/2886704.cms','http://timesofindia.indiatimes.com/rssfeeds/1081479906.cms','http://www.portugalvisitor.com/rssfeed.xml','http://www1.expatica.com/pt/common/rss_feeds.html','http://world.einnews.com/country/portugal']
        feed_list = [v for k,v in feed_dict.items()]
        all_sources = l+feed_list+sources_existing+[links['feed_link'] for links in sources_new]

        #print('sources_new==============================',sources_new)
        #print('all_sources==============================',all_sources)
        list_for_google = []
        arg_list_final_mainstream = []
        arg_list_final_social = []
        for pair in category:
            if pair!=None:
                arg_list1 = [pair, all_sources, countries, project_id, remaining_article]
                arg_list2 = [pair, countries, project_id, remaining_article]
                arg_list_final_mainstream.append(arg_list1)
                arg_list_final_social.append(arg_list2)
            else:
                pass
        article_count = 0
        #try:
        article_count_single = 0
        if remaining_article>0:
            try:
                #print("========== arg_list_final_mainstream ====================",arg_list_final_mainstream)
                process_pool = Pool(processes=4)
                count_list_rss= 0
                #for arg in arg_list_final2:
                count_list_rss = process_pool.map(project_services_parm.dataFeedRss, arg_list_final_mainstream)  # other sources should be fetched like this
                #project_services_parm.dataFeedRss(arg)
                process_pool.close()
                process_pool.join()
                #======update alexa metrices

                #=========================
                #print "count list rss", count_list_rss
                article_count = article_count+sum(count_list_rss)
                article_count_single = sum(count_list_rss)
            except:
                pass
            remaining_article = remaining_article-article_count_single

        #alexa data crawling
        alexa_status = project_services_parm.alexaDataCrawling()
        print(alexa_status)

        access_token_response = db_conn.sociabyte.brands.find({'_id':ObjectId(brand_id)}, {"associated_accounts.tw_accounts.token":1, "associated_accounts.tw_accounts.tokenSecret":1, "_id":0})
        counter = 0
        token = ''
        for acc in access_token_response:
            for acc_unit in acc['associated_accounts']['tw_accounts']:
                token = acc['associated_accounts']['tw_accounts'][counter]['token']
                token_secret = acc['associated_accounts']['tw_accounts'][counter]['tokenSecret']
                counter = counter+1
        if token!='':
            tw_handle = Twython('Qwu1NiiV2uzGp3Hj0UqD1CKo2', '6CiVt3lDPnD4qOaccPOxpcPeTNkZ8h8ljelavK79xKsqpgEEYF', token, token_secret)
        else:
            tw_handle = Twython('inBAC5z1EiCt8k6z32dqw', 's8IK3HT8cWReb2YUSV8kTQvf3qANXwKZhCZORw5XAVg', '177512438-v68R6ZeW9dTZB6PzZgSZSvjnIfAI4rscDioxIcfs', 'oVw6W90eXspQrlAbib6QivHKJ2nJ6nf8dPOK7DxVic')
        for tw_arg in arg_list_final_social:
            article_count_single = 0
            if remaining_article>0:
                count_tw=0
                try:
                    count_tw = project_services_parm.fetchTwitterFeeds(tw_arg, tw_handle) #place the keys properly
                    article_count = article_count+count_tw
                except:
                    pass
                article_count_single = count_tw
            remaining_article = remaining_article-article_count_single
            article_count_single = 0
            if remaining_article>0:
                #try:
                print 'call to vk====>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
                count_vk = project_services_parm.fetchVkFeeds(tw_arg) #place the keys properly
                article_count = article_count+count_vk
                article_count_single = count_vk
                #except:
                #    print 'in vk call except=================================================>>>>>>>>>>>>>>>>>>>>>>>>>>>'
                #    pass
                remaining_article = remaining_article-article_count_single
        #print "Article_count=========>>>>", article_count
        article_json = {
            "project_id":project_id,
            "date":datetime.datetime.now(),
            "mention_count":article_count
        }
        db_conn.sociabyte.project_artical_count.insert(article_json)
        sendDict = {
            'acc_id':str(project_id),
            'code':'projectParm'
        }
        sendDict = json.dumps(sendDict)
        port = '5575'
        context = zmq.Context()
        print ("Connecting to server...")
        socket = context.socket(zmq.PUB)
        socket.bind("tcp://*:%s" % port)
        print ("Sending request ")
        a=0
        while a<2:
            socket.send_multipart(['',sendDict])
            print "send successfull"
            a+=1
            time.sleep(1)
        #  Get the reply.
        socket.close()
        db_conn.sociabyte.projects.update({'_id':ObjectId(project_id)}, {'$set':{'mentions_fetched':mention_fetched, 'status':'Active'}})
        checker = 1
        while checker:
            check_status = db_conn.sociabyte.projects.find({'_id':ObjectId(project_id)}, {'final_fetch':1, '_id':0})
            for status in check_status:
                final_status = status['final_fetch']
            if final_status == 'complete':
                insert_json = {
                    "to" : user_id,
                    "text" : "Project created with name: "+project_name,
                    "action_url" : "http://sociabyte.com/project/"+str(project_id)+"/dashboard",
                    "status" : "unseen",
                    "timestamp":datetime.datetime.now(),
                    "type":'add_project',
                    "project_id":str(project_id)
                }
                id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
                url_download ="http://sociabyte.com/notification/"+str(id_file_db)
                urllib.urlopen(url_download)
                #self.write('ok')
                checker=0
            if final_status == 'incomplete':
                time.sleep(1)
                continue

        #print 'okkk'
        #brand_overall.executorOverall(db_conn, brand_id, 'brand', "NR", "NR", "PRO")
        remaining_article = mentions_limit-(article_count+mention_fetched)
        if remaining_article<0:
            mention_fetched = mentions_limit
        else:
            mention_fetched = mention_fetched+article_count
        """except:
            #print "inside project error"
            user_check_res = db_conn.sociabyte.user.find({'_id':(user_id)})
            for users in user_check_res:
                user_type = users['access_type']['user_type']
                try:
                    members = [members['member_id'] for members in users['members'] if members['status'] is not 'Invited']
                except:
                    pass
                try:
                    owner_id = users['membership_details']['owner_id']
                except:
                    owner_id = None
            if user_type=='Administrator':
                members.append(user_id)
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'remaining_articles':mentions_limit}}, multi=True)
            else:
                admin_user_res = db_conn.sociabyte.user.find({'_id':(owner_id)})
                members = [members['member_id'] for members in admin_user_res['members'] if members['status'] is not 'Invited']
                members.append(owner_id)
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.remaining_articles':mentions_limit, "access_type.project_limit":1}}, multi=True)
            db_conn.sociabyte.brands.update({'_id':ObjectId(brand_id)}, {'$pull':{'projects':project_id}})
            db_conn.sociabyte.projects.remove({'_id':ObjectId(project_id)})
            insert_json = {
                        "to" : user_id,
                        "text" : "Some error occured for project with name: "+project_name,
                        "action_url" : "http://sociabyte.com/"+brand_id+"/projects",
                        "status" : "unseen",
                        "timestamp":datetime.datetime.now()
                    }
            id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
            urllib.urlopen(url_download)"""
        callback()

class FbServices(tornado.web.RequestHandler):
    account_id = ''
    user_id = ''
    @tornado.web.asynchronous
    @tornado.gen.coroutine

    def get(self):
        from brand_overall import brand_overall_daily as brand_overall
        #print('Hii I am in FB Services')
        self.account_id, self.user_id, self.brand_id, self.page_name = yield tornado.gen.Task(self.getId)
        brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "FB")
        self.set_status(200)
        self.finish()
        fb_user = yield tornado.gen.Task(self.fbFunction)
        #print "====>>>>", fb_user
        print('I am completed')
        if fb_user=="okk":
            try:
                check_default_res = db_conn.sociabyte.brands.find({'_id':ObjectId(self.brand_id)}, {'default_page':1})
                for res in check_default_res:
                    default_page = res['default_page']
            except:
                default_page = None
            if default_page==None:
                db_conn.sociabyte.brands.update({'_id':ObjectId(self.brand_id)}, {"$set":{'default_page':{'page_id':self.account_id, 'page_type':'facebook'}}})

            #----change social media limit

            user_check_res = db_conn.sociabyte.user.find({'_id':(self.user_id)})
            for users in user_check_res:
                user_type = users['access_type']['user_type']
                try:
                    members = [members['member_id'] for members in users['members'] if members['status'] is not 'Invited']
                except:
                    pass
                try:
                    owner_id = users['membership_details']['owner_id']
                except:
                    owner_id = None
            if user_type=='Administrator':
                members.append(self.user_id)
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
            else:
                admin_user_res = db_conn.sociabyte.user.find({'_id':(owner_id)})
                members = [members['member_id'] for members in admin_user_res['members'] if members['status'] is not 'Invited']
                members.append(owner_id)
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
            #----- end change social media limit

            #print "I am in okk"
            insert_json = {
        "to" : self.user_id,
        "text" : "The facebook page named as "+self.page_name+" has been successfully added",
        "action_url" : "http://sociabyte.com/brands/"+str(self.brand_id)+"/"+str(self.account_id)+"/facebook/overall",
        "status" : "unseen",
        "timestamp":datetime.datetime.now(),
        'type':'social_media',
        'details': {
           'userid':self.user_id,
           'brandid':self.brand_id,
           'pageid':self.account_id,
           'platform':'facebook'
        }
    }
            id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
            urllib.urlopen(url_download)
        elif fb_user=="new page":
            pass
        else:
            insert_json = {
        "to" : self.user_id,
        "text" : "Some error occured for facebook page with name: "+self.page_name,
        "action_url" : "http://sociabyte.com/"+str(self.brand_id)+"/dashboard",
        "status" : "unseen",
        "timestamp":datetime.datetime.now()
    }
            id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
            urllib.urlopen(url_download)
    def getId(self, callback):
        idaccount = self.get_argument('fb_account_id')
        idUser = self.get_argument('uname')
        idBrand = self.get_argument('brandId')
        page_name = self.get_argument('page_name')
        callback([idaccount, idUser, idBrand, page_name])

    def fbFunction(self, callback):
        import fb_module.fb_services as fb_services
        from brand_overall import brand_overall_daily as brand_overall
        fb_account_id = self.account_id#self.get_argument('fb_account_id')
        db_sociabyte = db_conn.sociabyte
        acc_list = []
        acc_list_res= db_sociabyte.fb_accounts.find({}, {'page_id':1, '_id':0})
        for acc in acc_list_res:
            acc_list.append(acc['page_id'])
        if self.account_id not in acc_list:
            res=db_conn.sociabyte.user.find({'_id':self.user_id},{'brands':1})
            from bson.objectid import ObjectId
            ids=[ObjectId(a) for a in res[0]['brands']]
            access_token_result = db_conn.sociabyte.brands.find({"associated_accounts.fb_accounts.page_id":fb_account_id,'_id':{'$in':ids}}, {"associated_accounts.fb_accounts.token":1,"associated_accounts.fb_accounts.page_id":1, "_id": 0, "associated_accounts.fb_accounts.name":1})
            token = ''
            token_secret = ''
            #print(access_token_result[0]['associated_accounts']['tw_accounts'])
            for accounts in access_token_result[0]['associated_accounts']['fb_accounts']:
                if accounts['page_id']==fb_account_id:
                    access_token = accounts['token']
                    name=accounts['name']
                    #print(name)
            try:
                fb_handle = facebook.GraphAPI(access_token)
                response = fb_services.userAccountsInfo(db_sociabyte, fb_account_id, fb_handle, access_token, self.brand_id)
                response2 = fb_services.pagePosts(fb_account_id, db_sociabyte, fb_handle, access_token, name)
                response3 = fb_services.comments_fetch(fb_account_id, db_sociabyte, fb_handle)
                response4 = fb_services.like_fetch(fb_account_id, db_sociabyte, fb_handle)
                response5 = fb_services.insight_fetch(fb_account_id, db_sociabyte, fb_handle)
                fb_services.topContributor(fb_account_id, db_sociabyte, fb_handle)
                comment_id_list=[]
                sendDict = {
                    'acc_id':fb_account_id,
                    'page_name':self.page_name,
                    'brand_id':self.brand_id,
                    'user_id':self.user_id,
                    'code':'fb'
                }
                sendDict = json.dumps(sendDict)
                port = '5559'
                context = zmq.Context()
                print ("Connecting to server...")
                socket = context.socket(zmq.PUB)
                socket.bind("tcp://*:%s" % port)
                print ("Sending request ")
                a=0

                while a<2:
                    socket.send_multipart(['',sendDict])
                    print "send successfull"
                    a+=1
                    time.sleep(1)
                #  Get the reply.
                socket.close()
                try:
                    check_default_res = db_conn.sociabyte.brands.find({'_id':ObjectId(self.brand_id)}, {'default_page':1})
                    for res in check_default_res:
                        default_page = res['default_page']
                except:
                    default_page = None
                if default_page==None:
                    db_conn.sociabyte.brands.update({'_id':ObjectId(self.brand_id)}, {"$set":{'default_page':{'page_id':self.account_id, 'page_type':'facebook'}}})

                #----change social media limit

                user_check_res = db_conn.sociabyte.user.find({'_id':(self.user_id)})
                for users in user_check_res:
                    user_type = users['access_type']['user_type']
                    try:
                        members = [members['member_id'] for members in users['members'] if members['status'] is not 'Invited']
                    except:
                        pass
                    try:
                        owner_id = users['membership_details']['owner_id']
                    except:
                        owner_id = None
                if user_type=='Administrator':
                    members.append(self.user_id)
                    db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
                else:
                    admin_user_res = db_conn.sociabyte.user.find({'_id':(owner_id)})
                    members = [members['member_id'] for members in admin_user_res['members'] if members['status'] is not 'Invited']
                    members.append(owner_id)
                    db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
                #----- end change social media limit
                brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "FB")
                callback("new page")
            except:
                db_conn.sociabyte.brands.update({'_id':ObjectId(self.brand_id)}, {"$pull":{"associated_accounts.fb_accounts":{"page_id":fb_account_id}}})
                db_conn.sociabyte.fb_accounts.remove({"page_id":self.brand_id})
                brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "FB")
                callback("not ok")
        else:
            db_conn.sociabyte.fb_accounts.update({'page_id':fb_account_id}, {'$addToSet':{'brand_id':self.brand_id}})
            brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "FB")
            callback("okk")

class TwServices(tornado.web.RequestHandler):
    account_id = ''
    user_id = ''
    @tornado.web.asynchronous
    @tornado.gen.coroutine

    def get(self):
        from brand_overall import brand_overall_daily as brand_overall
        self.account_id, self.user_id, self.brand_id, self.page_name = yield tornado.gen.Task(self.getId)
        brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "TW")
        self.set_status(200)
        self.finish()
        tw_user = yield tornado.gen.Task(self.twFunction)
        print(" ============================ tw_user =============================",tw_user)
        if tw_user=="okk":
            #----change social media limit

            user_check_res = db_conn.sociabyte.user.find({'_id':(self.user_id)})
            for users in user_check_res:
                user_type = users['access_type']['user_type']
                try:
                    members = [members['member_id'] for members in users['members'] if members['status'] is not 'Invited']
                except:
                    pass
                try:
                    owner_id = users['membership_details']['owner_id']
                except:
                    owner_id = None
            if user_type=='Administrator':
                members.append(self.user_id)
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
            else:
                admin_user_res = db_conn.sociabyte.user.find({'_id':(owner_id)})
                members = [members['member_id'] for members in admin_user_res['members'] if members['status'] is not 'Invited']
                members.append(owner_id)
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
            #----- end change social media limit
            insert_json = {
        "to" : self.user_id,
        "text" : "The twiter page named as "+self.page_name+" has been successfully added",
        "action_url" : "http://sociabyte.com/brands/"+str(self.brand_id)+"/"+str(self.account_id)+"/twitter/overall",
        "status" : "unseen",
        "timestamp":datetime.datetime.now(),
        'type':'social_media',
        'details': {
           'userid':self.user_id,
           'brandid':self.brand_id,
           'pageid':self.account_id,
           'platform':'twitter'
        }
    }
            id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
            urllib.urlopen(url_download)
        elif tw_user=="new page":
            pass
        else:
            insert_json = {
        "to" : self.user_id,
        "text" : "Some error occured for twitter page named as: "+self.page_name,
        "action_url" : "http://sociabyte.com/"+str(self.brand_id)+"/dashboard",
        "status" : "unseen",
        "timestamp":datetime.datetime.now()
    }
            id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
            urllib.urlopen(url_download)


    def getId(self, callback):
        idaccount = self.get_argument('tw_account_id')
        idUser = self.get_argument('uname')
        idBrand = self.get_argument('brandId')
        page_name = self.get_argument('page_name')
        callback([idaccount, idUser, idBrand, page_name])

    def twFunction(self, callback):
        import tw_module.tw_services as tw_services
        from brand_overall import brand_overall_daily as brand_overall
        tw_user_id = self.account_id
        db_sociabyte = db_conn.sociabyte
        acc_list = []
        acc_list_res= db_sociabyte.tw_users.find({}, {'id':1, '_id':0})
        for acc in acc_list_res:
            acc_list.append(str(acc['id']))
        if self.account_id not in acc_list:
            res=db_conn.sociabyte.user.find({'_id':self.user_id},{'brands':1})
            from bson.objectid import ObjectId
            ids=[ObjectId(a) for a in res[0]['brands']]

            access_token_result = db_conn.sociabyte.brands.find({"associated_accounts.tw_accounts.page_id":tw_user_id,'_id':{'$in':ids}}, {"associated_accounts.tw_accounts.token":1,"associated_accounts.tw_accounts.page_id":1, "_id": 0, "associated_accounts.tw_accounts.tokenSecret":1,"associated_accounts.tw_accounts.name":1})

            token = ''
            token_secret = ''
            #print(access_token_result[0]['associated_accounts']['tw_accounts'])
            for accounts in access_token_result[0]['associated_accounts']['tw_accounts']:
                if accounts['page_id']==tw_user_id:
                    token = accounts['token']
                    token_secret = accounts['tokenSecret']
                    name=accounts['name']
                    #print(name)
            #try:
                #tw_handle = Twython('inBAC5z1EiCt8k6z32dqw', 's8IK3HT8cWReb2YUSV8kTQvf3qANXwKZhCZORw5XAVg', '177512438-v68R6ZeW9dTZB6PzZgSZSvjnIfAI4rscDioxIcfs', 'oVw6W90eXspQrlAbib6QivHKJ2nJ6nf8dPOK7DxVic')
            tw_handle = Twython('Qwu1NiiV2uzGp3Hj0UqD1CKo2', '6CiVt3lDPnD4qOaccPOxpcPeTNkZ8h8ljelavK79xKsqpgEEYF', token, token_secret)
            tw_services.userMentionTimeline(tw_user_id, tw_handle, db_sociabyte)
            #print "mention============\n"
            tw_services.userTimeline(tw_user_id, tw_handle, db_sociabyte)
            #print "userTimeline============\n"
            tw_services.retweetsFetch(tw_user_id, tw_handle, db_sociabyte)
            #print "userTimeline============\n"
            #tw_services.trendsFetch(tw_user_id, tw_handle, db_sociabyte, self.brand_id)
            tw_services.trendsFetch(tw_user_id, tw_handle, db_sociabyte)
            tw_services.followersFetch(tw_user_id, tw_handle, db_sociabyte)
            tw_services.userInfo(tw_user_id, tw_handle, db_sociabyte, self.brand_id)
            tw_services.followerDetails(tw_user_id, tw_handle, db_sociabyte)
            tw_services.genderClassifier(tw_user_id, db_sociabyte)
            tw_services.dailyReachCalculator(tw_user_id, db_sociabyte)
            comment_id_list=[]
            sendDict = {
                'acc_id':tw_user_id,
                'page_name':self.page_name,
                'brand_id':self.brand_id,
                'user_id':self.user_id,
                'code':'tw'
            }
            sendDict = json.dumps(sendDict)
            port = '5560'
            context = zmq.Context()
            print ("Connecting to server...")
            socket = context.socket(zmq.PUB)
            socket.bind("tcp://*:%s" % port)
            print ("Sending request ")
            a=0

            while a<2:
                socket.send_multipart(['',sendDict])
                print "send successfull"
                a+=1
                time.sleep(1)
            #  Get the reply.
            socket.close()
            #----change social media limit

            user_check_res = db_conn.sociabyte.user.find({'_id':(self.user_id)})
            for users in user_check_res:
                user_type = users['access_type']['user_type']
                try:
                    members = [members['member_id'] for members in users['members'] if members['status'] is not 'Invited']
                except:
                    pass
                try:
                    owner_id = users['membership_details']['owner_id']
                except:
                    owner_id = None
            if user_type=='Administrator':
                members.append(self.user_id)
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
            else:
                admin_user_res = db_conn.sociabyte.user.find({'_id':(owner_id)})
                members = [members['member_id'] for members in admin_user_res['members'] if members['status'] is not 'Invited']
                members.append(owner_id)
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
            #----- end change social media limit
            brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "TW")
            callback("new page")
            '''except:
                db_conn.sociabyte.brands.update({'_id':ObjectId(self.brand_id)}, {"$pull":{"associated_accounts.tw_accounts":{"page_id":tw_user_id}}})
                db_conn.sociabyte.tw_users.remove({"id":int(tw_user_id)})
                brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "TW")
                callback("not ok")'''
        else:
            db_conn.sociabyte.tw_users.update({'id':int(tw_user_id)}, {'$addToSet':{'brand_id':self.brand_id}})
            brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "TW")
            callback("okk")


class InstServices(tornado.web.RequestHandler):
    account_id = ''
    user_id = ''
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        from brand_overall import brand_overall_daily as brand_overall
        self.account_id, self.user_id, self.brand_id, self.page_name = yield tornado.gen.Task(self.getId)
        brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "INST")
        self.set_status(200)
        self.finish()
        inst_user = yield tornado.gen.Task(self.instFunction)
        if inst_user=="okk":
            #----change social media limit

            user_check_res = db_conn.sociabyte.user.find({'_id':(self.user_id)})
            for users in user_check_res:
                user_type = users['access_type']['user_type']
                try:
                    members = [members['member_id'] for members in users['members'] if members['status'] is not 'Invited']
                except:
                    pass
                try:
                    owner_id = users['membership_details']['owner_id']
                except:
                    owner_id = None
            if user_type=='Administrator':
                members.append(self.user_id)
                #print 'members======>>>>', members
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
            else:
                admin_user_res = db_conn.sociabyte.user.find({'_id':(owner_id)})
                members = [members['member_id'] for members in admin_user_res['members'] if members['status'] is not 'Invited']
                members.append(owner_id)
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
            #----- end change social media limit
            insert_json = {
        "to" : self.user_id,
        "text" : "The instagram page named as "+self.page_name+" has been successfully added",
        "action_url" : "http://sociabyte.com/brands/"+str(self.brand_id)+"/"+str(self.account_id)+"/instagram/overall",
        "status" : "unseen",
        "timestamp":datetime.datetime.now(),
        'type':'social_media',
        'details': {
           'userid':self.user_id,
           'brandid':self.brand_id,
           'pageid':self.account_id,
           'platform':'instagram'
        }
    }
            id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
            urllib.urlopen(url_download)
        elif inst_user=='new page':
            pass
        else:
            insert_json = {
        "to" : self.user_id,
        "text" : "Some error occured for instagram page with id: "+self.account_id,
        "action_url" : "http://sociabyte.com/"+str(self.brand_id)+"/dashboard",
        "status" : "unseen",
        "timestamp":datetime.datetime.now()
    }
            id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
            urllib.urlopen(url_download)
    def getId(self, callback):
        idaccount = self.get_argument('inst_account_id')
        idUser = self.get_argument('uname')
        idBrand = self.get_argument('brandId')
        page_name = self.get_argument('page_name')
        callback([idaccount, idUser, idBrand, page_name])
    def instFunction(self, callback):
        import inst_module.inst_services as inst_services
        from brand_overall import brand_overall_daily as brand_overall
        inst_user_id = self.account_id
        db_sociabyte = db_conn.sociabyte
        acc_list = []
        acc_list_res= db_sociabyte.inst_user.find({}, {'ins_id':1, '_id':0})
        for acc in acc_list_res:
            acc_list.append(str(acc['ins_id']))
        if self.account_id not in acc_list:
            access_token_response = db_sociabyte.brands.find({"associated_accounts.ins_accounts.page_id":inst_user_id, '_id':ObjectId(self.brand_id)}, {"associated_accounts.ins_accounts.token":1, "associated_accounts.ins_accounts.page_id":1, "_id":0})
            counter = 0
            for acc in access_token_response:
                for acc_unit in acc['associated_accounts']['ins_accounts']:
                    if acc['associated_accounts']['ins_accounts'][counter]['page_id']==inst_user_id:
                        access_token = acc['associated_accounts']['ins_accounts'][counter]['token']
                    else:
                        pass
                    counter = counter+1
            try:
                inst_handle = InstagramAPI(access_token=access_token)
                inst_services.userFetch(inst_user_id, db_sociabyte, access_token, urllib, json, self.brand_id)
                inst_services.recentMediaFetch(inst_user_id, db_sociabyte, access_token, urllib, json)
                media_id_list = []
                media_id_list_response = db_sociabyte.inst_media.find({"user_id":inst_user_id}, {"_id":0, 'media_id':1})
                for media_id in media_id_list_response:
                    media_id_list.append(media_id)
                inst_services.commentFetch(inst_user_id, db_sociabyte, access_token, urllib, json, media_id_list)
                inst_services.likeFetch(inst_user_id, db_sociabyte, access_token, urllib, json, media_id_list)
                inst_services.followedByFetch(inst_user_id, db_sociabyte, access_token, urllib, json)
                inst_services.genderClassifier(inst_user_id, db_sociabyte)
                inst_services.dailyReachCalculator(inst_user_id, db_sociabyte)
                comment_id_list=[]
                sendDict = {
                    'acc_id':inst_user_id,
                    'page_name':self.page_name,
                    'brand_id':self.brand_id,
                    'user_id':self.user_id,
                    'code':'inst'
                }
                sendDict = json.dumps(sendDict)
                port = '5561'
                context = zmq.Context()
                print ("Connecting to server...")
                socket = context.socket(zmq.PUB)
                socket.bind("tcp://*:%s" % port)
                print ("Sending request ")
                a=0

                while a<2:
                    socket.send_multipart(['',sendDict])
                    print "send successfull"
                    a+=1
                    time.sleep(1)
                #  Get the reply.
                socket.close()
                #----change social media limit

                user_check_res = db_conn.sociabyte.user.find({'_id':(self.user_id)})
                for users in user_check_res:
                    user_type = users['access_type']['user_type']
                    try:
                        members = [members['member_id'] for members in users['members'] if members['status'] is not 'Invited']
                    except:
                        pass
                    try:
                        owner_id = users['membership_details']['owner_id']
                    except:
                        owner_id = None
                #print "members============>>>>>>", members
                if user_type=='Administrator':
                    members.append(self.user_id)
                    db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
                else:
                    admin_user_res = db_conn.sociabyte.user.find({'_id':(owner_id)})
                    members = [members['member_id'] for members in admin_user_res['members'] if members['status'] is not 'Invited']
                    members.append(owner_id)
                    db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
                #----- end change social media limit
                brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "INST")
                callback("new page")
            except:
                db_conn.sociabyte.brands.update({'_id':ObjectId(self.brand_id)}, {"$pull":{"associated_accounts.ins_accounts":{"page_id":inst_user_id}}})
                db_conn.sociabyte.inst_user.remove({"ins_id":inst_user_id})
                brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "INST")
                callback("not ok")
        else:
            db_conn.sociabyte.inst_user.update({'ins_id':inst_user_id}, {"$addToSet":{'brand_id':self.brand_id}})
            brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "INST")
            callback("okk")

class VkServices(tornado.web.RequestHandler):
    account_id = ''
    user_id = ''
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        from brand_overall import brand_overall_daily as brand_overall
        self.account_id, self.user_id, self.brand_id, self.page_name = yield tornado.gen.Task(self.getId)
        brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "VK")
        #print 'vk===========================>>>>'
        self.set_status(200)
        self.finish()
        vk_user = yield tornado.gen.Task(self.vkFunction)
        if vk_user=="okk":
            #----change social media limit

            user_check_res = db_conn.sociabyte.user.find({'_id':(self.user_id)})
            for users in user_check_res:
                user_type = users['access_type']['user_type']
                try:
                    members = [members['member_id'] for members in users['members'] if members['status'] is not 'Invited']
                except:
                    pass
                try:
                    owner_id = users['membership_details']['owner_id']
                except:
                    owner_id = None
            if user_type=='Administrator':
                members.append(self.user_id)
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
            else:
                admin_user_res = db_conn.sociabyte.user.find({'_id':(owner_id)})
                members = [members['member_id'] for members in admin_user_res['members'] if members['status'] is not 'Invited']
                members.append(owner_id)
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
            #----- end change social media limit
            insert_json = {
        "to" : self.user_id,
        "text" : "The vkontakte page named as "+self.page_name+" has been successfully added",
        "action_url" : "http://sociabyte.com/brands/"+str(self.brand_id)+"/"+str(self.account_id)+"/vkontakte/overall",
        "status" : "unseen",
        "timestamp":datetime.datetime.now(),
        'type':'social_media',
        'details': {
           'userid':self.user_id,
           'brandid':self.brand_id,
           'pageid':self.account_id,
           'platform':'vkontakte'
        }
    }
            id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
            urllib.urlopen(url_download)
        elif vk_user=='new page':
            insert_json = {
        "to" : self.user_id,
        "text" : "The vkontakte page named as "+self.page_name+" has been successfully added",
        "action_url" : "http://sociabyte.com/"+str(self.brand_id)+"/dashboard",
        "status" : "unseen",
        "timestamp":datetime.datetime.now(),
        'type':'social_media',
        'details': {
           'userid':self.user_id,
           'brandid':self.brand_id,
           'pageid':self.account_id,
           'platform':'vkontakte'
        }
    }
            id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
            urllib.urlopen(url_download)
        else:
            insert_json = {
        "to" : self.user_id,
        "text" : "Some error occured for vkontakte page with id: "+self.account_id,
        "action_url" : "http://sociabyte.com/"+str(self.brand_id)+"/dashboard",
        "status" : "unseen",
        "timestamp":datetime.datetime.now()
    }
            id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
            urllib.urlopen(url_download)
    def getId(self, callback):
        idaccount = self.get_argument('vk_account_id')
        idUser = self.get_argument('uname')
        idBrand = self.get_argument('brandId')
        page_name = self.get_argument('page_name')
        callback([idaccount, idUser, idBrand, page_name])
    def vkFunction(self, callback):
        import vk_module.vk_services as vk_services
        from brand_overall import brand_overall_daily as brand_overall
        vk_user_id = self.account_id
        #print "vk account======"+vk_user_id+"\n"
        db_sociabyte = db_conn.sociabyte
        acc_list = []
        acc_list_res= db_sociabyte.vk_user.find({}, {'uid':1, '_id':0})
        for acc in acc_list_res:
            acc_list.append(str(acc['uid']))
        if self.account_id not in acc_list:
            access_token_response = db_sociabyte.brands.find({'associated_accounts.vk_accounts.page_id':str(vk_user_id), '_id':ObjectId(self.brand_id)}, {'associated_accounts.vk_accounts.token':1, 'associated_accounts.vk_accounts.page_id':1, '_id':0})
            counter = 0
            for acc in access_token_response:
                #print acc, "===="
                for acc_unit in acc['associated_accounts']['vk_accounts']:
                    if acc['associated_accounts']['vk_accounts'][counter]['page_id']==vk_user_id:
                        access_token = acc['associated_accounts']['vk_accounts'][counter]['token']
                    else:
                        pass
                    counter = counter+1
            try:
                #print "vk started"
                vk_services.userFetch(vk_user_id, access_token, db_sociabyte, json, urllib, self.brand_id)
                vk_services.userFollowers(vk_user_id, access_token, db_sociabyte, json, urllib)
                vk_services.userFriends(vk_user_id, access_token, db_sociabyte, json, urllib)
                vk_services.getWallPosts(vk_user_id, access_token, db_sociabyte, json, urllib)
                vk_services.getWallComments(vk_user_id, access_token, db_sociabyte, json, urllib)
                vk_services.getWallLikes(vk_user_id, access_token, db_sociabyte, json, urllib)
                vk_services.genderClassifier(vk_user_id, db_sociabyte)
                vk_services.dailyReachCalculator(vk_user_id, db_sociabyte)
                vk_services.agegroupCalculator(vk_user_id, db_sociabyte)
                #url="http://predictyoursales.com:3000/fetchCompleted?type=brand&code=vk&id="+vk_user_id+"&userid="+self.user_id
                #resp = urllib.urlopen(url)
                db_sociabyte.brands.update({'associated_accounts.vk_accounts.page_id':str(vk_user_id), '_id':ObjectId(self.brand_id)}, {'$set':{'associated_accounts.vk_accounts.$.status':'active'}})
                #print "vk completed"
                #----change social media limit

                user_check_res = db_conn.sociabyte.user.find({'_id':(self.user_id)})
                for users in user_check_res:
                    user_type = users['access_type']['user_type']
                    try:
                        members = [members['member_id'] for members in users['members'] if members['status'] is not 'Invited']
                    except:
                        pass
                    try:
                        owner_id = users['membership_details']['owner_id']
                    except:
                        owner_id = None
                if user_type=='Administrator':
                    members.append(self.user_id)
                    db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
                else:
                    admin_user_res = db_conn.sociabyte.user.find({'_id':(owner_id)})
                    members = [members['member_id'] for members in admin_user_res['members'] if members['status'] is not 'Invited']
                    members.append(owner_id)
                    db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
                #----- end change social media limit
                brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "VK")
                callback("new page")
            except:
                db_conn.sociabyte.brands.update({'_id':ObjectId(self.brand_id)}, {"$pull":{"associated_accounts.vk_accounts":{"page_id":str(vk_user_id)}}})
                db_conn.sociabyte.vk_user.remove({"uid":int(vk_user_id)})
                brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "VK")
                callback("not ok")
        else:
            db_conn.sociabyte.vk_user.update({'uid':int(vk_user_id)}, {'$addToSet':{'brand_id':self.brand_id}})
            brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "VK")
            callback("okk")
class gplusServices(tornado.web.RequestHandler):
    account_id = ''
    user_id= ''
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        self.account_id, self.user_id = yield tornado.gen.Task(self.getId)
        inst_user = yield tornado.gen.Task(self.gplusFunction)
        #print(inst_user)
        #print('I am done in Gplus')
        self.set_status(200)
        self.finish()
    def getId(self, callback):
        idaccount = self.get_argument('gplus_account_id')
        idUser = self.get_argument('uname')
        callback([idaccount, idUser])
    def gplusFunction(self, callback):
        from brand_overall import brand_overall_daily as brand_overall
        #self.write('i Am in Vk module')
        gp_user_id = self.account_id
        user_id=self.user_id
        db_sociabyte = db_conn.sociabyte
        gserve=GPLUS({'client_id':gp_user_id,'userid':user_id},db_sociabyte)
        res=gserve.runFirst()
        #url="http://predictyoursales.com:3000/fetchCompleted?type=brand&code=gp&id="+gp_user_id+"&userid="+self.user_id
        #resp = urllib.urlopen(url)
        brand_overall.executorOverall(db_conn)
        callback(res)
class youtubeServices(tornado.web.RequestHandler):
    account_id = ''
    user_id = ''
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        from brand_overall import brand_overall_daily as brand_overall
        self.account_id, self.user_id, self.brand_id, self.page_name = yield tornado.gen.Task(self.getId)
        brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "UTUBE")
        self.set_status(200)
        self.finish()
        yt_user = yield tornado.gen.Task(self.youTubeFunction)
        if yt_user=="okk":
            #----change social media limit

            user_check_res = db_conn.sociabyte.user.find({'_id':(self.user_id)})
            for users in user_check_res:
                user_type = users['access_type']['user_type']
                try:
                    members = [members['member_id'] for members in users['members'] if members['status'] is not 'Invited']
                except:
                    pass
                try:
                    owner_id = users['membership_details']['owner_id']
                except:
                    owner_id = None
            if user_type=='Administrator':
                members.append(self.user_id)
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
            else:
                admin_user_res = db_conn.sociabyte.user.find({'_id':(owner_id)})
                members = [members['member_id'] for members in admin_user_res['members'] if members['status'] is not 'Invited']
                members.append(owner_id)
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
            #----- end change social media limit
            insert_json = {
        "to" : self.user_id,
        "text" : "The youtube page named as "+self.page_name+" has been successfully added",
        "action_url" : "http://sociabyte.com/brands/"+str(self.brand_id)+"/"+str(self.account_id)+"/youtube/overall",
        "status" : "unseen",
        "timestamp":datetime.datetime.now(),
        'type':'social_media',
        'details': {
           'userid':self.user_id,
           'brandid':self.brand_id,
           'pageid':self.account_id,
           'platform':'youtube'
        }
    }
            id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
            urllib.urlopen(url_download)
        elif yt_user=='new page':
            pass
        elif yt_user=="not page":
            insert_json = {
        "to" : self.user_id,
        "text" : "This channel has no video, so can't be added: channel id- "+self.account_id,
        "action_url" : "http://sociabyte.com/"+str(self.brand_id)+"/dashboard",
        "status" : "unseen",
        "timestamp":datetime.datetime.now()
    }
            id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
            urllib.urlopen(url_download)
        else:
            insert_json = {
        "to" : self.user_id,
        "text" : "Some error occured for youtube page with id: "+self.account_id,
        "action_url" : "http://sociabyte.com/"+str(self.brand_id)+"/dashboard",
        "status" : "unseen",
        "timestamp":datetime.datetime.now()
    }
            id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
            urllib.urlopen(url_download)
    def getId(self, callback):
        idaccount = self.get_argument('utube_account_id')
        idUser = self.get_argument('uname')
        idBrand = self.get_argument('brandId')
        page_name = self.get_argument('page_name')
        callback([idaccount, idUser, idBrand, page_name])
    def youTubeFunction(self, callback):
        import utube_module.utube_services as utube_services
        from brand_overall import brand_overall_daily as brand_overall
        yt_user_id = self.account_id
        db_sociabyte = db_conn.sociabyte
        acc_list = []
        acc_list_res= db_sociabyte.utube_basic_details.find({}, {'channel_id':1, '_id':0})
        for acc in acc_list_res:
            acc_list.append(str(acc['channel_id']))
        if self.account_id not in acc_list:
            access_token_response = db_sociabyte.brands.find({'associated_accounts.utube_accounts.page_id':str(yt_user_id), '_id':ObjectId(self.brand_id)}, {'associated_accounts.utube_accounts.accessToken':1, 'associated_accounts.utube_accounts.page_id':1, 'associated_accounts.utube_accounts.refreshToken':1, '_id':0})
            counter = 0
            for acc in access_token_response:
                #print acc, "===="
                for acc_unit in acc['associated_accounts']['utube_accounts']:
                    if acc['associated_accounts']['utube_accounts'][counter]['page_id']==str(yt_user_id):
                        access_token = acc['associated_accounts']['utube_accounts'][counter]['accessToken']
                        refresh_token = acc['associated_accounts']['utube_accounts'][counter]['refreshToken']
                    else:
                        pass
                    counter = counter+1
            if utube_services.verifyToken(access_token, urllib, json)==False:
                utube_services.updateToken(yt_user_id, access_token, refresh_token, urllib, json, db_sociabyte)
            else:
                pass
            try:
                utube_services.basicDataFetch(yt_user_id, access_token, db_sociabyte, urllib, json, self.brand_id)
                utube_services.channelGrowth(yt_user_id, access_token, db_sociabyte, urllib, json)
                utube_services.insightTrafficSource(yt_user_id, access_token, db_sociabyte, urllib, json)
                utube_services.activePlatform(yt_user_id, access_token, db_sociabyte, urllib, json)
                utube_services.insightFromMedia(yt_user_id, access_token, db_sociabyte, urllib, json)
                utube_services.insightPlayback(yt_user_id, access_token, db_sociabyte, urllib, json)
                video_idList = utube_services.getAllVideos(yt_user_id, access_token, db_sociabyte, urllib, json)
                #print "video_idList", video_idList
                if len(video_idList)==0:
                    db_conn.sociabyte.brands.update({'_id':ObjectId(self.brand_id)}, {"$pull":{"associated_accounts.utube_accounts":{"page_id":self.account_id}}})
                    db_conn.sociabyte.utube_basic_details.remove({"channel_id":self.account_id})
                    db_conn.sociabyte.utube_active_platform.remove({"channel_id":self.account_id})
                    db_conn.sociabyte.utube_channel_growth.remove({"channel_id":self.account_id})
                    db_conn.sociabyte.utube_channel_videos.remove({"channelId":self.account_id})
                    db_conn.sociabyte.utube_insight_playback.remove({"channel_id":self.account_id})
                    db_conn.sociabyte.utube_insight_tsType.remove({"channel_id":self.account_id})
                    db_conn.sociabyte.utube_traffic_source.remove({"channel_id":self.account_id})
                    brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "UTUBE")
                    callback("not page")
                else:
                    #print "in else"
                    utube_services.getAllComments(yt_user_id, access_token, db_sociabyte, urllib, json, video_idList)
                    utube_services.topVideos(yt_user_id, access_token, db_sociabyte, urllib, json)
                    sendDict = {
                        'acc_id':yt_user_id,
                        'page_name':self.page_name,
                        'brand_id':self.brand_id,
                        'user_id':self.user_id,
                        'code':'utube'
                    }
                    sendDict = json.dumps(sendDict)
                    port = '5564'
                    context = zmq.Context()
                    print ("Connecting to server...")
                    socket = context.socket(zmq.PUB)
                    socket.bind("tcp://*:%s" % port)
                    print ("Sending request ")
                    a=0

                    while a<2:
                        socket.send_multipart(['',sendDict])
                        print "send successfull"
                        a+=1
                        time.sleep(1)
                    #  Get the reply.
                    socket.close()
                    #----change social media limit

                user_check_res = db_conn.sociabyte.user.find({'_id':(self.user_id)})
                for users in user_check_res:
                    user_type = users['access_type']['user_type']
                    try:
                        members = [members['member_id'] for members in users['members'] if members['status'] is not 'Invited']
                    except:
                        pass
                    try:
                        owner_id = users['membership_details']['owner_id']
                    except:
                        owner_id = None
                if user_type=='Administrator':
                    members.append(self.user_id)
                    db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
                else:
                    admin_user_res = db_conn.sociabyte.user.find({'_id':(owner_id)})
                    members = [members['member_id'] for members in admin_user_res['members'] if members['status'] is not 'Invited']
                    members.append(owner_id)
                    db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
                #----- end change social media limit
                    brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "UTUBE")
                    callback("new page")
            except:
                db_conn.sociabyte.brands.update({'_id':ObjectId(self.brand_id)}, {"$pull":{"associated_accounts.utube_accounts":{"page_id":str(yt_user_id)}}})
                db_conn.sociabyte.utube_basic_details.remove({"channel_id":str(yt_user_id)})
                brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "UTUBE")
                callback("not ok")
        else:
            db_conn.sociabyte.utube_basic_details.update({'channel_id':str(yt_user_id)}, {'$addToSet':{'brand_id':self.brand_id}})
            brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "UTUBE")
            callback("okk")
class linkedInServices(tornado.web.RequestHandler):
    account_id = ''
    user_id = ''
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        from brand_overall import brand_overall_daily as brand_overall
        self.account_id, self.user_id, self.brand_id, self.page_name = yield tornado.gen.Task(self.getId)
        brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "IN")
        self.set_status(200)
        self.finish()
        li_user = yield tornado.gen.Task(self.linkedInFunction)
        if li_user=="okk":
            #----change social media limit

            user_check_res = db_conn.sociabyte.user.find({'_id':(self.user_id)})
            for users in user_check_res:
                user_type = users['access_type']['user_type']
                try:
                    members = [members['member_id'] for members in users['members'] if members['status'] is not 'Invited']
                except:
                    pass
                try:
                    owner_id = users['membership_details']['owner_id']
                except:
                    owner_id = None
            if user_type=='Administrator':
                members.append(self.user_id)
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
            else:
                admin_user_res = db_conn.sociabyte.user.find({'_id':(owner_id)})
                members = [members['member_id'] for members in admin_user_res['members'] if members['status'] is not 'Invited']
                members.append(owner_id)
                db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
            #----- end change social media limit
            insert_json = {
        "to" : self.user_id,
        "text" : "The linkedIn page named as "+self.page_name+" has been successfully added",
        "action_url" : "http://sociabyte.com/brands/"+str(self.brand_id)+"/"+str(self.account_id)+"/linkedin/overall",
        "status" : "unseen",
        "timestamp":datetime.datetime.now(),
        'type':'social_media',
        'details': {
           'userid':self.user_id,
           'brandid':self.brand_id,
           'pageid':self.account_id,
           'platform':'linkedin'
        }
    }
            id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
            urllib.urlopen(url_download)
        elif li_user=='new page':
            pass
        else:
            insert_json = {
        "to" : self.user_id,
        "text" : "Some error occured for linkedin page with id: "+self.account_id,
        "action_url" : "http://sociabyte.com/"+str(self.brand_id)+"/dashboard",
        "status" : "unseen",
        "timestamp":datetime.datetime.now()
    }
            id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
            url_download ="http://sociabyte.com/notification/"+str(id_file_db)
            urllib.urlopen(url_download)
    def getId(self, callback):
        idaccount = self.get_argument('ln_account_id')
        idUser = self.get_argument('uname')
        idBrand = self.get_argument('brandId')
        page_name = self.get_argument('page_name')
        callback([idaccount, idUser, idBrand, page_name])
    def linkedInFunction(self, callback):
        import li_module.li_services as li_services
        from brand_overall import brand_overall_daily as brand_overall
        li_user_id = self.account_id
        db_sociabyte = db_conn.sociabyte
        #print self.brand_id, '========>>'
        acc_list = []
        acc_list_res= db_sociabyte.li_basic_info.find({}, {'id':1, '_id':0})
        for acc in acc_list_res:
            acc_list.append(str(acc['id']))
        if self.account_id not in acc_list:
            access_token_response = db_sociabyte.brands.find({'associated_accounts.in_accounts.page_id':str(li_user_id), '_id':ObjectId(self.brand_id)}, {'associated_accounts.in_accounts.token':1, 'associated_accounts.in_accounts.page_id':1, '_id':0})
            counter = 0
            access_token = ''
            for acc in access_token_response:
                #print acc, '===========acc'
                for acc_unit in acc['associated_accounts']['in_accounts']:
                    #print acc_unit, '====================acc_unit'
                    if acc['associated_accounts']['in_accounts'][counter]['page_id']==str(li_user_id):
                        access_token = acc['associated_accounts']['in_accounts'][counter]['token']
                    else:
                        pass
                    counter = counter+1
            try:
                li_services.basicInfoFetch(li_user_id, db_sociabyte, access_token, urllib, json, self.brand_id)
                li_services.compShareInfo(li_user_id, db_sociabyte, access_token, urllib, json)
                li_services.historicalFollowerStatistics(li_user_id, db_sociabyte, access_token, urllib, json)
                li_services.historicalStatusUpdateStat(li_user_id, db_sociabyte, access_token, urllib, json)
                shares_list = []
                share_response = db_sociabyte.li_shares.find({'updateContent.company.id':int(li_user_id)}, {'updateKey':1, '_id':0})
                for shares in share_response:
                    shares_list.append(shares['updateKey'])
                li_services.commentFetch(li_user_id, shares_list, db_sociabyte, access_token, urllib, json)
                li_services.specificStatusStat(li_user_id, shares_list, db_sociabyte, access_token, urllib, json)
                li_services.companyStatistics(li_user_id, db_sociabyte, access_token, urllib, json)
                sendDict = {
                    'acc_id':li_user_id,
                    'page_name':self.page_name,
                    'brand_id':self.brand_id,
                    'user_id':self.user_id,
                    'code':'li'
                }
                sendDict = json.dumps(sendDict)
                port = '5563'
                context = zmq.Context()
                print ("Connecting to server...")
                socket = context.socket(zmq.PUB)
                socket.bind("tcp://*:%s" % port)
                print ("Sending request ")
                a=0

                while a<2:
                    socket.send_multipart(['',sendDict])
                    print "send successfull"
                    a+=1
                    time.sleep(1)
                #  Get the reply.
                socket.close()
                #----change social media limit

                user_check_res = db_conn.sociabyte.user.find({'_id':(self.user_id)})
                for users in user_check_res:
                    user_type = users['access_type']['user_type']
                    try:
                        members = [members['member_id'] for members in users['members'] if members['status'] is not 'Invited']
                    except:
                        pass
                    try:
                        owner_id = users['membership_details']['owner_id']
                    except:
                        owner_id = None
                if user_type=='Administrator':
                    members.append(self.user_id)
                    db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
                else:
                    admin_user_res = db_conn.sociabyte.user.find({'_id':(owner_id)})
                    members = [members['member_id'] for members in admin_user_res['members'] if members['status'] is not 'Invited']
                    members.append(owner_id)
                    db_conn.sociabyte.user.update({'_id':{"$in":members}}, {'$inc':{'access_type.social_media_limit':-1}}, multi=True)
                #----- end change social media limit
                brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "IN")
                callback("new page")
            except:
                db_conn.sociabyte.brands.update({'_id':ObjectId(self.brand_id)}, {"$pull":{"associated_accounts.in_accounts":{"page_id":str(li_user_id)}}})
                db_conn.sociabyte.li_basic_info.remove({"id":int(li_user_id)})
                brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "IN")
                callback("not ok")
        else:
            db_conn.sociabyte.li_basic_info.update({"id":int(li_user_id)},{'$addToSet':{'brand_id':self.brand_id}})
            brand_overall.executorOverall(db_conn, self.brand_id, 'brand', self.user_id, self.page_name, "IN")
            callback("okk")

class Interaction(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self,platform,action,entity):
        self.http_client=AsyncHTTPClient()
        interaction=yield tornado.gen.Task(self.doInteraction,platform,action,entity)
        self.finish(str(interaction))

    def doInteraction(self,platform,action, entity,callback):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Credentials", "true")
        self.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        self.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")

        try:
            platforms=['facebook','twitter','instagram', 'vkontakte','linkedin','youtube']
            actions={'facebook':['like','share','comment','getComment'], 'linkedin' : ['getComment','comment'] ,'youtube':['getComment','comment'],'vkontakte':['like','share','comment'],'twitter':['reply','favourite','getReply'],'instagram':['like','comment']}
            if platform in platforms:
                if action in actions[platform]:
                    callfunc=platform+action
                    #print(callfunc)
                    func=getattr(self,callfunc)
                    ret=func(entity)
            callback(ret)
        except Exception as e:
            #print(e)
            callback()

    def facebookcomment(self,entity):
        fb_account_id = self.get_argument('fb_account_id')#self.get_argument('fb_account_id')
        message=self.get_argument('message')
        db_sociabyte = db_conn.sociabyte
        access_token = ''
        access_token_response = db_sociabyte.brands.find({"associated_accounts.fb_accounts.page_id":fb_account_id}, {"associated_accounts.fb_accounts.token":1, "associated_accounts.fb_accounts.page_id":1, "_id":0})
        counter = 0
        for acc in access_token_response:
            for acc_unit in acc['associated_accounts']['fb_accounts']:
                #print 'acc===>>>',acc_unit
                if acc_unit['page_id']==fb_account_id:
                    access_token = acc_unit['token']
                else:
                    pass
                counter = counter+1

        #print 'counter====<>>>>',counter
        fb_handle = facebook.GraphAPI(access_token)
        commentId=fb_handle.put_comment(entity, message=message)

        if commentId:
            self.set_status(200)
            comment_count = db_sociabyte.fb_post_details.find({"page_id":fb_account_id,"post_id":entity},{"comments":1,"from_name":1, "_id":0})
            clc = int()
            ret_list = []
            from_name = ''
            for liked in comment_count:
                clc = liked['comments'] + 1
                from_name = liked['from_name']
                db_sociabyte.fb_post_details.update({"page_id":fb_account_id,"post_id":entity},{"$set":{"comments":clc}})
            cmnt_json = {
                            "from" : {
                                "id" :fb_account_id ,
                                "name" : from_name
                            },
                            "sentiment" : "",
                            "post_id" : entity,
                            "like_count" : 0,
                            "keywords" : [],
                            "created_time" : datetime.datetime.now(),
                            "message" : message,
                            "page_id" : fb_account_id
                        }
            #print 'comment_json', cmnt_json
            db_sociabyte.fb_comments.insert(cmnt_json)
            ret_dict = {"count": clc,
                        "from_name": from_name,
                        "message": message}
            #print 'ret_dict===>>>', ret_dict
            ret_list.append(ret_dict)
            return json.dumps(ret_list)
        else:
            self.set_status(404)

    def facebookgetComment(self,entity):
        #print('hii')
        fb_account_id = self.get_argument('fb_account_id')
        db_sociabyte = db_conn.sociabyte
        comments = db_sociabyte.fb_comments.find({"page_id":fb_account_id,"post_id":entity},{"message":1,"from.name":1, "_id":0})
        com_list = []
        for com in comments:
            com_list.append(com)

        #print (json.dumps(com_list))
        return json.dumps(com_list)




    def facebooklike(self, entity):
        fb_account_id = self.get_argument('fb_account_id')
        #print (fb_account_id)
        db_sociabyte = db_conn.sociabyte
        db_brands = db_sociabyte.brands
        access_token = ''
        access_token_response = db_sociabyte.brands.find({"associated_accounts.fb_accounts.page_id":fb_account_id}, {"associated_accounts.fb_accounts.token":1, "associated_accounts.fb_accounts.page_id":1, "_id":0})
        counter = 0
        for acc in access_token_response:
            for acc_unit in acc['associated_accounts']['fb_accounts']:
                #print 'acc===>>>',acc_unit
                if acc_unit['page_id']==fb_account_id:
                    access_token = acc_unit['token']
                else:
                    pass
                counter = counter+1
        fb_handle = facebook.GraphAPI(access_token)
        like_id = fb_handle.put_like(entity)
        #print(type(like_id))
        if like_id:
            self.set_status(200)
            like_count = db_sociabyte.fb_post_details.find({"page_id":fb_account_id,"post_id":entity},{"likes":1,"_id":0})
            for likes in like_count:
                lk = likes['likes'] = likes['likes'] + 1
                #print(likes['likes'])
            db_sociabyte.fb_post_details.update({"page_id":fb_account_id,"post_id":entity},{"$set":{"likes":lk}})
            db_sociabyte.fb_post_details.update({"page_id":fb_account_id,"post_id":entity},{"$set":{"liked":1}})
            return lk
        else:
            self.set_status(404)

    def twitterfavourite(self,entity):
        tw_account_id = self.get_argument('tw_user_id')
        #print(tw_account_id)
        #print(entity)
        db_sociabyte = db_conn.sociabyte
        db_brands = db_sociabyte.brands

        access_token_result = db_conn.sociabyte.brands.find({"associated_accounts.tw_accounts.page_id":tw_account_id}, {"associated_accounts.tw_accounts.token":1,"associated_accounts.tw_accounts.page_id":1, "_id": 0, "associated_accounts.tw_accounts.tokenSecret":1,"associated_accounts.tw_accounts.name":1})

        token = ''
        tokenSecret = ''
        #print(access_token_result[0]['associated_accounts']['tw_accounts'])
        for accounts in access_token_result[0]['associated_accounts']['tw_accounts']:
            if accounts['page_id']==tw_account_id:
                token = accounts['token']
                tokenSecret = accounts['tokenSecret']
                name=accounts['name']
                #print(name)
        #print "after tw"
        favorited = db_sociabyte.tw_tweets.find({"user_id":tw_account_id,"tw_id":entity},{"favorited":1,"_id":0})
        tf = ''
        for fav in favorited:
            tf = str(fav['favorited'])
            #print('tf====>',tf)

        if tf == 'False':
            #print('i m in if')
            tw_handle = Twython('Qwu1NiiV2uzGp3Hj0UqD1CKo2', '6CiVt3lDPnD4qOaccPOxpcPeTNkZ8h8ljelavK79xKsqpgEEYF',token,tokenSecret)
            favo_res = tw_handle.create_favorite(id=entity)
            if favo_res:
                fav_count = db_sociabyte.tw_tweets.find({"user_id":tw_account_id,"tw_id":entity},{"favorite_count":1,"_id":0})
                for favc in fav_count:
                    fc = favc['favorite_count'] = favc['favorite_count'] + 1
                    #print(fc)
                db_sociabyte.tw_tweets.update({"user_id":tw_account_id,"tw_id":entity},{"$set":{"favorite_count":fc}})
                db_sociabyte.tw_tweets.update({"user_id":tw_account_id,"tw_id":entity},{"$set":{"favorited":True}})
                self.set_status(200)
                return fc
            else:
                self.set_status(404)


        elif tf == 'True':
            #print('i m in else')
            tw_handle = Twython('Qwu1NiiV2uzGp3Hj0UqD1CKo2', '6CiVt3lDPnD4qOaccPOxpcPeTNkZ8h8ljelavK79xKsqpgEEYF',token,tokenSecret)
            favo_res = tw_handle.destroy_favorite(id=entity)
            if favo_res:
                fav_count = db_sociabyte.tw_tweets.find({"user_id":tw_account_id,"tw_id":entity},{"favorite_count":1,"_id":0})
                for favc in fav_count:
                    fc = favc['favorite_count'] = favc['favorite_count'] - 1
                    #print(fc)
                if fc < 0:
                    fc = 0
                db_sociabyte.tw_tweets.update({"user_id":tw_account_id,"tw_id":entity},{"$set":{"favorite_count":fc}})
                db_sociabyte.tw_tweets.update({"user_id":tw_account_id,"tw_id":entity},{"$set":{"favorited":False}})
                self.set_status(200)
                return fc
            else:
                self.set_status(404)

    def twitterreply(self,entity):
        tw_account_id = self.get_argument('tw_user_id')
        msg = self.get_argument('message')

        db_sociabyte = db_conn.sociabyte
        db_brands = db_sociabyte.brands

        access_token_result = db_conn.sociabyte.brands.find({"associated_accounts.tw_accounts.page_id":tw_account_id}, {"associated_accounts.tw_accounts.token":1,"associated_accounts.tw_accounts.page_id":1, "_id": 0, "associated_accounts.tw_accounts.tokenSecret":1,"associated_accounts.tw_accounts.name":1})

        token = ''
        tokenSecret = ''
        #print(access_token_result[0]['associated_accounts']['tw_accounts'])
        for accounts in access_token_result[0]['associated_accounts']['tw_accounts']:
            if accounts['page_id']==tw_account_id:
                token = accounts['token']
                tokenSecret = accounts['tokenSecret']
                name=accounts['name']
                #print(name)

        tw_handle = Twython('Qwu1NiiV2uzGp3Hj0UqD1CKo2', '6CiVt3lDPnD4qOaccPOxpcPeTNkZ8h8ljelavK79xKsqpgEEYF',token,tokenSecret)
        reply_res = tw_handle.update_status(status=msg,in_reply_to_status_id = entity)
        ret_list = []
        if reply_res:
            self.set_status(200)
            cmnt_json = {

                "symbols" : reply_res['entities']['symbols'],
                "user_id" : str(reply_res['in_reply_to_user_id']),
                "sentiment" : "",
                "text" : reply_res['text'],
                "created_at" : reply_res['user']['created_at'],
                "hashtags" : reply_res['entities']['hashtags'],
                "coordinates" : reply_res['coordinates'],
                "geo" : reply_res['geo'],
                "source" : reply_res['source'],
                "in_reply_to_status_id" : str(reply_res['in_reply_to_status_id_str']),
                "place" : reply_res['place'],
                "user" : {
                    "statuses_count" : reply_res['user']['statuses_count'],
                    "description" : reply_res['user']['entities']['description'],
                    "friends_count" : reply_res['user']['friends_count'],
                    "profile_image_url" : reply_res['user']['profile_image_url'],
                    "name" : reply_res['user']['name'],
                    "lang" : reply_res['user']['lang'],
                    "favourites_count" : reply_res['user']['favourites_count'],
                    "screen_name" : reply_res['user']['screen_name'],
                    "location" : reply_res['user']['location'],
                    "followers_count" : reply_res['user']['followers_count'],
                    "id_str" : str(reply_res['user']['id_str']),
                    "listed_count" : reply_res['user']['listed_count']
                },
                "urls" : reply_res['entities']['urls'],
                "keywords" : [],
                "mention_id" : str(reply_res['id_str']),
                "retweet_count" : reply_res['retweet_count'],
                "user_name" : reply_res['in_reply_to_screen_name'],
                "favorite_count" : reply_res['favorite_count']
                }
            #print(cmnt_json)
            db_sociabyte.tw_mentions.insert(cmnt_json)
            ret_dict = {
                        "screen_name": reply_res['user']['screen_name'],
                        "message": reply_res['text']}
            ret_list.append(ret_dict)
            return json.dumps(ret_list)
        else:
            self.set_status(404)

    def twittergetReply(self,entity):
        tw_user_id = self.get_argument('tw_user_id')
        db_sociabyte = db_conn.sociabyte
        comments = db_sociabyte.tw_mentions.find({"user_id":str(tw_user_id),"in_reply_to_status_id":str(entity)},{"text":1,"user.screen_name":1, "_id":0})
        com_list = []
        for com in comments:
            com_list.append(com)

        #print(json.dumps(com_list))

        return json.dumps(com_list)


    def vkontaktelike(self,entity):
        vk_user_id = self.get_argument('vk_user_id')
        #print(vk_user_id)
        #print(entity)

        db_sociabyte = db_conn.sociabyte
        db_brands = db_sociabyte.brands
        token_result = db_brands.find({"associated_accounts.vk_accounts.page_id":vk_user_id}, {"associated_accounts.vk_accounts.token":1,"_id": 0})
        token = ''
        for vk in token_result:
            token = vk["associated_accounts"]["vk_accounts"][0]["token"]

        liked = db_sociabyte.vk_wall_posts.find({"user_id":int(vk_user_id),"id":entity},{"liked":1,"_id":0})
        vl = int()
        for fav in liked:
            vl = fav['liked']
            #print('tf====>',vl)

        if vl == 0:
            #print('i m in if')
            vk_handle = urllib.urlopen('https://api.vk.com/method/wall.addLike?owner_id='+vk_user_id+'&token='+token+'&post_id='+entity)
            #print(json.loads(vk_handle.read()))



        else:
            #print('i m in else')
            vk_handle = urllib.urlopen('https://api.vk.com/method/wall.deleteLike?owner_id='+vk_user_id+'&token='+token+'&post_id='+entity)
            #print(json.loads(vk_handle.read()))

    def instagramlike(self,entity):


        inst_user_id = self.get_argument('inst_user_id')
        #print(inst_user_id)
        #print(entity)

        db_sociabyte = db_conn.sociabyte
        access_token_response = db_sociabyte.brands.find({"associated_accounts.ins_accounts.page_id":inst_user_id}, {"associated_accounts.ins_accounts.token":1, "associated_accounts.ins_accounts.page_id":1, "_id":0})
        counter = 0
        for acc in access_token_response:
            for acc_unit in acc['associated_accounts']['ins_accounts']:
                if acc['associated_accounts']['ins_accounts'][counter]['page_id']==inst_user_id:
                    access_token = acc['associated_accounts']['ins_accounts'][counter]['token']
                else:
                    pass
                counter = counter+1

        liked = db_sociabyte.inst_media.find({"user_id":inst_user_id,"media_id":entity},{"liked":1,"_id":0})
        #print(access_token)
        il = int()
        for fav in liked:
            il = fav['liked']
            #print('tf====>',il)

        if il == 0:
            try:
                api = InstagramAPI(access_token)
                inst_handle = api.like_media(entity)
                #print(inst_handle)
            except InstagramAPIError as e:
                print(e)
        else:
            try:
                api = InstagramAPI(access_token)
                inst_handle = api.unlike_media(entity)
                #print(inst_handle)
            except InstagramAPIError as e:
                print(e)

    def instagramcomment(self,entity):

        inst_user_id = self.get_argument('inst_user_id')
        msg = self.get_argument('message')
        #print(inst_user_id)
        #print(entity)

        db_sociabyte = db_conn.sociabyte
        access_token_response = db_sociabyte.brands.find({"associated_accounts.ins_accounts.page_id":inst_user_id}, {"associated_accounts.ins_accounts.token":1, "associated_accounts.ins_accounts.page_id":1, "_id":0})
        counter = 0
        access_token = ''
        for acc in access_token_response:
            for acc_unit in acc['associated_accounts']['ins_accounts']:
                if acc['associated_accounts']['ins_accounts'][counter]['page_id']==inst_user_id:
                    access_token = acc['associated_accounts']['ins_accounts'][counter]['token']
                else:
                    pass
                counter = counter+1

        #print(access_token)

        try:
            api = InstagramAPI(access_token)
            inst_handle = api.create_media_comment(entity, msg)
            #print(inst_handle)
        except InstagramAPIError as e:
            print(e)

    def linkedingetComment(self,entity):
        #print('hii')
        company_id = self.get_argument('company_id')
        db_sociabyte = db_conn.sociabyte
        comments = db_sociabyte.li_comments.find({"update_key":str(entity)},{"comment":1,"person.firstName":1, "_id":0})
        com_list = []
        for com in comments:
            com_list.append(com)

        #print (json.dumps(com_list))
        return json.dumps(com_list)

    def linkedincomment(self,entity):

        company_id = self.get_argument('company_id')#self.get_argument('fb_account_id')
        message=self.get_argument('message')
        #print(message)
        db_sociabyte = db_conn.sociabyte
        access_token_response = db_sociabyte.brands.find({"associated_accounts.in_accounts.page_id":company_id}, {"associated_accounts.in_accounts.token":1,"associated_accounts.in_accounts.name":1,'associated_accounts.in_accounts.page_id':1, "_id":0})
        counter = 0
        access_token = ''
        page_name = ''

        for acc in access_token_response:
            for acc_unit in acc['associated_accounts']['in_accounts']:
                if acc['associated_accounts']['in_accounts'][counter]['page_id'] == company_id:
                    access_token = acc['associated_accounts']['in_accounts'][counter]['token']
                    page_name= acc['associated_accounts']['in_accounts'][counter]['name']

                else:
                    pass
                counter = counter+1

        api_key = '75yjgp8cmgt0ya'
        secret_key = 'l9unFB81hyWnXTCe'
        from linkedin import linkedin
        app = linkedin.LinkedInApplication(token=access_token)
        linkdin_handle = app.comment_on_update(update_key=entity,comment=message)
        #print(linkdin_handle)

    def youtubegetComment(self,entity):
        #print('hii')
        channelId = self.get_argument('channel_Id')
        db_sociabyte = db_conn.sociabyte
        comments = db_sociabyte.utube_video_comments.find({"yt_channelId":str(channelId),"yt_videoid":str(entity)},{"content":1,"author.name":1, "_id":0})
        com_list = []
        for com in comments:
            com_list.append(com)

        #print (json.dumps(com_list))
        return json.dumps(com_list)

    def youtubecomment(self,entity):

        channelId = self.get_argument('channel_Id')
        message=self.get_argument('message')
        db_sociabyte = db_conn.sociabyte
        access_token_response = db_sociabyte.brands.find({"associated_accounts.utube_accounts.page_id":channelId}, {"associated_accounts.utube_accounts.accessToken":1,"associated_accounts.utube_accounts.name":1,'associated_accounts.utube_accounts.page_id':1, "_id":0})
        counter = 0
        access_token = ''
        page_name = ''

        for acc in access_token_response:
            for acc_unit in acc['associated_accounts']['utube_accounts']:
                if acc['associated_accounts']['utube_accounts'][counter]['page_id'] == channelId:
                    access_token = acc['associated_accounts']['utube_accounts'][counter]['accessToken']
                    page_name= acc['associated_accounts']['utube_accounts'][counter]['name']

                else:
                    pass
                counter = counter+1

        #print(access_token)
        #print(page_name)

        url = 'https://gdata.youtube.com/feeds/api/videos/'+str(entity)+'/comments'
        values = { 'Content-Type': 'application/atom+xml','Content-Length' : len(str(message)) ,'Authorization': str(access_token),'GData-Version' : 2 , 'X-GData-Key': 'AIzaSyApCAE4SbaCtSOAKayxMS6qFNNWbtzhhSo'}
        data = urllib.urlencode(values)
        req =  urllib2.request.Request(url, data,method='POST')
        req.add_header('Accept', 'application/json')
        response = urllib2.urlopen(req)
        result = response.read()
        #print (result)
class Uploadfile(tornado.web.RequestHandler):
    mongo_id = ''
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        #print('Hii I am in FB Services')
        self.mongo_id = yield tornado.gen.Task(self.getId)
        sendDict = {
                'acc_id':self.mongo_id,
                'code':'uploadfile'
            }
        sendDict = json.dumps(sendDict)
        port = '5565'
        context = zmq.Context()
        print ("Connecting to server...")
        socket = context.socket(zmq.PUB)
        socket.bind("tcp://*:%s" % port)
        print ("Sending request ")
        a=0

        while a<2:
            socket.send_multipart(['',sendDict])
            print "send successfull"
            a+=1
            time.sleep(1)
        #  Get the reply.
        socket.close()
        print('I am completed', self.mongo_id)
        self.write("ok")
        self.finish()
    def getId(self, callback):
        file_db_id = self.get_argument('mongo_id')
        callback(file_db_id)
class DeleteSocialAccount(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self):
        result = yield tornado.gen.Task(self.deletemedia)
        self.write("ok")
        self.finish()

    def deletemedia(self, callback):
        from brand_overall import brand_overall_daily as brand_overall
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Credentials", "true")
        self.set_header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
        self.set_header("Access-Control-Allow-Headers"," Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, Cache-Control")
        brand_id = self.get_argument("brand_id")
        social_id = self.get_argument("media_id")
        media = self.get_argument('media')
        #print "brand_id", brand_id, "social_id", social_id, "media", media
        if media=="FB":
            db_conn.sociabyte.brands.update({'_id':ObjectId(brand_id)}, {"$pull":{"associated_accounts.fb_accounts":{"page_id":social_id}}})
            account_checker = db_conn.sociabyte.fb_accounts.find({"page_id":social_id}, {'brand_id':1})
            for brands in account_checker:
                brand_list = brands['brand_id']
            if len(brand_list)>1:
                db_conn.sociabyte.fb_accounts.update({"page_id":social_id}, {"$pull":{"brand_id":brand_id}})
            else:
                db_conn.sociabyte.fb_accounts.remove({"page_id":social_id})
                db_conn.sociabyte.fb_accounts_daily.remove({"page_id":social_id})
                db_conn.sociabyte.fb_comments.remove({"page_id":social_id})
                db_conn.sociabyte.fb_likes.remove({"page_id":social_id})
                db_conn.sociabyte.fb_page_insights.remove({"page_id":social_id})
                db_conn.sociabyte.fb_post_details.remove({"page_id":social_id})
                db_conn.sociabyte.fb_top_contributors.remove({"master_id":social_id})
                db_conn.sociabyte.service_request.remove({"page_id":social_id})
            #competitor delete
            db_conn.sociabyte.fb_accounts_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.fb_accounts_comp_daily.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.fb_comments_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.fb_page_insights_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.fb_post_details_comp.remove({'brand_id': brand_id,'master_page': social_id})
        elif media=="TW":
            db_conn.sociabyte.brands.update({'_id':ObjectId(brand_id)}, {"$pull":{"associated_accounts.tw_accounts":{"page_id":social_id}}})
            account_checker = db_conn.sociabyte.tw_users.find({"id":int(social_id)}, {'brand_id':1})
            for brands in account_checker:
                brand_list = brands['brand_id']
            if len(brand_list)>1:
                db_conn.sociabyte.tw_users.update({"id":int(social_id)}, {"$pull":{"brand_id":brand_id}})
            else:
                db_conn.sociabyte.tw_users.remove({"id":int(social_id)})
                db_conn.sociabyte.tw_daily_reach.remove({"user_id":(social_id)})
                db_conn.sociabyte.tw_follower_list.remove({"user_id":(social_id)})
                db_conn.sociabyte.tw_followers.remove({"celeb":(social_id)})
                db_conn.sociabyte.tw_retweets.remove({"user_id":(social_id)})
                db_conn.sociabyte.tw_tweets.remove({"user_id":(social_id)})
                db_conn.sociabyte.tw_users_daily.remove({"user_id":(social_id)})
                db_conn.sociabyte.service_request.remove({"page_id":social_id})
            #delete competitor
            db_conn.sociabyte.tw_users_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.tw_tweets_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.tw_followers_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.tw_follower_list_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.tw_daily_reach_comp.remove({'brand_id': brand_id,'master_page': social_id})
        elif media=="INST":
            db_conn.sociabyte.brands.update({'_id':ObjectId(brand_id)}, {"$pull":{"associated_accounts.ins_accounts":{"page_id":social_id}}})
            account_checker = db_conn.sociabyte.inst_user.find({"ins_id":social_id}, {'brand_id':1})
            for brands in account_checker:
                brand_list = brands['brand_id']
            if len(brand_list)>1:
                db_conn.sociabyte.inst_user.update({"ins_id":social_id}, {"$pull":{"brand_id":brand_id}})
            else:
                db_conn.sociabyte.inst_user.remove({"ins_id":social_id})
                db_conn.sociabyte.inst_comments.remove({"user_id":social_id})
                db_conn.sociabyte.inst_daily_reach.remove({"user_id":social_id})
                db_conn.sociabyte.inst_followedby.remove({"follower_of":social_id})
                db_conn.sociabyte.inst_likes.remove({"user_id":social_id})
                db_conn.sociabyte.inst_media.remove({"user_id":social_id})
                db_conn.sociabyte.inst_user_daily.remove({"ins_id":social_id})
                db_conn.sociabyte.service_request.remove({"page_id":social_id})
            #delete competitor
            db_conn.sociabyte.inst_user_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.inst_media_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.inst_followedby_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.inst_daily_reach_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.inst_comments_comp.remove({'brand_id': brand_id,'master_page': social_id})
        elif media=="IN":
            db_conn.sociabyte.brands.update({'_id':ObjectId(brand_id)}, {"$pull":{"associated_accounts.in_accounts":{"page_id":social_id}}})
            account_checker = db_conn.sociabyte.li_basic_info.find({"id":int(social_id)}, {'brand_id':1})
            for brands in account_checker:
                brand_list = brands['brand_id']
            if len(brand_list)>1:
                db_conn.sociabyte.li_basic_info.update({"id":int(social_id)}, {"$pull":{"brand_id":brand_id}})
            else:
                db_conn.sociabyte.li_basic_info.remove({"id":int(social_id)})
                db_conn.sociabyte.li_basic_info_daily.remove({"id":int(social_id)})
                db_conn.sociabyte.li_comments.remove({"id":(social_id)})
                db_conn.sociabyte.li_follower_stat.remove({"id":(social_id)})
                db_conn.sociabyte.li_followers_comp_size.remove({"id":(social_id)})
                db_conn.sociabyte.li_followers_country.remove({"id":(social_id)})
                db_conn.sociabyte.li_followers_function.remove({"id":(social_id)})
                db_conn.sociabyte.li_followers_industry.remove({"id":(social_id)})
                db_conn.sociabyte.li_followers_seniority.remove({"id":(social_id)})
                db_conn.sociabyte.li_shares.remove({"updateContent.company.id":int(social_id)})
                db_conn.sociabyte.li_status_stat.remove({"id":(social_id)})
                db_conn.sociabyte.service_request.remove({"page_id":social_id})
        elif media=="VK":
            db_conn.sociabyte.brands.update({'_id':ObjectId(brand_id)}, {"$pull":{"associated_accounts.vk_accounts":{"page_id":social_id}}})
            account_checker = db_conn.sociabyte.vk_user.find({"uid":int(social_id)}, {'brand_id':1})
            for brands in account_checker:
                brand_list = brands['brand_id']
            if len(brand_list)>1:
                db_conn.sociabyte.vk_user.update({"uid":int(social_id)}, {"$pull":{"brand_id":brand_id}})
            else:
                db_conn.sociabyte.vk_user.remove({"uid":int(social_id)})
                db_conn.sociabyte.vk_age_group.remove({"user_id":int(social_id)})
                db_conn.sociabyte.vk_daily_reach.remove({"user_id":int(social_id)})
                db_conn.sociabyte.vk_followers.remove({"user_id":int(social_id)})
                db_conn.sociabyte.vk_friends.remove({"user_id":int(social_id)})
                db_conn.sociabyte.vk_user_daily.remove({"uid":int(social_id)})
                db_conn.sociabyte.vk_wall_posts.remove({"user_id":int(social_id)})
                db_conn.sociabyte.service_request.remove({"page_id":social_id})
            #delete competitor
            db_conn.sociabyte.vk_user_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.vk_friends_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.vk_age_group_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.vk_daily_reach_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.vk_followers_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.vk_friends_comp.remove({'brand_id': brand_id,'master_page': social_id})
            db_conn.sociabyte.vk_wall_posts_comp.remove({'brand_id': brand_id,'master_page': social_id})
        elif media=="UTUBE":
            db_conn.sociabyte.brands.update({'_id':ObjectId(brand_id)}, {"$pull":{"associated_accounts.utube_accounts":{"page_id":social_id}}})
            account_checker = db_conn.sociabyte.utube_basic_details.find({"channel_id":(social_id)}, {'brand_id':1})
            for brands in account_checker:
                brand_list = brands['brand_id']
            if len(brand_list)>1:
                db_conn.sociabyte.utube_basic_details.update({"channel_id":(social_id)}, {"$pull":{"brand_id":brand_id}})
            else:
                db_conn.sociabyte.utube_basic_details.remove({"channel_id":social_id})
                db_conn.sociabyte.utube_active_platform.remove({"channel_id":social_id})
                db_conn.sociabyte.utube_channel_growth.remove({"channel_id":social_id})
                db_conn.sociabyte.utube_channel_videos.remove({"channelId":social_id})
                db_conn.sociabyte.utube_insight_playback.remove({"channel_id":social_id})
                db_conn.sociabyte.utube_insight_tsType.remove({"channel_id":social_id})
                db_conn.sociabyte.utube_traffic_source.remove({"channel_id":social_id})
                db_conn.sociabyte.service_request.remove({"page_id":social_id})

        brand_overall.executorOverall(db_conn, brand_id, 'brand', "NR", "NR", media)
        #print "okk delete"
        callback()
if __name__ == '__main__':
    #conn = MongoClient('localhost', 27017)
    application = tornado.web.Application([
        (r"/projectsearchtemp", ProjectDataFetchTemp), (r"/projectsearchparm", ProjectDataFetchParmanent), (r"/fbservices", FbServices), (r"/twservices", TwServices), (r"/instservices", InstServices),
        (r"/vkservices", VkServices), (r"/utubeservices", youtubeServices), (r"/gplusservices", gplusServices), (r"/lnservices", linkedInServices), (r"/interaction/(?P<platform>[^\/]+)/?(?P<action>[^\/]+)?/?(?P<entity>[^\/]+)?",Interaction),(r"/engagement/(?P<platform>[^\/]+)/?(?P<action>[^\/]+)?/?(?P<entity>[^\/]+)?",EngagementHandler),(r"/scheduling/(?P<platform>[^\/]+)/?(?P<action>[^\/]+)?/?(?P<entity>[^\/]+)?",SchedulingHandler)
        ,(r"/uploadfile", Uploadfile), (r"/deletemedia", DeleteSocialAccount)
    ],debug=True)
    application.listen(port=8888)
    print("Listenning on port 8888...")
    tornado.ioloop.IOLoop.instance().start()





