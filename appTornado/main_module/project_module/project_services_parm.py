# -*- coding: iso-8859-15 -*-
import urllib
import json
from pymongo import MongoClient
import xml
import global_settings
from urlparse import urlparse
client = global_settings.db_conn
from HTMLParser import HTMLParser
from bson.objectid import ObjectId
import datetime
import time
from nltk.corpus import names
import nltk
import random
from datetime import timedelta
def StrToDatetime(dt):

    d = datetime.datetime.strptime(dt,'%Y-%m-%d')
    return d
class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)
def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

"""def FeedUrlFetch(arg_formal):
    from langdetect import detect
    category, keystr, lebel, languages, project_id_formal = arg_formal
    url = 'https://ajax.googleapis.com/ajax/services/feed/find?' +'v=1.0&q='+keystr+'&userip=50.18.63.29'
    #print "url===>>>>", arg_formal
    response = urllib.urlopen(url)
    results = json.loads(response.read())
    #print "results==>>>", results['responseData']['entries']
    entry_list = []
    entry_to_rss_json ={
        "contentSnippet" : "",
        "keyword" : "",
        "title" : "",
        "link" : "",
        "project_id" : "",
	    "author":"Anonymous",
	    "categories":[],
        "content":"",
        "keywords":[],
        "publishedDate":datetime.datetime.now(),
        "sentiment":"",
        "source" : "",
        "location":"N/A",
        "category":category,
        "lebel": lebel,
        "languages": languages
    }
    sources_json = {
            "name" : "unknown",
            "domain_url" : "",
            "feed_link" : "",
            "topic" : "unknown",
            "country" : "unknown"
        }
    entry_list2 = []
    entry_list3 = []
    print "results['responseData']['entries']:", results
    for result in results['responseData']['entries']:
        entry_to_rss_json['contentSnippet']=entry_to_rss_json['content']=result['contentSnippet']= strip_tags(result['contentSnippet'])
        entry_to_rss_json['title']=result['title'] = strip_tags(result['title'])
        entry_to_rss_json['keyword']=result['keyword']=keystr
        entry_to_rss_json['project_id']=result['project_id'] = (project_id_formal)
        try:
            url_text = "http://ws.detectlanguage.com/0.2/detect?q={text}&key=c46f5c863e4957da6511c50da60a46c1".format(text=strip_tags(entry_to_rss_json['content'].encode('utf-8')))
            text_language_response = urllib.urlopen(url_text)
            lang_result = json.loads(text_language_response.read())
            print "lang_result", lang_result
            text_language = lang_result['data']['detections'][0]['language']
            entry_to_rss_json['lang']=text_language #detect(entry_to_rss_json['content'].encode('utf-8'))
        except:
            entry_to_rss_json['lang']="en"
        result['fetched'] = 'No'
        entry_to_rss_json['link']= result['link']
        source_temp = result['link']
        source_temp1= source_temp.replace('www.', '')
        source_temp2 = find_between(source_temp1, '//', '.')
        entry_to_rss_json['source']= source_temp2
        entry_list.append(result.copy())
        entry_list2.append(entry_to_rss_json.copy())
        sources_json['domain_url']=result['link']
        sources_json['feed_link']=result['url']
        entry_list3.append(sources_json.copy())
    try:
        client.sociabyte.feed_link.insert(entry_list)
        client.sociabyte.rss_response.insert(entry_list2)
        client.sociabyte.mainstream_sources.insert(entry_list3)
    except:
        pass
    print "len entry list 2", len(entry_list2)
    return len(entry_list2)
def rssCrawler(link_id, num, categ_formal, keystr_formal, project_idd, lang, lebel):
    from langdetect import detect
    link, idd= link_id
    final_url = 'https://ajax.googleapis.com/ajax/services/feed/load?v=1.0&q='+link+'&num='+str(num)
    response_load = urllib.urlopen(final_url)
    results_load = json.loads(response_load.read())
    #print "result_loads===============>>>>>", results_load
    feedtext_list = []
    no_of_entry = 0
    if results_load['responseStatus']==200:
        for feedtext in results_load['responseData']['feed']['entries']:
            text = (strip_tags(feedtext['content'])).lower()
            #print text, '===\n'
            allowed_words, not_allowed_words = allowedAndNOt(keystr_formal)
            if any(word in text for word in allowed_words) and not any(words in text for words in not_allowed_words):# write proper condition
                feedtext['project_id']= project_idd
                feedtext['keyword'] = keystr_formal
                feedtext['sentiment'] = ''
                feedtext['location'] = 'N/A'
                feedtext['keywords'] = []
                try:
                    url_text = "http://ws.detectlanguage.com/0.2/detect?q={text}&key=c46f5c863e4957da6511c50da60a46c1".format(text=strip_tags(text.encode('utf-8')))
                    text_language_response = urllib.urlopen(url_text)
                    lang_result = json.loads(text_language_response.read())
                    text_language = lang_result['data']['detections'][0]['language']
                    feedtext['lang'] =text_language
                except:
                    feedtext['lang']="en"
                feedtext['category'] = categ_formal
                feedtext['languages'] = lang
                feedtext['lebel'] = lebel
                if feedtext['publishedDate']!='':
                    feed_date = feedtext['publishedDate'][0:25]+' 0000'
                    feedtext['publishedDate'] = datetime.datetime.strptime(feed_date, "%a, %d %b %Y %H:%M:%S 0000")
                else:
                    feedtext['publishedDate']=datetime.datetime.now()
                source_temp = link
                source_temp1= source_temp.replace('www.', '')
                source_temp2 = find_between(source_temp1, '//', '.')
                feedtext['source'] = source_temp2
                feedtext_list.append(feedtext.copy())
                no_of_entry+=1
        if no_of_entry > 0:
            print 'dhukeche'
            client.sociabyte.rss_response.insert(feedtext_list)
        client.sociabyte.feed_link.update({'_id':ObjectId(idd)}, {'$set':{'fetched':'yes'}})
        return len(feedtext_list)
    else:
        return 0
def dataFeed(arg_formal):
    #print "arg_formal====>>>>>", arg_formal
    feedlink_count=0
    keystr, project_id_formal, acc_type, categ, lang, lebel = arg_formal
    feed_link_response = client.sociabyte.feed_link.find({'keyword':keystr, 'project_id':project_id_formal, 'fetched':'No'}, {'url':1})
    for url_list in feed_link_response:
        #print url_list
        link_id_no_list = []
        if url_list['url']!='':
            link_id_no_list.append(url_list['url'])
            link_id_no_list.append(url_list['_id'])
            feedlink_count=rssCrawler(link_id_no_list, 50, categ, keystr, project_id_formal, lang, lebel)
    return feedlink_count"""
def dataFeedRss(arg_formal):

    rss_count = 0
    pair, all_sources, countries, project_id, remaining_article = arg_formal
    print(' ========== I am in dataFeedRss =======================arg_formal_remaining_article ===========',remaining_article)
    for res in all_sources:
        if remaining_article>0:
            rss_count=rss_count+fetchRssFeeds(res, countries, pair['name'], pair['query'], project_id, pair['languages'], pair['labels'], pair['sub_topic'])
            remaining_article = remaining_article-rss_count
    return rss_count
def fetchRssFeeds(link_rss, loc, category, keyStr, project_id_formal, lang, lebel, sub_topic):
    import feedparser
    allowed_words, not_allowed_words = allowedAndNOt(keyStr)
    print "all words====================>>>>>>>>>>",allowed_words, not_allowed_words
    info = feedparser.parse(link_rss)
    loop_through = len(info.entries)
    #print "info=================================>>>", info

    try:
        if info.status == 200:
            i = 0
            count =0
            while(i<loop_through):
                data = {}
                data['categories']=[]
                data['author'] = 'Anonymous'
                try:
                    data['content'] = info.entries[i].summary_detail.value.encode('utf-8')
                except:
                    data['content'] = ''
                try:
                    data['contentSnippet'] = info.entries[i].summary.encode('utf-8')
                except:
                    data['contentSnippet'] = ''
                try:
                    #print("data['Content']=======================",data['content'])
                    data['link'] = info.entries[i].link.encode('utf-8')
                    #print("data['link']==========",info.entries[i].link)
                    Url = info.entries[i].link.encode('utf-8')
                    print("Url============",Url)
                    def domainFunc(url):
                        parsed_uri = urlparse(str(url))
                        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
                        return domain
                    domain_name = domainFunc(Url)
                    data['domain_name'] = domain_name
                    #print('domain_name=========',domain_name)

                except:
                    data['link'] = 'none'
                    data['domain_name'] = 'none'
                try:
                    feedDate = info.entries[i].published.encode('utf-8')
                    feedDate_formated = feedDate[0:19]+ ' 0000'
                    data['publishedDate'] = datetime.datetime.strptime(feedDate_formated, "%Y-%m-%d %H:%M:%S 0000")
                except:
                    data['publishedDate'] = datetime.datetime.now()
                try:
                    source_temp = info.entries[i].summary_detail.base.encode('utf-8')
                    source_temp1= source_temp.replace('www.', '')
                    source_temp2 = find_between(source_temp1, '//', '.')
                    data['source'] = source_temp2
                except:
                    data['source'] = 'none'
                try:
                    data['title'] = info.entries[i].title.encode('utf-8')
                except:
                    data['title'] = 'none'
                data['keyword']=keyStr
                data['keywords'] = []
                data['project_id'] = project_id_formal
                data['sentiment'] = ''
                data['category'] = category
                data['modified_status'] = 'not_modified'
                try:
                    country_loc = client.sociabyte.mainstream_sources.find({'feed_link':link_rss}, {'_id':0, 'country':1})
                    for country in country_loc:
                        data['location'] = country['country']
                except:
                    data['location'] = ''
                data['languages'] = lang
                data['lebel'] = lebel
                data['sub_topic'] = sub_topic
                try:
                    url_text = "http://ws.detectlanguage.com/0.2/detect?q={text}&key=c46f5c863e4957da6511c50da60a46c1".format(text=strip_tags(data['content'].encode('utf-8')))
                    text_language_response = urllib.urlopen(url_text)
                    lang_result = json.loads(text_language_response.read())
                    text_language = lang_result['data']['detections'][0]['language']
                    data['lang'] = text_language
                except:
                    data['lang']="en"
                #print data
                #print "\n"
                i = i+1
                if any(word in data['content'] for word in allowed_words) and not any(words in data['content'] for words in not_allowed_words):
                    #if detect(data['content'].encode('utf-8')) in lang:
                    count = global_settings.db_conn.sociabyte.alexa_domain_details.find({'domain_name' : domain_name}).count()
                    if count == 0:
                        print('entry')
                        alexa_object = {'domain_name' : domain_name,
                                        'fetched_status' : 'not_fetched'}
                        global_settings.db_conn.sociabyte.alexa_domain_details.insert(alexa_object)
                    else:
                        fetched_date_res = global_settings.db_conn.sociabyte.alexa_domain_details.find({'domain_name' : domain_name},{'fetched_date' : 1,'_id' : 0})
                        for existing_fetched_date in fetched_date_res:
                            fetched_date = existing_fetched_date['fetched_date']
                        if fetched_date > (StrToDatetime(time.strftime("%Y-%m-%d")) - timedelta(183)):
                            rank_obj = global_settings.db_conn.sociabyte.alexa_domain_details.findOne({'domain_name' : domain_name},{'alexa_data.UrlInfo.Rank' : 1,'_id' : 0})
                            rank = rank_obj['alexa_data']['UrlInfo']['Rank']
                            data['rank'] = rank
                        else:
                            pass

                    print "Got data ============================================================", data
                    client.sociabyte.rss_response.insert(data)
                    count+=1
            return count
        else:
            pass
    except:
        return 0
def alexaDataCrawling():
    print('i am in Alexa data Crawling')
    six_months_before_date = StrToDatetime(time.strftime("%Y-%m-%d")) - timedelta(183)
    print('six_months_before_date===============================',six_months_before_date)
    alexa_db_res = global_settings.db_conn.sociabyte.alexa_domain_details.find( { '$or': [ { 'fetched_date': { '$lt':six_months_before_date}},{ 'fetched_status': 'not_fetched' } ] } )
    for each_alexa_object in alexa_db_res:
        domain_name = each_alexa_object['domain_name']
        import alexa_demo as alexa
        alexa_rank= alexa.AlexaCrawler(domain_name)
        print('Alexa Data crawling done for single link')
        update_status = global_settings.db_conn.sociabyte.rss_response.update({"domain_name" : domain_name},{ '$set': { 'rank': alexa_rank }},multi = True)
        print(update_status)
    return 'Alexa Data Fetch Completed'

def fetchTwitterFeeds(arg_formal, twitter_handle_formal):
    from langdetect import detect
    #print "tw_arg_formal", arg_formal
    tweets_list = []
    pair, countries, project_id, remaining_article = arg_formal
    cat = pair['name']
    query = pair['query']
    lebel = pair['labels']
    languages = pair['languages']
    project_id_formal = project_id
    sub_topic = pair['sub_topic']
    twitter_res= twitter_handle_formal.search(q = query, count = 100)
    #print "twitter res len===>>>>", len(twitter_res['statuses'])
    i=0
    data = {
            'category':"",
            'categories':"",
            'link':"",
            'source':"",
            'title': "",
            'content':"",
            'contentSnippet':"",
            'author':"",
            'publishedDate':"",
            'gender':"",
            'keyword':"",
            'keywords':[],
            'project_id':"",
            'sentiment':"",
            'location':"",
            'lang':"",
            'lebel':lebel,
            'languages':languages,
            'sub_topic':'',
            'modified_status':'not_modified'
        }
    for tweets in twitter_res['statuses']:
        data['sub_topic'] = sub_topic
        data['category'] = cat
        data['categories']=[]
        data['link'] = 'https://twitter.com/'+tweets['user']['screen_name']+'/status/'+tweets['id_str']
        data['source'] = "Twitter"
        data['title'] = ''
        try:
            data['content'] = tweets['text'].encode('utf-8')
        except:
            data['content'] = ''
        try:
            data['contentSnippet'] = tweets['text'].encode('utf-8')
        except:
            data['contentSnippet'] = ''
        try:
            data['author'] = tweets['user']['name'].encode('utf-8')
        except:
            data['author'] = 'Anonymous'
        try:
            data['user_id'] = tweets['user']['screen_name']
        except:
            pass
        try:
            data['publishedDate'] = datetime.datetime.strptime(tweets['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
        except:
            data['publishedDate'] = datetime.datetime.now()
        try:
            data['gender']=genderClassifier(tweets['user']['name'].encode('utf-8'))
        except:
            data['gender']='Unknown'
        data['keyword']=query
        data['keywords'] = []
        data['project_id'] = project_id_formal
        data['sentiment'] = ''
        data['location'] = tweets['user']['location']
        try:
            url_text = "http://ws.detectlanguage.com/0.2/detect?q={text}&key=c46f5c863e4957da6511c50da60a46c1".format(text=strip_tags(data['content'].encode('utf-8')))
            text_language_response = urllib.urlopen(url_text)
            lang_result = json.loads(text_language_response.read())
            text_language = lang_result['data']['detections'][0]['language']
            data['lang'] = text_language
        except:
            data['lang']="en"
        try:
            data['media_url']=[media_url_list['media_url'] for media_url_list in tweets['entities']['media']]
            #print "media url =======================>>>>>>>>>>>>>",data['media_url']
            #print "media url =======================>>>>>>>>>>>>>",tweets['entities']['media']
        except:
            #print "media url =======================>>>>>>>>>>>>>in except"
            pass
        try:
            data['author_pic'] = tweets['user']['profile_image_url']
        except:
            pass
        #print "dataTwitter=====>>>>>", data
        tweets_list.append(data.copy())
        i+=1
    client.sociabyte.rss_response.insert(tweets_list)
    return i
def fetchVkFeeds(arg_formal):
    from langdetect import detect
    pair, countries, project_id, remaining_article = arg_formal
    cat = pair['name']
    query = pair['query']
    lebel = pair['labels']
    languages = pair['languages']
    project_id_formal = project_id
    sub_topic = pair['sub_topic']
    import json
    vk_feed_list = []
    vkfeed_res = urllib.urlopen("https://api.vk.com/method/newsfeed.search?v=5.21&q="+query+"&appid=4451246")
    vk_res = json.loads(vkfeed_res.read())
    i=0
    for res in vk_res['response']['items']:
        print('res====================',res)
        data = {}
        data['categories']=[]
        try:
            if res['owner_id']>0:
                data['link'] = 'https://vk.com/id'+str(res['owner_id'])+'?w=wall'+str(res['owner_id'])+'_'+str(res['id'])+'%2Fall'
                profile_fields = "sex, bdate, city, country, photo_50, contacts, followers_count, status, counters"
                user_info = urllib.urlopen('https://api.vk.com/method/users.get?v=5.8&user_id='+str(res['owner_id'])+'&fields='+profile_fields)
                user_info_res = json.loads(user_info.read())
                for user_detail in user_info_res['response']:
                    try:
                        data['author'] = user_detail['first_name']+' '+user_detail['last_name']
                    except:
                        data['author'] = 'Anonymous'
                    try:
                        data['author_pic'] = user_detail['photo_50']
                    except:
                        pass
                    try:
                        data['gender'] = 'female' if user_detail['sex'] is 1 else 'male' if user_detail['sex'] is 2 else 'Unknown'
                    except:
                        data['gender'] = 'Unknown'
                    try:
                        data['location']=user_detail['country']['title']
                    except:
                        data['location'] = ''
                    data['isCommunity'] = 0
            else:
                data['link']= 'https://vk.com/club'+str(abs(res['owner_id']))+'?w=wall'+str(res['owner_id'])+'_'+str(res['id'])+'%2Fall'
                profile_fields = "city, country, photo, contacts, status, counters, name, screen_name "
                user_info = urllib.urlopen('https://api.vk.com/method/groups.getById?v=5.48&group_ids='+str(abs(res['owner_id']))+'&fields='+profile_fields)
                user_info_res = json.loads(user_info.read())
                for user_detail in user_info_res['response']:
                    try:
                        data['author'] = user_detail['name']
                    except:
                        data['author'] = 'Anonymous'
                    try:
                        data['author_pic'] = user_detail['photo_50']
                    except:
                        pass
                    data['gender'] = 'Unknown'
                    try:
                        data['location']=user_detail['country']['title']
                    except:
                        data['location'] = ''
                    data['isCommunity'] = 1
        except:
            data['author'] = 'Anonymous'
            data['gender'] = 'Unknown'
            data['location'] =''
            data['isCommunity'] = 2

        data['source'] = "Vkontakte"
        data['title'] = ''
        try:
            data['content'] = res['text'].encode('utf-8')
        except:
            data['content'] = ''
        try:
            data['contentSnippet'] = res['text'].encode('utf-8')
        except:
            data['contentSnippet'] = ''
        try:
            data['user_id'] = res['owner_id']
        except:
            pass
        try:
            vktime = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(res['date'])))
            data['publishedDate'] = datetime.datetime.strptime(vktime, "%Y-%m-%dT%H:%M:%SZ")
        except:
            data['publishedDate'] = datetime.datetime.now()
        data['keyword']=query
        data['keywords'] = []
        data['project_id'] = project_id_formal
        data['sentiment'] = ''
        data['category'] = cat
        data['lebel'] = lebel
        data['languages'] = languages
        data['sub_topic'] = sub_topic
        data['modified_status'] = 'not_modified'
        try:
            data['media_url'] = res['attachments']
        except:
            pass
        try:
            url_text = "http://ws.detectlanguage.com/0.2/detect?q={text}&key=c46f5c863e4957da6511c50da60a46c1".format(text=strip_tags(data['content'].encode('utf-8')))
            text_language_response = urllib.urlopen(url_text)
            lang_result = json.loads(text_language_response.read())
            text_language = lang_result['data']['detections'][0]['language']
            data['lang'] = text_language
        except:
            data['lang']="en"
        print "dataVK====>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", data
        vk_feed_list.append(data.copy())
        i+=1
    if len(vk_feed_list) != 0:
        client.sociabyte.rss_response.insert(vk_feed_list)
    else:
        pass
    return i
def doSomeCalculation(project_id_formal, db_connection_formal):
    all_response = db_connection_formal.sociabyte.rss_response.find({'project_id':project_id_formal}, {'_id':0})
    final_json_response = {
        'feeds':[],
        'pychart':[],
        'wordCloud':[]
    }
    positive_count = negative_count = neutral_count = 0
    words = []
    links = []
    for all in all_response:
        all_snipet= all.copy()
        all_snipet.pop('categories')
        all_snipet.pop('keyword')
        all_snipet.pop('keywords')
        try:
            all_snipet['lang']=langName(all_snipet['lang'])
        except:
            all_snipet['lang']='undefined'
        print type(all_snipet['publishedDate'])
        all_snipet['publishedDate'] =time.mktime((all_snipet['publishedDate']).timetuple())
        if all_snipet['author']=='':
            all_snipet['author']= 'Anonymous'
        final_json_response['feeds'].append(all_snipet)
        if all['sentiment']=='pos':
            positive_count+=1
        if all['sentiment'] == 'neg':
            negative_count+=1
        if all['sentiment'] == 'neutral':
            neutral_count+=1
        words.extend(all['keywords'])
        links.append(all['link'])
    final_json_response['pychart']=[['Positive', positive_count], ['Negative', negative_count], ['Neutral', neutral_count]]
    word_set = list(set(words))
    word_count = []
    for a in word_set:
        word_count.append(words.count(a))
    final_wordCounts = zip(word_set, word_count, links)
    for wc in final_wordCounts:
        temp_json = {
            'text':wc[0],
            'weight':wc[1],
            'link':wc[2]
        }
        final_json_response['wordCloud'].append(temp_json)
    return final_json_response

def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""
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
def allowedAndNOt(keystr_formal):
    import copy
    keystr_formal = keystr_formal.lower()
    keyall_list = keystr_formal.split(' ')
    allowed = []
    not_allowed =[]
    try:
        index_near = [i for i, x in enumerate(keyall_list) if x == "near"]
    except:
        index_near = []
    try:
        index_not = [i for i, x in enumerate(keyall_list) if x == "not"]
    except:
        index_not = []
    try:
        index_and = [i for i, x in enumerate(keyall_list) if x == "and"]
    except:
        index_and = []
    try:
        index_or = [i for i, x in enumerate(keyall_list) if x == "or"]
    except:
        index_or = []
    try:
        index_in = [i for i, x in enumerate(keyall_list) if x == "in"]
    except:
        index_in = []
    index_arr = index_and+index_in+index_near+index_not+index_or
    sorted_index_arr = copy.copy(index_arr)
    sorted_index_arr.sort()
    #print "sorted index arr===================>>>>>>>>>>>>>", sorted_index_arr
    for not_indexes in index_not:
        if(sorted_index_arr.index(not_indexes)+1!=len(sorted_index_arr)):
            print "not indexes", not_indexes, sorted_index_arr[sorted_index_arr.index(not_indexes)+1]
            not_allowed.extend(keyall_list[not_indexes+1:sorted_index_arr[sorted_index_arr.index(not_indexes)+1]])
        else:
            not_allowed.extend(keyall_list[not_indexes+1:])
    allowed = list(set(keyall_list)-set(['near', 'not', 'and', 'or','in']))
    allowed = list(set(allowed)-set(not_allowed))
    return allowed, not_allowed
def langName(code):
    lang_dict = {
        "cs":"Czech",
        "en":"English",
        "fr":"French",
        "it":"Italian",
        "pt":"Portuguese",
        "sl":"Slovene",
        "tr":"Turkish",
        "da":"Danish",
        "et":"Estonian",
        "de":"German",
        "no":"Norwegian",
        "es":"Spanish",
        "nl":"Dutch",
        "fi":"Finnish",
        "el":"Greek",
        "pl":"Polish",
        "sv":"Swedish"
    }
    return lang_dict[code]
def gender_features(word):
    return {'last_letter': word[-1]}
def genderClassifier(author):
    labeled_names = ([(name, 'male') for name in names.words('male.txt')] + [(name, 'female') for name in names.words('female.txt')])
    random.shuffle(labeled_names)
    featuresets = [(gender_features(n), gender) for (n, gender) in labeled_names]
    classifier = nltk.NaiveBayesClassifier.train(featuresets)
    try:
        first_name = author.split(' ')
        gender = classifier.classify(gender_features(first_name[0]))
    except:
        gender = 'male'
    return gender
