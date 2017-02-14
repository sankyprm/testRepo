# -*- coding: iso-8859-15 -*-
import urllib
import json
from pymongo import MongoClient
import xml
import global_settings
client = global_settings.db_conn
from HTMLParser import HTMLParser
from bson.objectid import ObjectId
import datetime
import time
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

def FeedUrlFetch(arg_formal):
    from langdetect import detect
    category, keystr, project_id_formal = arg_formal
    url = 'https://ajax.googleapis.com/ajax/services/feed/find?' +'v=1.0&q='+keystr+'&userip=50.18.45.172'
    response = urllib.urlopen(url)
    results = json.loads(response.read())
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
        "category":category
    }
    entry_list2 = []
    for result in results['responseData']['entries']:
        entry_to_rss_json['contentSnippet']=entry_to_rss_json['content']=result['contentSnippet']= strip_tags(result['contentSnippet'])
        entry_to_rss_json['title']=result['title'] = strip_tags(result['title'])
        entry_to_rss_json['keyword']=result['keyword']=keystr
        entry_to_rss_json['project_id']=result['project_id'] = (project_id_formal)
        entry_to_rss_json['lang']=detect(entry_to_rss_json['content'].encode('utf-8'))
        result['fetched'] = 'No'
        entry_to_rss_json['link']= result['link']
        source_temp = result['link']
        source_temp1= source_temp.replace('www.', '')
        source_temp2 = find_between(source_temp1, '//', '.')
        entry_to_rss_json['source']= source_temp2
        client.sociabyte.feed_link.save(result.copy())
        client.sociabyte.rss_response.save(entry_to_rss_json.copy())
def rssCrawler(link_id, num, categ_formal, keystr_formal, project_idd, lang):
    from langdetect import detect
    link, idd= link_id
    final_url = 'https://ajax.googleapis.com/ajax/services/feed/load?v=1.0&q='+link+'&num='+str(num)
    response_load = urllib.urlopen(final_url)
    results_load = json.loads(response_load.read())
    print "result_loads===============>>>>>", results_load
    feedtext_list = []
    no_of_entry = 0
    if results_load['responseStatus']==200:
        for feedtext in results_load['responseData']['feed']['entries']:
            text = (strip_tags(feedtext['content'])).lower()
            print text, '===\n'
            allowed_words, not_allowed_words = allowedAndNOt(keystr_formal)
            if any(word in text for word in allowed_words) and not any(words in text for words in not_allowed_words):# write proper condition
                feedtext['project_id']= project_idd
                feedtext['keyword'] = keystr_formal
                feedtext['sentiment'] = ''
                feedtext['location'] = 'N/A'
                feedtext['keywords'] = []
                feedtext['lang'] =detect(data['content'].encode('utf-8'))
                feedtext['category'] = categ_formal
                if feedtext['publishedDate']!='':
                    feed_date = feedtext['publishedDate'][0:25]+' 0000'
                    feedtext['publishedDate'] = datetime.datetime.strptime(feed_date, "%a, %d %b %Y %H:%M:%S 0000")
                else:
                    feedtext['publishedDate']=datetime.datetime.now()
                client.sociabyte.rss_response.save(feedtext.copy())
                no_of_entry+=1
        client.sociabyte.feed_link.update({'_id':ObjectId(idd)}, {'$set':{'fetched':'yes'}})
    else:
        pass
def dataFeed(arg_formal):
    print "arg_formal====>>>>>", arg_formal
    keystr, project_id_formal, acc_type, categ, lang = arg_formal
    feed_link_response = client.sociabyte.feed_link.find({'keyword':keystr, 'project_id':project_id_formal, 'fetched':'No'}, {'url':1})
    for url_list in feed_link_response:
        print url_list
        link_id_no_list = []
        if url_list['url']!='':
            link_id_no_list.append(url_list['url'])
            link_id_no_list.append(url_list['_id'])
            rssCrawler(link_id_no_list, 50, categ, keystr, project_id_formal, lang)
def dataFeedRss(arg_formal):
    keytosearch, project_id, account_type, categ, country_list, sources, lang = arg_formal
    if len(sources)==0:
        rss_sources_res = client.sociabyte.mainstream_sources.find({'country':{'$in':country_list}}, {'_id':0})
        for res in rss_sources_res:
            fetchRssFeeds(res['url'], res['country'], categ, keytosearch, project_id, lang)
    else:
        for res in sources:
            fetchRssFeeds(res, 'not available', categ, keytosearch, project_id, lang)
def fetchRssFeeds(link_rss, loc, category, keyStr, project_id_formal, lang):
    import feedparser
    from langdetect import detect
    info = feedparser.parse(link_rss)
    loop_through = len(info.entries)
    if info.status == 200:
        i = 0
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
                data['link'] = info.entries[i].link.encode('utf-8')
            except:
                data['link'] = 'none'
            try:
                feedDate = info.entries[i].published.encode('utf-8')
                feedDate_formated = feedDate[0:19]+ ' 0000'
                data['publishedDate'] = datetime.datetime.strptime(feedDate_formated, "%Y-%m-%d %H:%M:%S 0000")
            except:
                data['publishedDate'] = datetime.datetime.now()
            try:
                data['source'] = info.entries[i].summary_detail.base.encode('utf-8')
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
            data['location'] = loc
            data['lang']=detect(data['content'].encode('utf-8'))
            print data
            print "\n"
            i = i+1
            allowed_words, not_allowed_words = allowedAndNOt(keyStr)
            print "all words====================>>>>>>>>>>",allowed_words, not_allowed_words
            if any(word in data['content'] for word in allowed_words) and not any(words in data['content'] for words in not_allowed_words):
                if detect(data['content'].encode('utf-8')) in lang:
                    client.sociabyte.rss_response.save(data)
def fetchTwitterFeeds(arg_formal, twitter_handle_formal):
    from langdetect import detect
    print "tw_arg_formal", arg_formal
    cat, query, project_id_formal = arg_formal
    twitter_res= twitter_handle_formal.search(q = query, count = 100)
    for tweets in twitter_res['statuses']:
        data = {}
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
            data['publishedDate'] = datetime.datetime.strptime(tweets['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
        except:
            data['publishedDate'] = datetime.datetime.now()
        data['keyword']=query
        data['keywords'] = []
        data['project_id'] = project_id_formal
        data['sentiment'] = ''
        data['location'] = tweets['user']['location']
        data['lang'] = detect(data['content'].encode('utf-8'))
        print "dataTwitter=====>>>>>", data
        client.sociabyte.rss_response.save(data)
def fetchVkFeeds(arg_formal):
    from langdetect import detect
    cat, query, project_id_formal= arg_formal
    import json
    vkfeed_res = urllib.urlopen("https://api.vk.com/method/newsfeed.search?v=5.21&q="+query+"&appid=4451246")
    vk_res = json.loads(vkfeed_res.read())
    for res in vk_res['response']['items']:
        data = {}
        data['categories']=[]
        if res['owner_id']>0:
            data['link'] = 'https://vk.com/id'+str(res['owner_id'])+'?w=wall'+str(res['owner_id'])+'_'+str(res['id'])+'%2Fall'
        else:
            data['link']= '#'
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
        data['author'] = 'Anonymous'
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
        data['lang'] = detect(data['content'].encode('utf-8'))
        loc_res = urllib.urlopen('https://api.vk.com/method/users.get?user_id='+str(res['from_id'])+'&fields=country')
        loc_res_formated = json.loads(loc_res.read())
        print "loc_res_formated======>>>>", loc_res_formated
        country_name = ''
        if loc_res_formated.has_key('error'):
            pass
        else:
            country_response = client.sociabyte.vk_country.find({'id':loc_res_formated['response'][0]['country']})
            for country in country_response:
                country_name = country['name']
            if country_name == '':
                country_name=countryFinder(loc_res_formated['response'][0]['country'], client.sociabyte, json, urllib)
        data['location'] = country_name
        print "dataVK====>>>>", data
        client.sociabyte.rss_response.save(data)
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
        all_snipet['lang']=langName(all_snipet['lang'])
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
    keyall_list = keystr_formal.split(' ')
    allowed = []
    not_allowed =[]
    try:
        index_near = keyall_list.index('near')
    except:
        index_near = -1
    try:
        index_not = keyall_list.index('not')
    except:
        index_not = -1
    try:
        index_and = keyall_list.index('and')
    except:
        index_and = -1
    try:
        index_or = keyall_list.index('or')
    except:
        index_or = -1
    try:
        index_in = keyall_list.index('in')
    except:
        index_in = -1
    index_arr = [index_near, index_not, index_and, index_or, index_in]
    sorted_index_arr = copy.copy(index_arr)
    sorted_index_arr.sort()
    print "sorted index arr===================>>>>>>>>>>>>>", sorted_index_arr
    index_counter = 0
    while index_counter<len(index_arr):
        if sorted_index_arr[index_counter]== index_near:
            if index_counter==0:
                allowed.extend(keyall_list[0:index_near-1])
                allowed.extend(keyall_list[index_near+1:sorted_index_arr[index_counter-1]-1])
            elif index_near==-1:
                pass
            else:
                allowed.extend(keyall_list[sorted_index_arr[index_counter-1]+1:index_near-1])
                allowed.extend(keyall_list[index_near+1:sorted_index_arr[index_counter-1]-1])
        if sorted_index_arr[index_counter]== index_not:
            if index_counter==0:
                allowed.extend(keyall_list[0:index_not-1])
                not_allowed.extend(keyall_list[index_not+1:sorted_index_arr[index_counter-1]-1])
            elif index_not==-1:
                pass
            else:
                allowed.extend(keyall_list[sorted_index_arr[index_counter-1]+1:index_not-1])
                not_allowed.extend(keyall_list[index_not+1:sorted_index_arr[index_counter-1]-1])
        if sorted_index_arr[index_counter]== index_and:
            if index_counter==0:
                allowed.extend(keyall_list[0:index_and-1])
                allowed.extend(keyall_list[index_and+1:sorted_index_arr[index_counter-1]-1])
            elif index_and==-1:
                pass
            else:
                allowed.extend(keyall_list[sorted_index_arr[index_counter-1]+1:index_and-1])
                allowed.extend(keyall_list[index_and+1:sorted_index_arr[index_counter-1]-1])
        if sorted_index_arr[index_counter]== index_or:
            if index_counter==0:
                allowed.extend(keyall_list[0:index_or-1])
                allowed.extend(keyall_list[index_or+1:sorted_index_arr[index_counter-1]-1])
            elif index_or==-1:
                pass
            else:
                allowed.extend(keyall_list[sorted_index_arr[index_counter-1]+1:index_or-1])
                allowed.extend(keyall_list[index_or+1:sorted_index_arr[index_counter-1]-1])
        if sorted_index_arr[index_counter]== index_in:
            if index_counter==0:
                allowed.extend(keyall_list[0:index_in-1])
                allowed.extend(keyall_list[index_in+1:sorted_index_arr[index_counter-1]-1])
            elif index_in==-1:
                pass
            else:
                allowed.extend(keyall_list[sorted_index_arr[index_counter-1]+1:index_in-1])
                allowed.extend(keyall_list[index_in+1:sorted_index_arr[index_counter-1]-1])
        index_counter+=1
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

