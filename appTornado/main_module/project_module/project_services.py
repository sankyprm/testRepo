__author__ = 'sanky_cse'
import urllib2
import simplejson
from pymongo import MongoClient
import xml
import global_settings
client = global_settings.db_conn
from HTMLParser import HTMLParser
from bson.objectid import ObjectId

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
    keystr, project_id_formal = arg_formal
    url = ('https://ajax.googleapis.com/ajax/services/feed/find?' +
       'v=1.0&q='+keystr+'&userip=50.18.124.225')

    request = urllib2.Request(url, None, {'Referer': 'http://sociabyte.com'})
    response = urllib2.urlopen(request)
    results = simplejson.load(response)
    entry_list = []
    for result in results['responseData']['entries']:
        result['contentSnippet']= strip_tags(result['contentSnippet'])
        result['title'] = strip_tags(result['title'])
        result['keyword']=keystr
        result['project_id'] = project_id_formal
        result['fetched'] = 'No'
        entry_list.append(result.copy())
    client.sociabyte.feed_link.insert(entry_list)
def rssCrawler(link_id, num, keyAll_formal, keystr_formal, project_idd):
    link, idd= link_id
    final_url = 'https://ajax.googleapis.com/ajax/services/feed/load?v=1.0&q='+link+'&num='+str(num)
    request_load = urllib2.Request(final_url, None, {'Referer': 'http://sociabyte.com'})
    response_load = urllib2.urlopen(request_load)
    results_load = simplejson.load(response_load)
    feedtext_list = []
    no_of_entry = 0
    for feedtext in results_load['responseData']['feed']['entries']:
        text = strip_tags(feedtext['content'])
        if any(word in text for word in keyAll_formal):
            feedtext['project_id']= project_idd
            feedtext['keyword'] = keystr_formal
            feedtext['similar_keywords'] = keyAll_formal
            feedtext_list.append(feedtext.copy())
            no_of_entry+=1
    if no_of_entry > 0:
        print 'dhukeche'
        client.sociabyte.rss_response.insert(feedtext_list)
    client.sociabyte.feed_link.update({'_id':ObjectId(idd)}, {'$set':{'fetched':'yes'}}, multi=True, upsert=False)
    till_now_entry = client.sociabyte.project_artical_count.find({'_id':project_idd}, {'artical_count':1, '_id':0})
    if till_now_entry is None:
        artical_count_json = {
            '_id': project_idd,
            'artical_count': no_of_entry
        }
        client.sociabyte.project_artical_count.insert(artical_count_json)
    else:
        artical_count = 0
        for val in till_now_entry:
            artical_count = artical_count+val['artical_count']
        artical_count = artical_count+no_of_entry
        client.sociabyte.project_artical_count.update({'_id':project_idd}, {'$set':{'artical_count':artical_count}}, upsert=False)

def dataFeed(arg_formal):
    #print arg_formal
    keystr, project_id_formal, acc_type, keyAll = arg_formal
    feed_link_response = client.sociabyte.feed_link.find({'keyword':keystr, 'project_id':project_id_formal, 'fetched':'No'}, {'url':1})
    for url_list in feed_link_response:
        link_id_no_list = []
        link_id_no_list.append(url_list['url'])
        link_id_no_list.append(url_list['_id'])
        if acc_type=='1':
            rssCrawler(link_id_no_list, 100, keyAll, keystr, project_id_formal)
        elif acc_type=='2':
            rssCrawler(link_id_no_list, 100, keyAll, keystr, project_id_formal)
        elif acc_type=='3':
            rssCrawler(link_id_no_list, 100, keyAll, keystr, project_id_formal)
        else:
            print 'Case khelam'



def data_fetch_twitter(client, keyStr, twitter):
    db = client.tweet_database
    if db.tweet_worlds.count() > 0:
            last_element = db.tweet_worlds.find().sort( [( '_id' , -1) ] ).limit(1)
            last_element_id = last_element[0]['id']


            data = twitter.search(q = keyStr, count = 100, since_id = last_element_id)
            response = data['statuses']
            response_metadata = data['search_metadata']



            for i in range(len(response)):
                text = response[i]['text']
                tweet_id = response[i]['id']

                tweet_dict = { 'text' : text,
                               'id' : tweet_id,
                              }
                final_tweet_list.append(tweet_dict)

                #tweet_worlds.insert(tweet_dict)

    else:
            data = twitter.search(q = keyStr, count = 100)
            response = data['statuses']
            response_metadata = data['search_metadata']



            for i in range(len(response)):
                text = response[i]['text']
                tweet_id = response[i]['id']

                tweet_dict = { 'text' : text,
                               'id' : tweet_id,
                              }
                final_tweet_list.append(tweet_dict)

                #tweet_worlds.insert(tweet_dict)


        #print (tweet_dict)
    return final_tweet_list

def data_fetch_vkontakte(client, keyStr, urllib_formal, json_formal):
    db = client.vk_database
    if db.vk_world.count() > 0:
            asc_collection = db.vk_world.aggregate([{ '$sort' : { 'date' : -1}}])
            last_element_date = str(asc_collection['result'][0]['date'])
            current_timestamp = str(time.time())
            url = ('https://api.vk.com/method/newsfeed.search?v=5.21&q='+keyStr+'&start_time='+last_element_date+'&end_time='+current_timestamp+'&appid=4451246')
            req = urllib_formal.Request(url)
            res = urllib_formal.urlopen(req)
            results = json_formal.load(res)

            result_len = len(results['response']['items'] )

            for i in range(result_len):
                ids = results['response']['items'][i]['id']
                text = results['response']['items'][i]['text']

                vk_dict = {
                    'id' : ids,
                    'text' : text,
                   }
                final_vk_list.append(vk_dict)
                #vk_world.insert(vk_dict)


    else:

            url = ('https://api.vk.com/method/newsfeed.search?v=5.21&q='+keyStr+'&appid=4451246')
            req = urllib_formal.Request(url)
            res = urllib_formal.urlopen(req)
            result = json_formal.load(res)
            #print (result['response']['items'])
            #print(result)

            result_len = len(result['response']['items'] )

            for i in range(result_len):
                ids = result['response']['items'][i]['id']
                text = result['response']['items'][i]['text']

                vk_dict = {
                    'id' : ids,
                    'text' : text,
                   }
                final_vk_list.append(vk_dict)
                #vk_world.insert(vk_dict)

    return final_vk_list

def data_fetch_facebook(client, keyStr, urllib_formal, json_formal):
        url = 'https://graph.facebook.com/v1.0/search?access_token=1404603603151140|4TY1wX6QGhNFIsyolyaKAPjjJYM&type=post&q='+keyStr+'&limit=100'
        resultfb = urllib_formal.urlopen(url)
        resultfb = resultfb.read()
        result = json_formal.loads(resultfb)
        result_data_len = len(result)
        for i in range(result_data_len):

            id = result['data'][i]['id']
            text = ''
            #msg = result['data'][i]['message']
            #dsc = result['data'][i]['description']
            #cap = result['data'][i]['caption']

            if 'message' in result['data'][i]:
                text = result['data'][i]['message']
            else:
                if 'description' in result['data'][i]:
                    text = result['data'][i]['description']
                elif 'caption' in result['data'][i]:
                    text = result['data'][i]['caption']
                else:
                     pass
            fb_dict = {
                'id': id,
                'text' : text
            }
            final_facebook_list.append(fb_dict)
        return final_facebook_list