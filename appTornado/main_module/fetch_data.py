__author__ = 'soumik'
__author__ = 'soumik'
import datetime
from pymongo import MongoClient


def fetchRssFeeds(link_rss, db_conn):
    import feedparser
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
            data['keywords'] = []
            data['sentiment'] = ''
            db_conn.news.insert(data)
            i=i+1
def main():
    import zmq
    import json
    import time
    import global_settings
    client = global_settings.db_conn
    db_connection = client.sociabyte
    list_source = ["http://feeds.feedburner.com/NdtvNews-TopStories",
"http://feeds.feedburner.com/NDTV-Trending",
"http://feeds.feedburner.com/NDTV-Business",
"http://feeds.feedburner.com/ndtv/Lsgd",
"http://feeds.feedburner.com/ndtv/TqgX",
"http://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms",
"http://timesofindia.indiatimes.com/rssfeedstopstories.cms",
"http://timesofindia.indiatimes.com/rssfeeds/296589292.cms",
"http://www.ibnlive.com/rss/business.xml",
"http://www.ibnlive.com/rss/india.xml",
"http://www.ibnlive.com/rss/politics.xml"]
    for source in list_source:
        fetchRssFeeds(source, db_connection)
    sendDict = {
            'code':'news'
        }
    sendDict = json.dumps(sendDict)
    port = '5574'
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
main()




