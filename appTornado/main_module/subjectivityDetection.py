import collections, itertools
import nltk.classify.util, nltk.metrics
from nltk.classify import NaiveBayesClassifier
from nltk.corpus import movie_reviews, stopwords
from nltk.collocations import BigramCollocationFinder
from nltk.metrics import BigramAssocMeasures
from nltk.probability import FreqDist, ConditionalFreqDist
import time
from featx import bag_of_words
from nltk.corpus import PlaintextCorpusReader
import sentimentDetection
import zmq
import json
import sys
import kewwordExtractors
from bson.objectid import ObjectId
from HTMLParser import HTMLParser
import global_settings
db_conn = global_settings.db_conn
corpus_root = "/home/ubuntu/sociabyte/appTornado/subjectivity/"
subjectivity = PlaintextCorpusReader(corpus_root, '.*')
subjective_fileLoc = "/home/ubuntu/sociabyte/appTornado/subjectivity/sub.txt"
objective_fileLoc = "/home/ubuntu/sociabyte/appTornado/subjectivity/obj.txt"
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

def best_word_feats(words):
    return dict([(word, True) for word in words if word in bestwords])

def best_bigram_word_feats(words, score_fn=BigramAssocMeasures.chi_sq, n=200):
    bigram_finder = BigramCollocationFinder.from_words(words)
    bigrams = bigram_finder.nbest(score_fn, n)
    d = dict([(bigram, True) for bigram in bigrams])
    d.update(best_word_feats(words))
    return d

def evaluate_classifier(featx, subjective_file, objective_file):
    negids = []
    posids =[]
    with open(subjective_file, 'r') as content_file:
        content_sub = content_file.read()
    with open(objective_file, 'r') as content_file:
        content_obj = content_file.read()
    negids_org = content_sub.split('.')
    posids_org = content_obj.split('.')
    for eachText in negids_org:
        negids.append(eachText.rstrip())
    for eachText in posids_org:
        posids.append(eachText.rstrip())
    #negfeats = [(featx(subjectivity.words('sub.txt')), 'sub') for ]
    #posfeats = [(featx(subjectivity.words('obj.txt')), 'obj')]
    negfeats = [(featx(f.split()), 'sub') for f in negids]
    posfeats = [(featx(f.split()), 'obj') for f in posids]

    negcutoff = len(negfeats)
    poscutoff = len(posfeats)

    trainfeats = negfeats[:negcutoff] + posfeats[:poscutoff]
    #print trainfeats
    testfeats = negfeats[negcutoff:] + posfeats[poscutoff:]
    #print testfeats
    testfeatsfile = open("/home/ubuntu/sociabyte/appTornado/main_module/testfeatsfile.txt", "w")
    print_neg = negfeats[:5]
    testfeatsfile.write(str(print_neg))
    testfeatsfile.close()
    classifier = NaiveBayesClassifier.train(trainfeats)
    return classifier

def fbSentiment(acc_id_formal, page_name, brand_id, user_id, sent_classifier_formal, pos_neg_classifier_formal):
    import urllib
    import json
    import sentiment_analyser_otherLang.spanish_sentiment_detector as spanish_sent
    import datetime
    comment_ids=db_conn.sociabyte.fb_comments.find({'from.id':acc_id_formal, 'sentiment':''}, {'post_id':1, 'message':1, '_id':0})
    for comment_id in comment_ids:
        sentiment = ''
        keywords = []
        input = comment_id['message'].lower()
        try:
            text_language_response = urllib.urlopen("http://ws.detectlanguage.com/0.2/detect?q="+input+"&key=f6493a9c926889dc6d44e68ba78e28fa")
            lang_result = json.loads(text_language_response.read())
            print "lang result1 ==>>>", lang_result

            text_language = lang_result['data']['detections'][0]['language']
        except:
            text_language = "en"
        replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
        for char in replace_char:
            input = input.replace(char, '')
        inputx = strip_tags(input)
        if text_language=='en':
            input = inputx.split()
            input = bag_of_words(input)
            subjectivity_check = str(sent_classifier_formal.classify(input))
            if subjectivity_check=='obj':
                sentiment = "neutral"
            else:
                sentiment = pos_neg_classifier_formal.classify(input)
            tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
            terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)

            for term in terms:
                for word in term:
                    keywords.append(word)
            print '\n==================\n',keywords
        elif text_language=='es':
            sentiment = spanish_sent.spanishSentimentDetector(inputx)
        db_conn.sociabyte.fb_comments.update({'from.id':acc_id_formal, 'post_id':comment_id['post_id']}, {'$set':{'sentiment':sentiment, 'keywords':keywords}})
    posts=db_conn.sociabyte.fb_post_details.find({'from_id':acc_id_formal, 'sentiment':''}, {'post_id':1, 'message':1, '_id':0})
    for post in posts:
        sentiment = ''
        keywords = []
        input = post['message'].lower()
        try:
            text_language_response = urllib.urlopen("http://ws.detectlanguage.com/0.2/detect?q="+input+"&key=f6493a9c926889dc6d44e68ba78e28fa")
            lang_result = json.loads(text_language_response.read())
            print "lang result ==>>>", lang_result

            text_language = lang_result['data']['detections'][0]['language']
        except:
            text_language = ""
        replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
        for char in replace_char:
            input = input.replace(char, '')
        inputx = strip_tags(input)
        if text_language=="en":
            input = inputx.split()
            input = bag_of_words(input)
            subjectivity_check = str(sent_classifier_formal.classify(input))
            if subjectivity_check=='obj':
                sentiment = "neutral"
            else:
                sentiment = pos_neg_classifier_formal.classify(input)
            tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
            terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)

            for term in terms:
                for word in term:
                    keywords.append(word)
            print '\n==================\n',keywords
        elif text_language=='es':
            sentiment = spanish_sent.spanishSentimentDetector(inputx)
        db_conn.sociabyte.fb_post_details.update({'from_id':acc_id_formal, 'post_id':post['post_id']}, {'$set':{'sentiment':sentiment, 'keywords':keywords}})
    db_conn.sociabyte.brands.update({'associated_accounts.fb_accounts.page_id':str(acc_id_formal), '_id':ObjectId(brand_id)}, {'$set':{'associated_accounts.fb_accounts.$.status':'active', 'last_updated':datetime.datetime.now()}})
    insert_json = {
        "to" : user_id,
        "text" : "The facebook page named as "+page_name+" has been successfully added",
        "action_url" : "http://sociabyte.com/brands/"+str(brand_id)+"/"+str(acc_id_formal)+"/facebook/overall",
        "status" : "unseen",
        "timestamp":datetime.datetime.now(),
        'type':'social_media',
        'details': {
           'userid':user_id,
           'brandid':brand_id,
           'pageid':acc_id_formal,
           'platform':'facebook'
        }
    }
    id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
    url_download ="http://sociabyte.com/notification/"+str(id_file_db)
    urllib.urlopen(url_download)
    print "fb completed", url_download
def twSentiment(acc_id_formal, page_name, brand_id, user_id, sent_classifier_formal, pos_neg_classifier_formal):
    import urllib
    import json
    import datetime
    import sentiment_analyser_otherLang.spanish_sentiment_detector as spanish_sent
    comment_count = db_conn.sociabyte.tw_mentions.find({'user_id':acc_id_formal, 'sentiment':''}, {'text':1, '_id':0, 'mention_id':1}).count()
    if comment_count!=0:
        comment_ids=db_conn.sociabyte.tw_mentions.find({'user_id':acc_id_formal, 'sentiment':''}, {'text':1, '_id':0, 'mention_id':1})
        for comment_id in comment_ids:
            sentiment = ''
            keywords = []
            input = comment_id['text'].lower()
            try:
                text_language_response = urllib.urlopen("http://ws.detectlanguage.com/0.2/detect?q="+input+"&key=f6493a9c926889dc6d44e68ba78e28fa")
                lang_result = json.loads(text_language_response.read())

                text_language = lang_result['data']['detections'][0]['language']
            except:
                text_language = "en"
            replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
            for char in replace_char:
                input = input.replace(char, '')
            inputx = strip_tags(input)
            if text_language=='en':
                input = inputx.split()
                input = bag_of_words(input)
                subjectivity_check = str(sent_classifier_formal.classify(input))
                if subjectivity_check=='obj':
                    sentiment = "neutral"
                else:
                    sentiment = pos_neg_classifier_formal.classify(input)
                tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
                terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)

                for term in terms:
                    for word in term:
                        keywords.append(word)
                print '\n==================\n',keywords
            elif text_language=='es':
                sentiment = spanish_sent.spanishSentimentDetector(inputx)
            db_conn.sociabyte.tw_mentions.update({'user_id':acc_id_formal, 'mention_id':comment_id['mention_id']}, {'$set':{'sentiment':sentiment, 'keywords':keywords}})
            tweets_list = db_conn.sociabyte.tw_tweets.find({'user_id':acc_id_formal, 'sentiment':''}, {'tw_id':1, '_id':0})
            for tw_id in tweets_list:
                pos_count = db_conn.sociabyte.tw_mentions.find({'in_reply_to_status_id':tw_id['tw_id'], 'sentiment':'pos'}).count()
                neg_count = db_conn.sociabyte.tw_mentions.find({'in_reply_to_status_id':tw_id['tw_id'], 'sentiment':'neg'}).count()
                nut_count = db_conn.sociabyte.tw_mentions.find({'in_reply_to_status_id':tw_id['tw_id'], 'sentiment':'neutral'}).count()
                max_val = max(pos_count, neg_count, nut_count)
                if max_val==pos_count:
                    sent = 'pos'
                if max_val == neg_count:
                    sent = 'neg'
                if max_val == nut_count:
                    sent = 'neutral'
                db_conn.sociabyte.tw_tweets.update({'user_id':acc_id_formal, 'tw_id':tw_id['tw_id']}, {'$set':{'sentiment':sent}})

    else:
        tweets_list = db_conn.sociabyte.tw_tweets.find({'user_id':acc_id_formal, 'sentiment':''}, {'tw_id':1, '_id':0})
        for tw_id in tweets_list:
            db_conn.sociabyte.tw_tweets.update({'user_id':acc_id_formal, 'tw_id':tw_id['tw_id']}, {'$set':{'sentiment':"neutral"}})
    db_conn.sociabyte.brands.update({'associated_accounts.tw_accounts.page_id':str(acc_id_formal), '_id':ObjectId(brand_id)}, {'$set':{'associated_accounts.tw_accounts.$.status':'active'}})
    insert_json = {
        "to" : user_id,
        "text" : "The twiter page named as "+page_name+" has been successfully added",
        "action_url" : "http://sociabyte.com/brands/"+str(brand_id)+"/"+str(acc_id_formal)+"/twitter/overall",
        "status" : "unseen",
        "timestamp":datetime.datetime.now(),
        'type':'social_media',
        'details': {
           'userid':user_id,
           'brandid':brand_id,
           'pageid':acc_id_formal,
           'platform':'twitter'
        }
    }
    id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
    url_download ="http://sociabyte.com/notification/"+str(id_file_db)
    urllib.urlopen(url_download)
    print "tw completed"
def twSentimentComp(acc_id_formal, sent_classifier_formal, pos_neg_classifier_formal):
    import urllib
    import json
    import sentiment_analyser_otherLang.spanish_sentiment_detector as spanish_sent
    comment_ids=db_conn.sociabyte.tw_tweets_comp.find({'user_id':acc_id_formal, 'sentiment':''}, {'text':1, '_id':0, 'tw_id':1})
    for comment_id in comment_ids:
        sentiment = ''
        keywords = []
        input = comment_id['text'].lower()
        try:
            text_language_response = urllib.urlopen("http://ws.detectlanguage.com/0.2/detect?q="+input+"&key=f6493a9c926889dc6d44e68ba78e28fa")
            lang_result = json.loads(text_language_response.read())
            text_language = lang_result['data']['detections'][0]['language']
        except:
            text_language = "en"
        replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
        for char in replace_char:
            input = input.replace(char, '')
        inputx = strip_tags(input)
        if text_language=='en':
            input = inputx.split()
            input = bag_of_words(input)
            subjectivity_check = str(sent_classifier_formal.classify(input))
            if subjectivity_check=='obj':
                sentiment = "neutral"
            else:
                sentiment = pos_neg_classifier_formal.classify(input)
            tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
            terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)

            for term in terms:
                for word in term:
                    keywords.append(word)
            print '\n==================\n',keywords
        elif text_language=='es':
            sentiment = spanish_sent.spanishSentimentDetector(inputx)
        db_conn.sociabyte.tw_tweets_comp.update({'user_id':acc_id_formal, 'tw_id':comment_id['tw_id']}, {'$set':{'sentiment':sentiment, 'keywords':keywords}})
def instSentimentComp(acc_id_formal, sent_classifier_formal, pos_neg_classifier_formal):
    import urllib
    import json
    import sentiment_analyser_otherLang.spanish_sentiment_detector as spanish_sent
    comment_ids=db_conn.sociabyte.inst_comments_comp.find({'user_id':acc_id_formal, 'sentiment':''}, {'text':1, '_id':0, 'media_id':1})
    for comment_id in comment_ids:
        sentiment = ''
        keywords = []
        input = comment_id['text'].lower()
        try:
            text_language_response = urllib.urlopen("http://ws.detectlanguage.com/0.2/detect?q="+input+"&key=f6493a9c926889dc6d44e68ba78e28fa")
            lang_result = json.loads(text_language_response.read())

            text_language = lang_result['data']['detections'][0]['language']
        except:
            text_language = "en"
        replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
        for char in replace_char:
            input = input.replace(char, '')
        inputx = strip_tags(input)
        if text_language=='en':                                               
            input = inputx.split()
            input = bag_of_words(input)
            subjectivity_check = str(sent_classifier_formal.classify(input))
            if subjectivity_check=='obj':
                sentiment = "neutral"
            else:
                sentiment = pos_neg_classifier_formal.classify(input)
            tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
            terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)

            for term in terms:
                for word in term:
                    keywords.append(word)
            print '\n==================\n',keywords
        elif text_language=='es':
            sentiment = spanish_sent.spanishSentimentDetector(inputx)                                                          
        db_conn.sociabyte.inst_comments_comp.update({'user_id':acc_id_formal, 'media_id':comment_id['media_id']}, {'$set':{'sentiment':sentiment, 'keywords':keywords}})
    media_list = db_conn.sociabyte.inst_media_comp.find({'user_id':acc_id_formal, 'sentiment':''}, {'_id':0, 'media_id':1})
    for media in media_list:
        pos_count = db_conn.sociabyte.inst_comments_comp.find({'media_id':media['media_id'], 'sentiment':'pos'}).count()
        neg_count = db_conn.sociabyte.inst_comments_comp.find({'media_id':media['media_id'], 'sentiment':'neg'}).count()
        nut_count = db_conn.sociabyte.inst_comments_comp.find({'media_id':media['media_id'], 'sentiment':'neutral'}).count()
        max_val = max(pos_count, neg_count, nut_count)
        if max_val==pos_count:
            sent = 'pos'
        if max_val == neg_count:
            sent = 'neg'
        if max_val == nut_count:
            sent = 'neutral'
        db_conn.sociabyte.inst_media_comp.update({'user_id':acc_id_formal, 'media_id':media['media_id']}, {'$set':{'sentiment':sent}})

def instSentiment(acc_id_formal, page_name, brand_id, user_id, sent_classifier_formal, pos_neg_classifier_formal):
    import urllib
    import json
    import sentiment_analyser_otherLang.spanish_sentiment_detector as spanish_sent
    import datetime
    comment_ids=db_conn.sociabyte.inst_comments.find({'user_id':acc_id_formal, 'sentiment':''}, {'text':1, 'comment_id':1, '_id':0})
    for comment_id in comment_ids:
        sentiment = ''
        keywords = []
        input = comment_id['text'].lower()
        try:
            text_language_response = urllib.urlopen("http://ws.detectlanguage.com/0.2/detect?q="+input+"&key=f6493a9c926889dc6d44e68ba78e28fa")
            lang_result = json.loads(text_language_response.read())
            text_language = lang_result['data']['detections'][0]['language']
        except:
            text_language = "en"
        replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
        for char in replace_char:
            input = input.replace(char, '')
        inputx = strip_tags(input)
        if text_language=='en':
            input = inputx.split()
            input = bag_of_words(input)
            subjectivity_check = str(sent_classifier_formal.classify(input))
            if subjectivity_check=='obj':
                sentiment = "neutral"
            else:
                sentiment = pos_neg_classifier_formal.classify(input)
            tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
            terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)

            for term in terms:
                for word in term:
                    keywords.append(word)
            print '\n==================\n',keywords
        elif text_language=='es':
            sentiment = spanish_sent.spanishSentimentDetector(inputx)
        db_conn.sociabyte.inst_comments.update({'comment_id':comment_id['comment_id']}, {'$set':{'sentiment':sentiment, 'keywords':keywords}})
    media_list = db_conn.sociabyte.inst_media.find({'user_id':acc_id_formal, 'sentiment':''}, {'_id':0, 'media_id':1})
    for media in media_list:
        pos_count = db_conn.sociabyte.inst_comments.find({'media_id':media['media_id'], 'sentiment':'pos'}).count()
        neg_count = db_conn.sociabyte.inst_comments.find({'media_id':media['media_id'], 'sentiment':'neg'}).count()
        nut_count = db_conn.sociabyte.inst_comments.find({'media_id':media['media_id'], 'sentiment':'neutral'}).count()
        max_val = max(pos_count, neg_count, nut_count)
        if max_val==pos_count:
            sent = 'pos'
        if max_val == neg_count:
            sent = 'neg'
        if max_val == nut_count:
            sent = 'neutral'
        db_conn.sociabyte.inst_media.update({'user_id':acc_id_formal, 'media_id':media['media_id']}, {'$set':{'sentiment':sent}})
    db_conn.sociabyte.brands.update({'associated_accounts.ins_accounts.page_id':str(acc_id_formal), '_id':ObjectId(brand_id)}, {'$set':{'associated_accounts.ins_accounts.$.status':'active'}})
    insert_json = {
        "to" : user_id,
        "text" : "The instagram page named as "+page_name+" has been successfully added",
        "action_url" : "http://sociabyte.com/brands/"+str(brand_id)+"/"+str(acc_id_formal)+"/instagram/overall",
        "status" : "unseen",
        "timestamp":datetime.datetime.now(),
        'type':'social_media',
        'details': {
           'userid':user_id,
           'brandid':brand_id,
           'pageid':acc_id_formal,
           'platform':'instagram'
        }
    }
    id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
    url_download ="http://sociabyte.com/notification/"+str(id_file_db)
    urllib.urlopen(url_download)
    print "inst completed", url_download
def liSentiment(acc_id_formal, page_name, brand_id, user_id, sent_classifier_formal, pos_neg_classifier_formal):
    import urllib
    import json
    import sentiment_analyser_otherLang.spanish_sentiment_detector as spanish_sent
    import datetime
    comment_ids=db_conn.sociabyte.li_comments.find({'id':acc_id_formal, 'sentiment':''}, {'comment':1, 'update_key':1, '_id':0})
    for comment_id in comment_ids:
        sentiment = ''
        keywords = []
        input = comment_id['comment'].lower()
        try:
            text_language_response = urllib.urlopen("http://ws.detectlanguage.com/0.2/detect?q="+input+"&key=f6493a9c926889dc6d44e68ba78e28fa")
            lang_result = json.loads(text_language_response.read())

            text_language = lang_result['data']['detections'][0]['language']
        except:
            text_language = "en"
        replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
        for char in replace_char:
            input = input.replace(char, '')
        inputx = strip_tags(input)
        if text_language=='en':
            input = inputx.split()
            input = bag_of_words(input)
            subjectivity_check = str(sent_classifier_formal.classify(input))
            if subjectivity_check=='obj':
                sentiment = "neutral"
            else:
                sentiment = pos_neg_classifier_formal.classify(input)
            tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
            terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)

            for term in terms:
                for word in term:
                    keywords.append(word)
            print '\n==================\n',keywords
        elif text_language=='es':
            sentiment = spanish_sent.spanishSentimentDetector(inputx)
        db_conn.sociabyte.li_comments.update({'update_key':comment_id['update_key']}, {'$set':{'sentiment':sentiment, 'keywords':keywords}})
    media_list = db_conn.sociabyte.li_shares.find({'updateContent.company.id':int(acc_id_formal), 'sentiment':''}, {'_id':0, 'updateKey':1})
    for media in media_list:
        pos_count = db_conn.sociabyte.li_comments.find({'update_key':media['updateKey'], 'sentiment':'pos'}).count()
        neg_count = db_conn.sociabyte.li_comments.find({'update_key':media['updateKey'], 'sentiment':'neg'}).count()
        nut_count = db_conn.sociabyte.li_comments.find({'update_key':media['updateKey'], 'sentiment':'neutral'}).count()
        max_val = max(pos_count, neg_count, nut_count)
        if max_val==pos_count:
            sent = 'pos'
        if max_val == neg_count:
            sent = 'neg'
        if max_val == nut_count:
            sent = 'neutral'
        db_conn.sociabyte.li_shares.update({'updateContent.company.id':int(acc_id_formal), 'updateKey':media['updateKey']}, {'$set':{'sentiment':sent}, 'keywords':keywords})
    db_conn.sociabyte.brands.update({'associated_accounts.in_accounts.page_id':str(acc_id_formal), '_id':ObjectId(brand_id)}, {'$set':{'associated_accounts.in_accounts.$.status':'active'}})
    insert_json = {
        "to" : user_id,
        "text" : "The linkedIN page named as "+page_name+" has been successfully added",
        "action_url" : "http://sociabyte.com/brands/"+str(brand_id)+"/"+str(acc_id_formal)+"/linkedin/overall",
        "status" : "unseen",
        "timestamp":datetime.datetime.now(),
        'type':'social_media',
        'details': {
           'userid':user_id,
           'brandid':brand_id,
           'pageid':acc_id_formal,
           'platform':'linkedin'
        }
    }
    id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
    url_download ="http://sociabyte.com/notification/"+str(id_file_db)
    urllib.urlopen(url_download)
    print "LInked In completed"
def utubeSentiment(acc_id_formal, page_name, brand_id, user_id, sent_classifier_formal, pos_neg_classifier_formal):
    import urllib
    import json
    import sentiment_analyser_otherLang.spanish_sentiment_detector as spanish_sent
    import datetime
    comment_ids=db_conn.sociabyte.utube_video_comments.find({'yt_channelId':acc_id_formal, 'sentiment':''}, {'content':1, '_id':1})
    for comment_id in comment_ids:
        sentiment = ''
        keywords = []
        input = comment_id['content'].lower()
        try:
            text_language_response = urllib.urlopen("http://ws.detectlanguage.com/0.2/detect?q="+input+"&key=f6493a9c926889dc6d44e68ba78e28fa")
            lang_result = json.loads(text_language_response.read())

            text_language = lang_result['data']['detections'][0]['language']
        except:
            text_language = "en"
        print "input===>>>>>", input, '==', acc_id_formal
        if input!='':
            replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
            for char in replace_char:
                input = input.replace(char, '')
            inputx = strip_tags(input)
            if text_language=='en':
                input = inputx.split()
                input = bag_of_words(input)
                subjectivity_check = str(sent_classifier_formal.classify(input))
                if subjectivity_check=='obj':
                    sentiment = "neutral"
                else:
                    sentiment = pos_neg_classifier_formal.classify(input)
                tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
                terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)

                for term in terms:
                    for word in term:
                        keywords.append(word)
                print '\n==================\n',keywords
            elif text_language=='es':
                sentiment = spanish_sent.spanishSentimentDetector(inputx)
        else:
            sentiment = 'neutral'
            keywords = []
        db_conn.sociabyte.utube_video_comments.update({'_id':ObjectId(comment_id['_id'])}, {'$set':{'sentiment':sentiment, 'keywords':keywords}})
    media_list = db_conn.sociabyte.utube_channel_videos.find({'channelId':(acc_id_formal), 'sentiment':''}, {'_id':0, 'resourceId.videoId':1})
    for media in media_list:
        pos_count = db_conn.sociabyte.li_comments.find({'yt_videoid':media['resourceId']['videoId'], 'sentiment':'pos'}).count()
        neg_count = db_conn.sociabyte.li_comments.find({'yt_videoid':media['resourceId']['videoId'], 'sentiment':'neg'}).count()
        nut_count = db_conn.sociabyte.li_comments.find({'yt_videoid':media['resourceId']['videoId'], 'sentiment':'neutral'}).count()
        max_val = max(pos_count, neg_count, nut_count)
        if max_val==pos_count:
            sent = 'pos'
        if max_val == neg_count:
            sent = 'neg'
        if max_val == nut_count:
            sent = 'neutral'
        db_conn.sociabyte.utube_channel_videos.update({'resourceId.videoId':media['resourceId']['videoId']}, {'$set':{'sentiment':sent}})
    db_conn.sociabyte.brands.update({'associated_accounts.utube_accounts.page_id':str(acc_id_formal), '_id':ObjectId(brand_id)}, {'$set':{'associated_accounts.utube_accounts.$.status':'active'}})
    insert_json = {
        "to" : user_id,
        "text" : "The youtuve page named as "+page_name+" has been successfully added",
        "action_url" : "http://sociabyte.com/brands/"+str(brand_id)+"/"+str(acc_id_formal)+"/youtube/overall",
        "status" : "unseen",
        "timestamp":datetime.datetime.now(),
        'type':'social_media',
        'details': {
           'userid':user_id,
           'brandid':brand_id,
           'pageid':acc_id_formal,
           'platform':'youtube'
        }
    }
    id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
    url_download ="http://sociabyte.com/notification/"+str(id_file_db)
    urllib.urlopen(url_download)
    print "utube completed"
def projectSentimentTemp(acc_id_formal, sent_classifier_formal, pos_neg_classifier_formal):
    import urllib
    import json
    import sentiment_analyser_otherLang.spanish_sentiment_detector as spanish_sent
    comment_ids=db_conn.sociabyte.rss_response_temp.find({'project_id':acc_id_formal, 'sentiment':''}, {'content':1, '_id':1})
    for comment_id in comment_ids:
        sentiment = ''
        keywords = []
        input = comment_id['content'].lower()
        try:
            text_language_response = urllib.urlopen("http://ws.detectlanguage.com/0.2/detect?q="+input+"&key=f6493a9c926889dc6d44e68ba78e28fa")
            lang_result = json.loads(text_language_response.read())

            text_language = lang_result['data']['detections'][0]['language']
        except:
            text_language = "en"
        if input!='':
            replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
            for char in replace_char:
                input = input.replace(char, '')
            inputx = strip_tags(input)
            if text_language=='en':
                input = inputx.split()
                input = bag_of_words(input)
                subjectivity_check = str(sent_classifier_formal.classify(input))
                if subjectivity_check=='obj':
                    sentiment = "neutral"
                else:
                    sentiment = pos_neg_classifier_formal.classify(input)
                tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
                terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)

                for term in terms:
                    for word in term:
                        keywords.append(word)
                print '\n==================\n',keywords
            elif text_language=='es':
                sentiment = spanish_sent.spanishSentimentDetector(inputx)
        else:
            sentiment = 'neutral'
            keywords = []
        db_conn.sociabyte.rss_response_temp.update({'_id':ObjectId(comment_id['_id'])}, {'$set':{'sentiment':sentiment, 'keywords':keywords}})
    db_conn.sociabyte.project_temp.update({'project_id':acc_id_formal}, {'$set':{'status':'active'}})
def projectSentimentParm(acc_id_formal, sent_classifier_formal, pos_neg_classifier_formal):
    import urllib
    import json
    import sentiment_analyser_otherLang.spanish_sentiment_detector as spanish_sent
    comment_ids=db_conn.sociabyte.rss_response.find({'project_id':(acc_id_formal), 'sentiment':''}, {'content':1, '_id':1})
    for comment_id in comment_ids:
        sentiment = ''
        keywords = []
        input = comment_id['content'].lower()
        try:
            text_language_response = urllib.urlopen("http://ws.detectlanguage.com/0.2/detect?q="+input+"&key=f6493a9c926889dc6d44e68ba78e28fa")
            lang_result = json.loads(text_language_response.read())
            text_language = lang_result['data']['detections'][0]['language']
        except:
            text_language = "en"
        if input!='':
            replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
            for char in replace_char:
                input = input.replace(char, '')
            inputx = strip_tags(input)
            if text_language=='en':
                input = inputx.split()
                input = bag_of_words(input)
                subjectivity_check = str(sent_classifier_formal.classify(input))
                if subjectivity_check=='obj':
                    sentiment = "neutral"
                else:
                    sentiment = pos_neg_classifier_formal.classify(input)
                tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
                terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)

                for term in terms:
                    for word in term:
                        keywords.append(word)
                print '\n==================\n',keywords
            elif text_language=='es':
                sentiment = spanish_sent.spanishSentimentDetector(inputx)
            else:
                sentiment = 'neutral'
        else:
            sentiment = 'neutral'
            keywords = []
        db_conn.sociabyte.rss_response.update({'_id':ObjectId(comment_id['_id'])}, {'$set':{'sentiment':sentiment, 'keywords':keywords}})
    db_conn.sociabyte.projects.update({'_id':ObjectId(acc_id_formal)}, {'$set':{'final_fetch':'complete'}})

def fbSentimentComp(acc_id_formal, sent_classifier_formal, pos_neg_classifier_formal):
    import urllib
    import json
    import sentiment_analyser_otherLang.spanish_sentiment_detector as spanish_sent
    print "in fb competitor"
    comment_ids=db_conn.sociabyte.fb_comments_comp.find({'page_id':acc_id_formal, 'sentiment':''}, {'post_id':1, 'message':1, '_id':0})
    for comment_id in comment_ids:
        sentiment = ''
        keywords = []
        input = comment_id['message'].lower()
        try:
            text_language_response = urllib.urlopen("http://ws.detectlanguage.com/0.2/detect?q="+input+"&key=f6493a9c926889dc6d44e68ba78e28fa")
            lang_result = json.loads(text_language_response.read())
            text_language = lang_result['data']['detections'][0]['language']
        except:
            text_language = "en"
        replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
        for char in replace_char:
            input = input.replace(char, '')
        inputx = strip_tags(input)
        if text_language=='en':
            input = inputx.split()
            input = bag_of_words(input)
            subjectivity_check = str(sent_classifier_formal.classify(input))
            if subjectivity_check=='obj':
                sentiment = "neutral"
            else:
                sentiment = pos_neg_classifier_formal.classify(input)
            tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
            terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)

            for term in terms:
                for word in term:
                    keywords.append(word)
            print '\n==================\n',keywords
        elif text_language=='es':
            sentiment = spanish_sent.spanishSentimentDetector(inputx)
        db_conn.sociabyte.fb_comments_comp.update({'page_id':acc_id_formal, 'post_id':comment_id['post_id']}, {'$set':{'sentiment':sentiment, 'keywords':keywords}})
    posts=db_conn.sociabyte.fb_post_details_comp.find({'page_id':acc_id_formal, 'sentiment':''}, {'post_id':1, 'message':1, '_id':0})
    for post in posts:
        sentiment = ''
        keywords = []
        input = post['message'].lower()
        try:
            text_language_response = urllib.urlopen("http://ws.detectlanguage.com/0.2/detect?q="+input+"&key=f6493a9c926889dc6d44e68ba78e28fa")
            lang_result = json.loads(text_language_response.read())
            text_language = lang_result['data']['detections'][0]['language']
        except:
            text_language = ""
        replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
        for char in replace_char:
            input = input.replace(char, '')
        inputx = strip_tags(input)
        if text_language=='en':
            input = inputx.split()
            input = bag_of_words(input)
            subjectivity_check = str(sent_classifier_formal.classify(input))
            if subjectivity_check=='obj':
                sentiment = "neutral"
            else:
                sentiment = pos_neg_classifier_formal.classify(input)
            tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
            terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)

            for term in terms:
                for word in term:
                    keywords.append(word)
            print '\n==================\n',keywords
        elif text_language=='es':
            sentiment = spanish_sent.spanishSentimentDetector(inputx)
        db_conn.sociabyte.fb_post_details_comp.update({'page_id':acc_id_formal, 'post_id':post['post_id']}, {'$set':{'sentiment':sentiment, 'keywords':keywords}})
def uploadFileAnalysis(acc_id_formal, sent_classifier_formal, pos_neg_classifier_formal):
    import urllib
    import json
    import sentiment_analyser_otherLang.spanish_sentiment_detector as spanish_sent
    import os
    import datetime
    import xlsxwriter
    import xlrd
    os.chdir("/home/ubuntu/sociabyte/sociabyte_app/public/upload/csv_files")
    dnld_dir = "/home/ubuntu/sociabyte/sociabyte_app/public/download/csv_files"
    file_name_res = db_conn.sociabyte.upload_csvs.find({'_id':ObjectId(acc_id_formal)}, {'_id':0})
    for file in file_name_res:
        file_to_analise = file['converted_file_name']
        user_id_main = file['user_id']
        actual_name = file['actual_file_name']
        print "file to analyse=====>>>>", file_to_analise
    name_convention = file_to_analise.split(".")
    file_to_write =xlsxwriter.Workbook(os.path.join(dnld_dir, name_convention[0]+'.xlsx'))
    worksheet_for_file_to_writer = file_to_write.add_worksheet('sentiment_data')
    header = ('Text', 'Sentiment', 'Keywords')
    worksheet_for_file_to_writer.write_row('A1',header)
    opened_book = xlrd.open_workbook(file_to_analise)
    working_sheet = opened_book.sheet_by_index(0)
    row_counter = 1
    input_cell = working_sheet.cell(row_counter,0)
    while input_cell.value!='':
        print "input cell values =====>>>>",input_cell.value
        input = input_cell.value
        try:
            text_language_response = urllib.urlopen("http://ws.detectlanguage.com/0.2/detect?q="+input+"&key=f6493a9c926889dc6d44e68ba78e28fa")
            lang_result = json.loads(text_language_response.read())

            text_language = lang_result['data']['detections'][0]['language']
        except:
            text_language = "en"
        raw_input = input
        sentiment = ''
        replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
        for char in replace_char:
            input = input.replace(char, '')
        inputx = strip_tags(input)
        if text_language=='en' or text_language=='':
            input = inputx.split()
            input = bag_of_words(input)
            subjectivity_check = str(sent_classifier_formal.classify(input))
            if subjectivity_check=='obj':
                sentiment = "neutral"
            else:
                sentiment = pos_neg_classifier_formal.classify(input)
            tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
            terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)
            keywords = ''
            for term in terms:
                for word in term:
                    keywords=(keywords+', '+word) if keywords is not '' else (word)
            print type(input), type(sentiment), type(keywords)
        elif text_language=='es':
            sentiment = spanish_sent.spanishSentimentDetector(inputx)
        write_list = (raw_input, sentiment, keywords)
        worksheet_for_file_to_writer.write_row('A'+str(row_counter+1), write_list)
        row_counter+=1
        try:
            input_cell = working_sheet.cell(row_counter,0)
        except:
            break
    file_to_write.close()
    db_conn.sociabyte.upload_csvs.update({'_id':ObjectId(acc_id_formal)},{'$set':{'status':'complete', 'download_location':'http://sociabyte.com/download/csv_files/'+name_convention[0]+'.xlsx'}})
    action_url ="http://sociabyte.com/download/csv_files/"+name_convention[0]+".xlsx"
    insert_json = {
        "to" : user_id_main,
        "text" : "Sentiment analysis completed for file "+actual_name,
        "action_url" : action_url,
        "status" : "unseen",
        "timestamp":datetime.datetime.now(),
        'type':'sentiment_analyser',
        'file':{
            'file_name':name_convention[0]+".xlsx",
            'id':acc_id_formal
        }
    }
    id_file_db = db_conn.sociabyte.notifications.insert(insert_json)
    url_download ="http://sociabyte.com/notification/"+str(id_file_db)
    urllib.urlopen(url_download)
def newsAnalysis(sent_classifier_formal, pos_neg_classifier_formal):
    import os
    dnld_dir = "/home/ubuntu/sociabyte/sociabyte_app/public/download/csv_files"
    file_to_write =open(os.path.join(dnld_dir, 'news.tsv'), 'w')
    header = "title \t text \t link \t sentiment \t keywords \n"
    file_to_write.write(header)
    news = db_conn.sociabyte.news.find()
    allowed_words1 = ["bjp", "naredra modi", "modi", "rajnath singh", "arun jaitely"]
    allowed_words2 = ["congress", "rahul gandhi", "sonia gandhi", "manmohan singh"]
    for new in news:
        input = new['content'].lower()
        print "input==>>", input
        if any(word in input for word in allowed_words1):
            raw_input = input
            sentiment = ''
            keywords = ''
            replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
            for char in replace_char:
                input = input.replace(char, '')
            inputx = strip_tags(input)
            input = inputx.split()
            input = bag_of_words(input)
            subjectivity_check = str(sent_classifier_formal.classify(input))
            if subjectivity_check=='obj':
                sentiment = "neutral"
            else:
                sentiment = pos_neg_classifier_formal.classify(input)
            tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
            terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)
            for term in terms:
                for word in term:
                    keywords=keywords+','+word
            print type(input), type(sentiment), type(keywords)
            write_str = "BJP"+'\t'+new['title']+ "\t" +inputx +"\t"+ new['link']+'\t'+sentiment+'\t'+keywords+'\n'
            file_to_write.write(write_str.encode('utf-8'))
        elif any(word in input for word in allowed_words2):
            raw_input = input
            sentiment = ''
            keywords = ''
            replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
            for char in replace_char:
                input = input.replace(char, '')
            inputx = strip_tags(input)
            input = inputx.split()
            input = bag_of_words(input)
            subjectivity_check = str(sent_classifier_formal.classify(input))
            if subjectivity_check=='obj':
                sentiment = "neutral"
            else:
                sentiment = pos_neg_classifier_formal.classify(input)
            tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
            terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)
            for term in terms:
                for word in term:
                    keywords=keywords+','+word
            print type(input), type(sentiment), type(keywords)
            write_str = "Congress"+'\t'+new['title']+ "\t" +inputx +"\t"+ new['link']+'\t'+sentiment+'\t'+keywords+'\n'
            file_to_write.write(write_str)
        else:
            raw_input = input
            sentiment = ''
            keywords = ''
            replace_char = ["..", "!", 'www.', 'http://', 'https://', '.com']
            for char in replace_char:
                input = input.replace(char, '')
            inputx = strip_tags(input)
            input = inputx.split()
            input = bag_of_words(input)
            subjectivity_check = str(sent_classifier_formal.classify(input))
            if subjectivity_check=='obj':
                sentiment = "neutral"
            else:
                sentiment = pos_neg_classifier_formal.classify(input)
            tree, stopwordss, stemmerr, lemmatizerr =  kewwordExtractors.keywordextractorVars(inputx)
            terms = kewwordExtractors.get_terms(tree, stopwordss, stemmerr, lemmatizerr)
            for term in terms:
                for word in term:
                    keywords=keywords+','+word
            print type(input), type(sentiment), type(keywords)
            write_str = "unknown"+'\t'+new['title']+ "\t" +inputx +"\t"+ new['link']+'\t'+sentiment+'\t'+keywords+'\n'
            file_to_write.write(write_str)
    file_to_write.close()
def main():
    word_fd = FreqDist()
    label_word_fd = ConditionalFreqDist()

    for word in subjectivity.words('sub.txt'):
        word_fd.inc(word.lower())
        label_word_fd['sub'].inc(word.lower())

    for word in subjectivity.words('obj.txt'):
        word_fd.inc(word.lower())
        label_word_fd['obj'].inc(word.lower())

    pos_word_count = label_word_fd['sub'].N()
    neg_word_count = label_word_fd['obj'].N()
    total_word_count = pos_word_count + neg_word_count

    word_scores = {}

    for word, freq in word_fd.iteritems():
        pos_score = BigramAssocMeasures.chi_sq(label_word_fd['sub'][word],
            (freq, pos_word_count), total_word_count)
        neg_score = BigramAssocMeasures.chi_sq(label_word_fd['obj'][word],
            (freq, neg_word_count), total_word_count)
        word_scores[word] = pos_score + neg_score

    best = sorted(word_scores.iteritems(), key=lambda (w,s): s, reverse=True)[:10000]
    global bestwords
    bestwords = set([w for w, s in best])
    subjectivity_check=''
    sent_classifier_bi=evaluate_classifier(best_bigram_word_feats, subjective_fileLoc, objective_fileLoc)
    pos_neg_classifier = sentimentDetection.findSentiment()
    portArray = ['5575', '5558', '5559', '5560', '5561', '5562', '5564', '5565', '5563', '5571','5572','5573', '5574']
    if len(sys.argv) > 1:
        port =  sys.argv[1]
        int(port)

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    topicfilter = ""
    socket.setsockopt(zmq.SUBSCRIBE, topicfilter)
    for port in portArray:
        socket.connect("tcp://localhost:%s" % port)
    print 'Listening on 5559....'
    while True:
        topic,messages = socket.recv_multipart()
        messages_formated = json.loads(messages)
        print "Received request: ", messages_formated
        if messages_formated['code']=='fb':
            fbSentiment(messages_formated['acc_id'], messages_formated['page_name'], messages_formated['brand_id'], messages_formated['user_id'], sent_classifier_bi, pos_neg_classifier)
        elif messages_formated['code']=='tw':
            twSentiment(messages_formated['acc_id'], messages_formated['page_name'], messages_formated['brand_id'], messages_formated['user_id'], sent_classifier_bi, pos_neg_classifier)
        elif messages_formated['code']=='inst':
            instSentiment(messages_formated['acc_id'], messages_formated['page_name'], messages_formated['brand_id'], messages_formated['user_id'], sent_classifier_bi, pos_neg_classifier)
        elif messages_formated['code']=='li':
            liSentiment(messages_formated['acc_id'], messages_formated['page_name'], messages_formated['brand_id'], messages_formated['user_id'], sent_classifier_bi, pos_neg_classifier)
        elif messages_formated['code'] == 'utube':
            utubeSentiment(messages_formated['acc_id'], messages_formated['page_name'], messages_formated['brand_id'], messages_formated['user_id'], sent_classifier_bi, pos_neg_classifier)
        elif messages_formated['code'] == 'project':
            projectSentimentTemp(messages_formated['acc_id'], sent_classifier_bi, pos_neg_classifier)
        elif messages_formated['code'] == 'projectParm':
            projectSentimentParm(messages_formated['acc_id'], sent_classifier_bi, pos_neg_classifier)
        elif messages_formated['code'] == 'fb_comp':
            fbSentimentComp(messages_formated['acc_id'], sent_classifier_bi, pos_neg_classifier)
        elif messages_formated['code'] == 'tw_comp':
            twSentimentComp(messages_formated['acc_id'], sent_classifier_bi, pos_neg_classifier)
        elif messages_formated['code'] == 'inst_comp':
            instSentimentComp(messages_formated['acc_id'], sent_classifier_bi, pos_neg_classifier)
        elif messages_formated['code'] == 'uploadfile':
            uploadFileAnalysis(messages_formated['acc_id'], sent_classifier_bi, pos_neg_classifier)
        elif messages_formated['code'] == 'news':
             newsAnalysis(sent_classifier_bi, pos_neg_classifier)
main()



