import time
from datetime import datetime
import re
def basicInfoFetch(li_comp_id_formal, db_connection_formal, access_token_formal, urllib_formal, json_formal, brand_id):
    url = 'https://api.linkedin.com/v1/companies/'+str(li_comp_id_formal)+':(id,name,ticker,description,company-type,website-url,industries,status,logo-url,twitter-id,employee-count-range,specialties,stock-exchange,num-followers)?oauth2_access_token='+str(access_token_formal)+'&format=json'
    print url, '========', access_token_formal
    basic_info = urllib_formal.urlopen(url)
    basic_info_formated = json_formal.loads(basic_info.read())
    try:
        basic_info_formated['brand_id'] = [brand_id]
        db_connection_formal.li_basic_info.insert(basic_info_formated)
        basic_info_formated['date']=datetime.now()
        db_connection_formal.li_basic_info_daily.insert(basic_info_formated)
    except:
        pass
    return 1
def compShareInfo(li_comp_id_formal, db_connection_formal, access_token_formal, urllib_formal, json_formal):
    comp_share_info = urllib_formal.urlopen('https://api.linkedin.com/v1/companies/{id}/updates?format=json&start=0&count=250&oauth2_access_token={token}'.format(id=li_comp_id_formal, token=access_token_formal))
    comp_share_info_formated = json_formal.loads(comp_share_info.read())
    update_list = []
    if comp_share_info_formated['_total']!=0:
        for updates in comp_share_info_formated['values']:
            time_stamp = updates['timestamp']/1000
            timestamp_to_date = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(time_stamp)))
            updates['timestamp'] = datetime.strptime(timestamp_to_date ,"%Y-%m-%dT%H:%M:%SZ")
            if updates['updateContent'].has_key('companyStatusUpdate'):
                text = updates['updateContent']['companyStatusUpdate']['share']['comment']
                formated_text1 = text.replace('\n', '')
                r = re.compile(r'(http://[^ ]+)')
                updates['updateContent']['companyStatusUpdate']['share']['comment'] = (r.sub(r"<a href='\1'>\1</a>", formated_text1))
                updates['updateContent']['companyStatusUpdate']['share']['comment'].encode('UTF-8')
                updates['reach']=updates['numLikes']+updates['updateComments']['_total']
                updates['sentiment'] = ''
                updates['keywords']=[]
            update_list.append(updates.copy())
        if comp_share_info_formated['_total']>0:
            db_connection_formal.li_shares.insert(update_list)
        else:
            pass
    return 1

def historicalFollowerStatistics(li_comp_id_formal, db_connection_formal, access_token_formal, urllib_formal, json_formal):
    one_month = (int(time.time()*1000)-(30*24*60*60*1000))
    url = 'https://api.linkedin.com/v1/companies/{id}/historical-follow-statistics?format=json&start-timestamp={timeStamp}&time-granularity={granule}&oauth2_access_token={token}'.format(id=li_comp_id_formal, timeStamp=one_month, granule='day', token=access_token_formal)
    follower_stat = urllib_formal.urlopen(url)
    follower_stat_formated = json_formal.loads(follower_stat.read())
    stats_list = []
    for stats in follower_stat_formated['values']:
        time_stamp= stats['time']/1000
        timestamp_to_date = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(time_stamp)))
        stats['time'] = datetime.strptime(timestamp_to_date ,"%Y-%m-%dT%H:%M:%SZ")
        stats['id'] = li_comp_id_formal
        stats_list.append(stats.copy())
    if follower_stat_formated['_total']>0:
        db_connection_formal.li_follower_stat.insert(stats_list)
    else:
        pass
    return 1

def historicalStatusUpdateStat(li_comp_id_formal, db_connection_formal, access_token_formal, urllib_formal, json_formal):
    one_month = (int(time.time()*1000)-(30*24*60*60*1000))
    url = 'https://api.linkedin.com/v1/companies/{id}/historical-status-update-statistics?format=json&start-timestamp={time_stamp}&time-granularity=day&oauth2_access_token={token}'.format(id=li_comp_id_formal, time_stamp=one_month, token=access_token_formal)
    status_stat = urllib_formal.urlopen(url)
    status_stat_formated= json_formal.loads(status_stat.read())
    stat_list = []
    for stats in status_stat_formated['values']:
        time_stamp= stats['time']/1000
        timestamp_to_date = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(time_stamp)))
        stats['time'] = datetime.strptime(timestamp_to_date ,"%Y-%m-%dT%H:%M:%SZ")
        stats['id'] = li_comp_id_formal
        stat_list.append(stats.copy())
    if status_stat_formated['_total']>0:
        db_connection_formal.li_status_stat.insert(stat_list)
    else:
        pass
    return 1
def specificStatusStat(li_comp_id_formal, li_stat_id_list_formal, db_connection_formal, access_token_formal, urllib_formal, json_formal):
    one_month = (int(time.time()*1000)-(30*24*60*60*1000))
    for update_key in li_stat_id_list_formal:
        specific_status_stat = urllib_formal.urlopen('https://api.linkedin.com/v1/companies/{id}/historical-status-update-statistics:(time,like-count,comment-count,share-count)?format=json&start-timestamp={time_stamp}&time-granularity=day&update-key={key}&oauth2_access_token={token}'.format(id=li_comp_id_formal, time_stamp=one_month, key=update_key, token=access_token_formal))
        specific_status_stat_formated = json_formal.loads(specific_status_stat.read())
        if specific_status_stat_formated['_total']>0:
            for daily_stat in specific_status_stat_formated['values']:
                time_stamp= daily_stat['time']/1000
                timestamp_to_date = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(time_stamp)))
                daily_stat['time'] = datetime.strptime(timestamp_to_date ,"%Y-%m-%dT%H:%M:%SZ")
        specific_status_stat_formated['id']=li_comp_id_formal
        specific_status_stat_formated['post_id'] = update_key
        db_connection_formal.li_share_specific_stat.insert(specific_status_stat_formated)
    return 1

def commentFetch(li_comp_id_formal, li_stat_id_list_formal, db_connection_formal, access_token_formal, urllib_formal, json_formal):
    for update_key in li_stat_id_list_formal:
        comment_list = []
        url = 'https://api.linkedin.com/v1/companies/{id}/updates/key={updatekey}/update-comments?format=json&oauth2_access_token={token}'.format(id=li_comp_id_formal, updatekey=update_key, token=access_token_formal)
        comments = urllib_formal.urlopen(url)
        comments_formated = json_formal.loads(comments.read())
        try:
            comments_count = comments_formated['_total']
        except:
            comments_count = 0
        db_connection_formal.li_shares.update({"updateKey":update_key}, {"$set":{'numComments':comments_count}})
        if comments_formated['_total']>0:
            for comment in comments_formated['values']:
                time_stamp= comment['timestamp']/1000
                timestamp_to_date = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(time_stamp)))
                comment['timestamp'] = datetime.strptime(timestamp_to_date ,"%Y-%m-%dT%H:%M:%SZ")
                comment['comment_id'] = comment['id']
                comment['id'] = li_comp_id_formal
                comment['update_key'] = update_key
                comment['sentiment'] = ''
                comment_list.append(comment.copy())
            db_connection_formal.li_comments.insert(comment_list)
        else:
            pass
    return 1
def companyStatistics(li_comp_id_formal, db_connection_formal, access_token_formal, urllib_formal, json_formal):
    comp_statistics = urllib_formal.urlopen("https://api.linkedin.com/v1/companies/{id}/company-statistics?format=json&oauth2_access_token={token}".format(id=li_comp_id_formal, token=access_token_formal))
    comp_statistics_formated= json_formal.loads(comp_statistics.read())
    if comp_statistics_formated['followStatistics'].has_key('companySizes')==1 and comp_statistics_formated['followStatistics']['companySizes']['_total']>0:
        size_dict = {
            'A':'1',
            'B':'2-10',
            'C':'11-50',
            'D':'51-200',
            'E':'201-500',
            'F':'501-1000',
            'G':'1001-5000',
            'H':'5001-10,000',
            'I':'10,000+'
        }
        mapFunction(size_dict, comp_statistics_formated['followStatistics']['companySizes']['values'])
        comp_statistics_formated['followStatistics']['companySizes']['id']=li_comp_id_formal
        db_connection_formal.li_followers_comp_size.insert(comp_statistics_formated['followStatistics']['companySizes'])
    if comp_statistics_formated['followStatistics'].has_key('functions')==1 and comp_statistics_formated['followStatistics']['functions']['_total']>0:
        code_dict = {
            "-1":"None",
            "1":"Accounting",
            "2":"Administrative",
            "3":"Arts and Design",
            "4":"Business Development",
            "5":"Community and Social Services",
            "6":"Consulting",
            "7":"Education",
            "8":"Engineering",
            "9":"Entrepreneurship",
            "10":"Finance",
            "11":"Healthcare Services",
            "12":"Human Resources",
            "13":"Information Technology",
            "14": "Legal",
            "15": "Marketing",
            "16": "Media and Communication",
            "17": "Military and Protective Services",
            "18": "Operations",
            "19": "Product Management",
            "20": "Program and Project Management",
            "21": "Purchasing",
            "22": "Quality Assurance",
            "23": "Real Estate",
            "24": "Research",
            "25": "Sales",
            "26": "Support"
            }
        mapFunction(code_dict, comp_statistics_formated['followStatistics']['functions']['values'])
        comp_statistics_formated['followStatistics']['functions']['id']=li_comp_id_formal
        db_connection_formal.li_followers_function.insert(comp_statistics_formated['followStatistics']['functions'])
    if comp_statistics_formated['followStatistics'].has_key('countries')==1 and comp_statistics_formated['followStatistics']['countries']['_total']>0:
        country_codes = db_connection_formal.country_code.find({}, {'_id':0})
        for codes in country_codes:
            code_list=codes
        country_dict = {(y.lower()):x for x,y in code_list.iteritems()}
        mapFunction(country_dict, comp_statistics_formated['followStatistics']['countries']['values'])
        comp_statistics_formated['followStatistics']['countries']['id']=li_comp_id_formal
        db_connection_formal.li_followers_country.insert(comp_statistics_formated['followStatistics']['countries'])
    if comp_statistics_formated['followStatistics'].has_key('seniorities')==1 and comp_statistics_formated['followStatistics']['seniorities']['_total']>0:
        seniority_code = {
            "1":"Unpaid",
            "2":"Training",
            "3":"Entry",
            "4":"Senior",
            "5":"Manager",
            "6":"Director",
            "7":"VP",
            "8":"CXO",
            "9":"Partner",
            "10":"Owner"
            }
        mapFunction(seniority_code, comp_statistics_formated['followStatistics']['seniorities']['values'])
        comp_statistics_formated['followStatistics']['seniorities']['id'] = li_comp_id_formal
        db_connection_formal.li_followers_seniority.insert(comp_statistics_formated['followStatistics']['seniorities'])
    if comp_statistics_formated['followStatistics'].has_key('industries')==1 and comp_statistics_formated['followStatistics']['industries']['_total']>0:
        industry_code = db_connection_formal.li_industry_code.find({}, {'_id':0})
        for industry in industry_code:
            industries = industry
        mapFunction(industries, comp_statistics_formated['followStatistics']['industries']['values'])
        comp_statistics_formated['followStatistics']['industries']['id']=li_comp_id_formal
        db_connection_formal.li_followers_industry.insert(comp_statistics_formated['followStatistics']['industries'])
def mapFunction(dict1, dict2):
    for item in dict2:
        val = ''
        if dict1.has_key(item['entryKey']):
            val = dict1[item['entryKey']]
            item['entryKey']=val
            item['entryValue'] = int(item['entryValue'])




