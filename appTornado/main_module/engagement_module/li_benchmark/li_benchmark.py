import time
from datetime import datetime
import re
import urllib
import json
def basicInfoFetch(li_comp_id_formal, db_connection_formal, access_token_formal, brand_id_formal, master_page_id):
    url = 'https://api.linkedin.com/v1/companies/'+str(li_comp_id_formal)+':(id,name,ticker,description,company-type,website-url,industries,status,logo-url,twitter-id,employee-count-range,specialties,stock-exchange,num-followers)?oauth2_access_token='+str(access_token_formal)+'&format=json'
    print url, '========', access_token_formal
    basic_info = urllib.urlopen(url)
    basic_info_formated = json.loads(basic_info.read())
    #try:
    basic_info_formated['brand_id']=brand_id_formal
    basic_info_formated['master_page']=master_page_id
    db_connection_formal.li_basic_info_comp.insert(basic_info_formated)
#except:
        #pass
    return 1
'''
def compShareInfo(li_comp_id_formal, db_connection_formal, access_token_formal, brand_id_formal, master_page_id):
    url = 'https://api.linkedin.com/v1/companies/{id}/updates?format=json&start=0&count=250&oauth2_access_token={token}'.format(id=li_comp_id_formal, token=access_token_formal)
    comp_share_info = urllib.urlopen(url)
    comp_share_info_formated = json.loads(comp_share_info.read())
    print(url)
    print('comp_share_info_formated==========================>',comp_share_info_formated)
    update_list = []
    if ((comp_share_info_formated['errorCode'] == 0) and (comp_share_info_formated['status'] == 403)):
        pass
    else:
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
                    updates['brand_id']=brand_id_formal
                    updates['master_page']=master_page_id
                update_list.append(updates.copy())
            if comp_share_info_formated['_total']>0:
                db_connection_formal.li_shares_comp.insert(update_list)
            else:
                pass
        return 1
def commentFetch(li_comp_id_formal, li_stat_id_list_formal, db_connection_formal, access_token_formal, brand_id_formal, master_page_id):
    for update_key in li_stat_id_list_formal:
        comment_list = []
        url = 'https://api.linkedin.com/v1/companies/{id}/updates/key={updatekey}/update-comments?format=json&oauth2_access_token={token}'.format(id=li_comp_id_formal, updatekey=update_key, token=access_token_formal)
        comments = urllib.urlopen(url)
        comments_formated = json.loads(comments.read())
        print('url==================>',url)
        #try:]
        print('comments_formated==================>',comments_formated)
        if comments_formated != 'null':
            if comments_formated['_total']>0:
                for comment in comments_formated['values']:
                    time_stamp= comment['timestamp']/1000
                    timestamp_to_date = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(time_stamp)))
                    comment['timestamp'] = datetime.strptime(timestamp_to_date ,"%Y-%m-%dT%H:%M:%SZ")
                    comment['comment_id'] = comment['id']
                    comment['id'] = li_comp_id_formal
                    comment['update_key'] = update_key
                    comment['sentiment'] = ''
                    comment['brand_id']=brand_id_formal
                    comment['master_page']=master_page_id
                    comment_list.append(comment.copy())
                db_connection_formal.li_comments_comp.insert(comment_list)
        else:
            pass
        #except:
            #pass
    return 1
'''