import time
from datetime import datetime
import re
import urllib
import json
def basicInfoFetch(db_connection_formal,li_comp_id_formal,access_token_formal, brand_id_formal, master_page_id):
    url = 'https://api.linkedin.com/v1/companies/'+str(li_comp_id_formal)+':(id,name,ticker,description,company-type,website-url,industries,status,logo-url,twitter-id,employee-count-range,specialties,stock-exchange,num-followers)?oauth2_access_token='+str(access_token_formal)+'&format=json'
    print url, '========', access_token_formal
    basic_info = urllib.urlopen(url)
    basic_info_formated = json.loads(basic_info.read())
    try:
        basic_info_formated['brand_id']=brand_id_formal
        basic_info_formated['master_page']=master_page_id
        db_connection_formal.li_basic_info_comp.remove({'id':int(li_comp_id_formal)})
        db_connection_formal.li_basic_info_comp.insert(basic_info_formated)
    except:
        pass
    return 1