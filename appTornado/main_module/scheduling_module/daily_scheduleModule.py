__author__ = 'sibia-10'
from pymongo import MongoClient
import pprint
from datetime import datetime
import time
import json
from twython import Twython
from tornado.httpclient import AsyncHTTPClient
import urllib
import global_settings


def twScheduleFunc(msg,pic,tokenSecret,token):
    print('hii,i am in tw scheDule///......')
    if pic == 'no_image.png':
        tw_handle = Twython('Qwu1NiiV2uzGp3Hj0UqD1CKo2', '6CiVt3lDPnD4qOaccPOxpcPeTNkZ8h8ljelavK79xKsqpgEEYF',token,tokenSecret)
        update = tw_handle.update_status(status = msg)
        id = update['id']
        return id
    else:
        tw_handle = Twython('Qwu1NiiV2uzGp3Hj0UqD1CKo2', '6CiVt3lDPnD4qOaccPOxpcPeTNkZ8h8ljelavK79xKsqpgEEYF',token,tokenSecret)
        path = '/home/ubuntu/sociabyte/sociabyte_app/public/upload/schedule_pics/'
        photo = open(path+pic,'rb')
        media = tw_handle.upload_media(media=photo, headers={'Content-Type' : 'application/octet-stream'})
        media_id = media['media_id']
        print(media_id)

        update = tw_handle.update_status(status = 'hello',media_ids=[media_id])
        id = update['id']
        return id



'''def linkdScheduleFunc(msg,token,page_id):

    app = linkedin.LinkedInApplication(token=token)
    linked_handle = app.submit_share(comment=msg,submitted_image_url=None)
    pprint.pprint(linked_handle)

    http_client = AsyncHTTPClient()
    endpoint='https://api.linkedin.com/v1/companies/'+page_id+'/shares'
    data={'oauth2_access_token' : token, 'comment' : msg, 'content.submitted_image_url' : None, 'format' : 'json' }
    encodedData=urllib.urlencode(data)
    print('ENCODED===>',encodedData)
    def handle_request(response):
        if response.error:
            print ("Errors:", response.error)
        else:
            print(response.body)
    http_client.fetch(endpoint, handle_request, method='POST', headers=None, body=encodedData)'''




#def mainScheduleFunc():
if __name__ == '__main__':
    db_conn = global_settings.db_conn
    posts = db_conn.sociabyte.scheduling.find({'status' : 'active'},{'_id' : 0})
    for each_post in posts:
        if each_post['platform'] == 'Twitter':

            pprint.pprint(each_post)
            sche_time = each_post['scheduled_timestamp']
            msg = each_post['msg']
            token = each_post['token']
            tokenSecret = each_post['tokenSecret']
            pic = each_post['pic']

            current_time = time.time()
            print('current',current_time)
            max_time = current_time + (15*60)
            print('max',max_time)
            min_time = current_time - (15*60)
            print('min',min_time)

            if (sche_time >= min_time) and (sche_time < max_time):
                tweet_id = twScheduleFunc(msg,pic,tokenSecret,token)
                db_conn.sociabyte.scheduling.update({'scheduled_timestamp' : sche_time,'platform' : 'Twitter'},{'$set':{'returnedValue':tweet_id,'status' : 'inactive'}})
            else:
                print('I am in else')
                pass

        elif each_post['platform'] == 'Vk':
            pprint.pprint(each_post)

        else:
            pass

