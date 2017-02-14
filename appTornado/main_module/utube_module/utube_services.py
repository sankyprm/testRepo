from tornado.httpclient import AsyncHTTPClient as http_client
import datetime
def updateToken(user_id_formal, accessToken_formal, refreshToken, urllib_formal, json_formal, db_connection_formal):
    endpoint='https://accounts.google.com/o/oauth2/token'
    data={'client_id':'251325809315-67qlt3josiblkvkdcc54emu4qavb4m7f.apps.googleusercontent.com','client_secret':'40AC13yncUPj-KoNtURDx4aM','refresh_token':refreshToken,'grant_type':'refresh_token'}
    encodedData=urllib_formal.urlencode(data)
    def handle_request(response):
        if response.error:
            print "Errors:", response.error
        else:
            accessToken=json_formal.loads(response.body)['access_token']
            db_connection_formal.brands.update({'associated_accounts.utube_accounts.page_id':str(user_id_formal)},{'$set':{'associated_accounts.utube_accounts.$.accessToken':accessToken}},upsert=False)
    http_client.fetch(endpoint, handle_request, method='POST', headers=None, body=encodedData)
def verifyToken(token, urllib_formal, json_formal):
    url="https://www.googleapis.com/oauth2/v1/tokeninfo?access_token={0}".format(token)
    res=urllib_formal.urlopen(url).read()
    response=json_formal.loads(res)
    #print('==============')
    #print(response)
    if 'error' in response:
        return False
    if 'expires_in' in response:
        if response['expires_in']<300:
            return False
    return True
def basicDataFetch(user_id_formal, access_token_formal, db_connection_formal, urllib_formal, json_formal, brand_id):
    today = datetime.date.today()
    one_month_earlier = today - datetime.timedelta(days=30)
    edate = today.strftime('%Y-%m-%d')
    sdate = one_month_earlier.strftime('%Y-%m-%d')
    matrx = 'views,comments,favoritesAdded,favoritesRemoved,likes,dislikes,shares,estimatedMinutesWatched,averageViewDuration,averageViewPercentage'
    url = 'https://www.googleapis.com/youtube/analytics/v1/reports?ids=channel=={id}&start-date={s_date}&end-date={e_date}&metrics={matrix}&dimensions=day&access_token={token}'.format(id=user_id_formal, s_date=sdate, e_date=edate, matrix =matrx, token=access_token_formal)
    response = urllib_formal.urlopen(url)
    response_formated = json_formal.loads(response.read())
    print "response_formated basic===>>>", response_formated
    if 'rows' in response_formated:
        counter1 = 0
        while counter1<len(response_formated['rows']):
            response_json = {'value':{}}
            response_json['date'] =datetime.datetime.strptime(response_formated['rows'][counter1][0], '%Y-%m-%d')
            response_json['channel_id'] = user_id_formal
            response_json['brand_id'] = [brand_id]
            counter2 = 1
            while counter2 < len(response_formated['columnHeaders']):
                response_json['value'][response_formated['columnHeaders'][counter2]['name']]= response_formated['rows'][counter1][counter2]
                counter2+=1
            db_connection_formal.utube_basic_details.insert(response_json)
            counter1+=1
    else:
        response_json = {'value':{}}
        response_json['date'] =datetime.datetime.now()
        response_json['channel_id'] = user_id_formal
        response_json['brand_id'] = [brand_id]
        counter2 = 1
        while counter2 < len(response_formated['columnHeaders']):
            response_json['value'][response_formated['columnHeaders'][counter2]['name']]= 0
            counter2+=1
        db_connection_formal.utube_basic_details.insert(response_json)
    return 1
def channelGrowth(user_id_formal, access_token_formal, db_connection_formal, urllib_formal, json_formal):
    today = datetime.date.today()
    one_month_earlier = today - datetime.timedelta(days=30)
    edate = today.strftime('%Y-%m-%d')
    sdate = one_month_earlier.strftime('%Y-%m-%d')
    matrx = 'subscribersGained,subscribersLost'
    url = 'https://www.googleapis.com/youtube/analytics/v1/reports?ids=channel=={id}&start-date={s_date}&end-date={e_date}&metrics={matrix}&dimensions=day&access_token={token}'.format(id=user_id_formal, s_date=sdate, e_date=edate, matrix =matrx, token=access_token_formal)
    response = urllib_formal.urlopen(url)
    response_formated = json_formal.loads(response.read())
    print "response_formated channel growth===>>>", response_formated
    if 'rows' in response_formated:
        counter1 = 0
        while counter1<len(response_formated['rows']):
            response_json = {'value':{}}
            response_json['date'] =datetime.datetime.strptime(response_formated['rows'][counter1][0], '%Y-%m-%d')
            response_json['channel_id'] = user_id_formal
            counter2 = 1
            while counter2 < len(response_formated['columnHeaders']):
                response_json['value'][response_formated['columnHeaders'][counter2]['name']]= response_formated['rows'][counter1][counter2]
                counter2+=1
            db_connection_formal.utube_channel_growth.insert(response_json)
            counter1+=1
    else:
        response_json = {'value':{}}
        response_json['date'] =datetime.datetime.now()
        response_json['channel_id'] = user_id_formal
        counter2 = 1
        while counter2 < len(response_formated['columnHeaders']):
            response_json['value'][response_formated['columnHeaders'][counter2]['name']]= 0
            counter2+=1
        db_connection_formal.utube_channel_growth.insert(response_json)
    return 1
def insightTrafficSource(user_id_formal, access_token_formal, db_connection_formal, urllib_formal, json_formal):
    today = datetime.date.today()
    one_month_earlier = today - datetime.timedelta(days=30)
    edate = today.strftime('%Y-%m-%d')
    sdate = one_month_earlier.strftime('%Y-%m-%d')
    matrx = 'views,estimatedMinutesWatched,comments,favoritesAdded,favoritesRemoved,likes,dislikes,shares'
    url = 'https://www.googleapis.com/youtube/analytics/v1/reports?ids=channel=={id}&start-date={s_date}&end-date={e_date}&metrics={matrix}&dimensions=country&access_token={token}'.format(id=user_id_formal, s_date=sdate, e_date=edate, matrix =matrx, token=access_token_formal)
    response = urllib_formal.urlopen(url)
    response_formated = json_formal.loads(response.read())
    print "response_utube insight=========>>>>>>", response_formated
    if 'rows' in response_formated:
        counter1 = 0
        while counter1<len(response_formated['rows']):
            response_json = {'value':{}}
            response_json['date'] =today = datetime.datetime.now()
            response_json['channel_id'] = user_id_formal
            counter2 = 0
            while counter2 < len(response_formated['columnHeaders']):
                if response_formated['columnHeaders'][counter2]['name']== 'country':
                    country_codes = db_connection_formal.country_code.find({}, {'_id':0})
                    for codes in country_codes:
                        code_list=codes
                    country_dict = {y:x for x,y in code_list.iteritems()}
                    response_formated['rows'][counter1][counter2]=country_dict[response_formated['rows'][counter1][counter2]]
                response_json['value'][response_formated['columnHeaders'][counter2]['name']]= response_formated['rows'][counter1][counter2]
                counter2+=1
            db_connection_formal.utube_traffic_source.insert(response_json)
            counter1+=1
    else:
        response_json = {'value':{}}
        response_json['date'] =today = datetime.datetime.now()
        response_json['channel_id'] = user_id_formal
        db_connection_formal.utube_traffic_source.insert(response_json)
    return 1
def activePlatform(user_id_formal, access_token_formal, db_connection_formal, urllib_formal, json_formal):
    today = datetime.date.today()
    one_month_earlier = today - datetime.timedelta(days=30)
    edate = today.strftime('%Y-%m-%d')
    sdate = one_month_earlier.strftime('%Y-%m-%d')
    matrx = 'views,estimatedMinutesWatched'
    url = 'https://www.googleapis.com/youtube/analytics/v1/reports?ids=channel=={id}&start-date={s_date}&end-date={e_date}&metrics={matrix}&dimensions=deviceType&access_token={token}'.format(id=user_id_formal, s_date=sdate, e_date=edate, matrix =matrx, token=access_token_formal)
    response = urllib_formal.urlopen(url)
    response_formated = json_formal.loads(response.read())
    print "response_formated activePlatform===>>>", response_formated
    if 'rows' in response_formated:
        counter1 = 0
        while counter1<len(response_formated['rows']):
            response_json = {'value':{}}
            response_json['date'] =datetime.datetime.now()
            response_json['channel_id'] = user_id_formal
            counter2 = 0
            while counter2 < len(response_formated['columnHeaders']):
                response_json['value'][response_formated['columnHeaders'][counter2]['name']]= response_formated['rows'][counter1][counter2]
                counter2+=1
            db_connection_formal.utube_active_platform.insert(response_json)
            counter1+=1
    else:
        response_json = {'value':{}}
        response_json['date'] =datetime.datetime.now()
        response_json['channel_id'] = user_id_formal
        counter2 = 0
        while counter2 < len(response_formated['columnHeaders']):
            response_json['value'][response_formated['columnHeaders'][counter2]['name']]= 0
            counter2+=1
        db_connection_formal.utube_active_platform.insert(response_json)
    return 1
def insightFromMedia(user_id_formal, access_token_formal, db_connection_formal, urllib_formal, json_formal):
    today = datetime.date.today()
    one_month_earlier = today - datetime.timedelta(days=30)
    edate = today.strftime('%Y-%m-%d')
    sdate = one_month_earlier.strftime('%Y-%m-%d')
    matrx = 'views,estimatedMinutesWatched'
    url = 'https://www.googleapis.com/youtube/analytics/v1/reports?ids=channel=={id}&start-date={s_date}&end-date={e_date}&metrics={matrix}&dimensions=insightTrafficSourceType&access_token={token}'.format(id=user_id_formal, s_date=sdate, e_date=edate, matrix =matrx, token=access_token_formal)
    response = urllib_formal.urlopen(url)
    response_formated = json_formal.loads(response.read())
    print "response_formated insight from media===>>>", response_formated
    if 'rows' in response_formated:
        counter1 = 0
        while counter1<len(response_formated['rows']):
            response_json = {'value':{}}
            response_json['date'] =datetime.datetime.now()
            response_json['channel_id'] = user_id_formal
            counter2 = 0
            while counter2 < len(response_formated['columnHeaders']):
                response_json['value'][response_formated['columnHeaders'][counter2]['name']]= response_formated['rows'][counter1][counter2]
                counter2+=1
                print "counter2"
            db_connection_formal.utube_insight_tsType.insert(response_json)
            print "counter1"
            counter1+=1
    else:
        response_json = {'value':{}}
        response_json['date'] =datetime.datetime.now()
        response_json['channel_id'] = user_id_formal
        counter2 = 0
        while counter2 < len(response_formated['columnHeaders']):
            response_json['value'][response_formated['columnHeaders'][counter2]['name']]= 0
            counter2+=1
            print "counter2"
        db_connection_formal.utube_insight_tsType.insert(response_json)
    return 1

def insightPlayback(user_id_formal, access_token_formal, db_connection_formal, urllib_formal, json_formal):
    today = datetime.date.today()
    one_month_earlier = today - datetime.timedelta(days=30)
    edate = today.strftime('%Y-%m-%d')
    sdate = one_month_earlier.strftime('%Y-%m-%d')
    matrx = 'views,estimatedMinutesWatched'
    url = 'https://www.googleapis.com/youtube/analytics/v1/reports?ids=channel=={id}&start-date={s_date}&end-date={e_date}&metrics={matrix}&dimensions=insightPlaybackLocationType&access_token={token}'.format(id=user_id_formal, s_date=sdate, e_date=edate, matrix =matrx, token=access_token_formal)
    response = urllib_formal.urlopen(url)
    response_formated = json_formal.loads(response.read())
    print "response_formated insight from playback===>>>", response_formated
    if 'rows' in response_formated:
        counter1 = 0
        while counter1<len(response_formated['rows']):
            response_json = {'value':{}}
            response_json['date'] =datetime.datetime.now()
            response_json['channel_id'] = user_id_formal
            counter2 = 0
            while counter2 < len(response_formated['columnHeaders']):
                response_json['value'][response_formated['columnHeaders'][counter2]['name']]= response_formated['rows'][counter1][counter2]
                counter2+=1
            db_connection_formal.utube_insight_playback.insert(response_json)
            counter1+=1
    else:
        response_json = {'value':{}}
        response_json['date'] =datetime.datetime.now()
        response_json['channel_id'] = user_id_formal
        counter2 = 0
        while counter2 < len(response_formated['columnHeaders']):
            response_json['value'][response_formated['columnHeaders'][counter2]['name']]= 0
            counter2+=1
        db_connection_formal.utube_insight_playback.insert(response_json)
        pass
    return 1
def getAllVideos(user_id_formal, access_token_formal, db_connection_formal, urllib_formal, json_formal):
    url = 'https://www.googleapis.com/youtube/v3/channels?part=contentDetails&mine=true&access_token={token}'.format(token=access_token_formal)
    response = urllib_formal.urlopen(url)
    response_formated = json_formal.loads(response.read())
    video_idlist=[]
    uploads = response_formated['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    video_api_url = 'https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&playlistId={listId}&access_token={token}'.format(listId=uploads, token=access_token_formal)
    video_response = urllib_formal.urlopen(video_api_url)
    video_response_formated = json_formal.loads(video_response.read())
    print "video_response_formated===>>>>>", video_response_formated
    video_list = []
    if len(video_response_formated['items'])>0:
        for video in video_response_formated['items']:
            video['snippet']['sentiment']=''
            video['snippet']['keywords'] = []
            video['snippet']['rank'] = 9856
            video['snippet']['publishedAt'] = datetime.datetime.strptime(video['snippet']['publishedAt'], "%Y-%m-%dT%H:%M:%S.000Z")
            video_idlist.append(video['snippet']['resourceId']['videoId'])
            video_detail_url = 'https://gdata.youtube.com/feeds/api/videos/{videoId}?v=2&alt=json&prettyprint=true'.format(videoId=video['snippet']['resourceId']['videoId'])
            try:
                video_detail = json_formal.loads((urllib_formal.urlopen(video_detail_url)).read())
                video['snippet']['link']=video_detail['entry']['link']
                video['snippet']['comments']=video_detail['entry']['gd$comments']
                video['snippet']['statistics']=video_detail['entry']['yt$statistics']
                video['snippet']['rating']=video_detail['entry']['yt$rating']
                video_list.append(video['snippet'])

            except:
                video['snippet']['link']=''
                video['snippet']['comments']={
        "gd$feedLink" : {
            "href" : "",
            "countHint" : 0,
            "rel" : ""
        }}
                video['snippet']['statistics']={
        "viewCount" : 0,
        "favoriteCount" : 0
    }
                video['snippet']['rating']={
        "numLikes" : 0,
        "numDislikes" : 0
    }
                video_list.append(video['snippet'])
        if len(video_list)>0:
            db_connection_formal.utube_channel_videos.insert(video_list)
    return video_idlist

def getAllComments(user_id_formal, access_token_formal, db_connection_formal, urllib_formal, json_formal, video_idlist_formal):
    print "video_idlist_formal", video_idlist_formal
    for each_video in video_idlist_formal:
        url = 'http://gdata.youtube.com/feeds/api/videos/{video_id}/comments?v=2&alt=json&prettyprint=true'.format(video_id=each_video)
        response = urllib_formal.urlopen(url)
        try:
            response_formated = json_formal.loads(response.read())
            print "response_formated getAllComments===>>>", response_formated
            comment_json = {
                    "gd_etag": "",
                    "id": '',
                    "published": '',
                    "updated": '',
                    "category": [

        ],
                    "title": '',
                    "content": '',
                    "link": [

        ],
                    "author":
                        {
                            "name": '',
                            "uri": '',
                            "yt_userId": ''
         },
                    "yt_channelId": '',
                    "yt_googlePlusUserId":'',
                    "yt_replyCount": '',
                    "yt_videoid": '',
                    "sentiment":'',
                    "keywords":[]
       }
            if len(response_formated['feed']['entry'])>0:
                cmnt_list = []
                for cmnt in response_formated['feed']['entry']:
                    comment_json['gd_etag']=cmnt['gd$etag']
                    comment_json['id'] = cmnt['id']['$t']
                    comment_json['published'] = datetime.datetime.strptime(cmnt['published']['$t'], "%Y-%m-%dT%H:%M:%S.000Z")
                    comment_json["updated"] = datetime.datetime.strptime(cmnt['updated']['$t'], "%Y-%m-%dT%H:%M:%S.000Z")
                    comment_json['category'] = cmnt['category']
                    comment_json['title'] = cmnt['title']['$t']
                    comment_json["content"] = cmnt['content']['$t']
                    comment_json['link'] = cmnt['link']
                    comment_json['author']['name'] = cmnt['author'][0]['name']['$t']
                    comment_json['author']['uri'] = cmnt['author'][0]['uri']['$t']
                    comment_json['author']['yt_userId'] = cmnt['author'][0]['yt$userId']['$t']
                    comment_json['yt_channelId'] = user_id_formal
                    comment_json['yt_googlePlusUserId'] = cmnt['yt$googlePlusUserId']['$t']
                    comment_json['yt_replyCount'] = cmnt['yt$replyCount']['$t']
                    comment_json['yt_videoid'] = cmnt['yt$videoid']['$t']
                    cmnt_list.append(comment_json.copy())
                db_connection_formal.utube_video_comments.insert(cmnt_list)
        except:
            pass
    return True
def topVideos(user_id_formal, access_token_formal, db_connection_formal, urllib_formal, json_formal):
    db_connection_formal.utube_channel_videos.update({'channelId':user_id_formal},{'$set':{'rank':0}})
    url = 'https://www.googleapis.com/youtube/v3/search?part=snippet&forMine=true&order=viewCount&type=video&access_token={token}'.format(token = access_token_formal)
    response = urllib_formal.urlopen(url)
    response_formated = json_formal.loads(response.read())
    print "response_formated top video", response_formated
    if response_formated.has_key('error'):
        return False
    else:
        rank = 0
        video_list = []
        for video in response_formated['items']:
            db_connection_formal.utube_channel_videos.update({'resourceId.videoId':video['id']['videoId']},{'$set':{'rank':rank}})
            rank +=1
        return True


