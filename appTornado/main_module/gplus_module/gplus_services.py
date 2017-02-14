import urllib2
import urllib
import json
from tornado.httpclient import AsyncHTTPClient

from bson.objectid import ObjectId

class Gplus_Services(object):

    def __init__(self,options,db):
        self.client_id = options['client_id']
        self.user_id   = options['userid']
        self.db= db
        self.resources=['People','Activities','Comments','Moments']
        self.http_client=AsyncHTTPClient()

    def getClientCredentials(self):
        res=self.db.user.find({'_id':self.user_id},{'brands':1})
        ids=[ObjectId(a) for a in res[0]['brands']]
        access_token_result = self.db.brands.find({"associated_accounts.google_accounts.page_id":self.client_id,'_id':{'$in':ids}}, {"associated_accounts.google_accounts.accessToken":1,"associated_accounts.google_accounts.refreshToken":1,"associated_accounts.google_accounts.page_id":1,"associated_accounts.google_accounts.name":1, "_id": 0, "associated_accounts.tw_accounts.name":1})
        for accounts in access_token_result[0]['associated_accounts']['google_accounts']:
            #print(accounts)
            if accounts['page_id']==self.client_id:
                self.refreshToken = accounts['refreshToken']
                self.accessToken = accounts['accessToken']
                self.name=accounts['name']
        #print('Hiiii')
        if(self.verifyToken(self.accessToken)==False):  self.updateToken(self.refreshToken)
        else: print('Token not Updated')


    def updateToken(self,refreshToken):
        endpoint='https://accounts.google.com/o/oauth2/token'
        data={'client_id':'251325809315-67qlt3josiblkvkdcc54emu4qavb4m7f.apps.googleusercontent.com','client_secret':'40AC13yncUPj-KoNtURDx4aM','refresh_token':self.refreshToken,'grant_type':'refresh_token'}
        encodedData=urllib.urlencode(data)
        def handle_request(response):
            if response.error:
                print "Errors:", response.error
            else:
                self.accessToken=json.loads(response.body)['access_token']
                self.db.brands.update({'associated_accounts.google_accounts.page_id':self.client_id},{'$set':{'associated_accounts.google_accounts.$.accessToken':self.accessToken}},upsert=False)
        self.http_client.fetch(endpoint, handle_request, method='POST', headers=None, body=encodedData)



    def verifyToken(self,token):
        url="https://www.googleapis.com/oauth2/v1/tokeninfo?access_token={0}".format(token)
        res=urllib.urlopen(url).read()
        response=json.loads(res)
        #print('==============')
        #print(response)
        if 'error' in response:
            return False
        if 'expires_in' in response:
            if response['expires_in']<300:
                return False
        return True

    def runFirst(self):
        # First Fetch
        self.getClientCredentials()
        self.getProfile()
        self.getPeoples()
        self.getActivities()
        return (self.client_id,self.user_id)

    def getProfile(self):
        resource_url='https://www.googleapis.com/plus/v1/people/{0}?access_token={1}'.format(self.client_id,self.accessToken)
        print(resource_url)
        #self.http_client.fetch(resource_url,self.handle_profile_data)
        data=urllib.urlopen(resource_url).read()
        profile=json.loads(data)
        tot=self.db.gplus_user.find({'id':profile['id']}).count()
        if(tot==0):
            profile['_id']=ObjectId()
            self.db.gplus_user.insert(profile)
            print('Profile Data for {0} update'.format(self.name.encode('utf-8')))
        else:
            self.db.gplus_user.update({'_id':profile['id']}, {"$set": profile}, upsert=False)
            dpName=''
            if 'displayName' in profile:
                dpName=profile['displayName'].encode('utf-8')
            print('Profile Data Updated for {0}'.format(dpName))



    def getPeoples(self,nextPageToken=''):
        if nextPageToken:
            print('I am again with new PageToken')
        resource_url='https://www.googleapis.com/plus/v1/people/{0}/people/{1}?access_token={2}&maxResults={3}&pageToken={4}'.format(self.client_id,'visible',self.accessToken,'100',nextPageToken)
        #self.http_client.fetch(resource_url,self.handle_people_data)
        content=urllib.urlopen(resource_url).read()
        self.handle_people_data(content)


    def handle_people_data(self,response):

        #print(response.body)
        data=response
        profile=json.loads(data)
        if 'error' in profile:
            print("Errorsss:{0}".format(response.error))
        else:
            print(profile['totalItems'])
            nextPageToken=''
            totalItems=0
            if 'nextPageToken' in profile:
                nextPageToken=profile['nextPageToken']
            if 'totalItems' in profile:
                totalItems=profile['totalItems']
            if totalItems > 0:
                peoples=(peoples for peoples in profile['items'])
                peopleFetcher=self.fetchPeople()
                peopleFetcher.next()
                for people in peoples:
                    peopleFetcher.send(people)
            if nextPageToken:
                self.getPeoples(nextPageToken=nextPageToken)





    def fetchPeople(self):
        resource_url='https://www.googleapis.com/plus/v1/people/{0}?access_token={1}'
        #http_client=AsyncHTTPClient()
        try:
            while True:
                people=yield
                id=people['id']
                url=resource_url.format(id,self.accessToken)
                data=urllib.urlopen(url).read()
                profile=json.loads(data)
                tot=0
                if 'id' in profile:
                    tot=self.db.gplus_circles.find({'id':profile['id']}).count()
                if(tot==0):
                    profile['_id']=ObjectId()
                    profile['gplus_id']=[self.client_id]
                    self.db.gplus_circles.insert(profile)
                    dpName=''
                    if 'displayName' in profile:
                        dpName=profile['displayName'].encode('utf-8')
                    print('Circle Profile Data for {0} Inserted'.format(dpName))
                else:
                    self.db.gplus_circles.update({'id':profile['id']},{'$set':profile,'$addToSet':{'gplus_id':self.client_id}},upsert=False)
                    dpName=''
                    if 'displayName' in profile:
                        dpName=profile['displayName'].encode('utf-8')
                    print('Circle Profile Data of {0} Updated.'.format(dpName))
        except GeneratorExit as e:
            print(e)
            pass

    def getActivities(self,nextPageToken=''):
        resource_url='https://www.googleapis.com/plus/v1/people/{0}/activities/{1}?maxResults={2}&access_token={3}&pageToken={4}'
        url=resource_url.format(self.client_id,'public','100',self.accessToken,nextPageToken)
        content=urllib.urlopen(url).read()
        self.handleActivities(content)
        #self.http_client.fetch(url,self.handleActivities)

    def handleActivities(self,activities):
        print('In handle activities')
        data=activities
        activities=json.loads(data)
        if 'error' in activities:
            print(activities['error'])
        else:
            #print(activities)
            nextPageToken=''
            totalItems=0
            activityFetcher=self.fetchActivity()
            activityFetcher.next()
            if 'nextPageToken' in activities:
                nextPageToken=activities['nextPageToken']
            if len(activities['items']) > 0:
                activit=(activity for activity in activities['items'])
                for activity in activit:
                    print('Sending Activity')
                    activityFetcher.send(activity)
            if nextPageToken:
                self.getActivities(nextPageToken=nextPageToken)
            else:
                activityFetcher.close()

    def fetchActivity(self):
        try:
            commentFetcher=self.fetchComments()
            commentFetcher.next()
            while True:
                activity=yield
                id=activity['id']
                #print('This is id')
                tot=0
                if 'id' in activity:
                    tot=self.db.gplus_activities.find({'id':activity['id']}).count()
                if(tot==0):
                    activity['_id']=ObjectId()
                    activity['gplus_id']=[self.client_id]
                    self.db.gplus_activities.insert(activity)
                    print('Activity Inserted')
                    print('Fetching Comments')
                    commentFetcher.send(id)
                else:
                    self.db.gplus_circles.update({'id':activity['id']},{'$set':activity,'$addToSet':{'gplus_id':self.client_id}},upsert=False)
                    print('Activity Updated')
                    print('Fetching Comments')
                    commentFetcher.send(id)

        except GeneratorExit as e:
            print(e)
            commentFetcher.close()
            pass

    def fetchComments(self):
        try:

            while True:
                resource_url='https://www.googleapis.com/plus/v1/activities/{0}/comments?maxResults={1}&access_token={2}'
                id=yield
                print('Fetching Comment of {0}'.format(id))
                url= resource_url.format(id,'5',self.accessToken)
                #print(url)
                data=urllib.urlopen(url).read()
                comments=json.loads(data)
                import pprint
                pprint.pprint(comments)
                while ('items' in comments) and (len(comments['items']) > 0):
                    comment=(comment for comment in comments['items'])
                    for cmnt in comment:
                        cmnt['_id']=ObjectId()
                        cmnt['gplus_id']=[self.client_id]
                        self.db.gplus_comments.insert(cmnt)
                        print('Comment Inserted')
                    if ('nextPageToken' in comments) and (comments['nextPageToken']!=''):
                        url=url+'&pageToken={0}'.format(comments['nextPageToken'])
                        print(url)
                        data=urllib.urlopen(url).read()
                        comments=json.loads(data)
                    else:
                        comments=''
        except GeneratorExit as e:
            print(e)
            pass




















