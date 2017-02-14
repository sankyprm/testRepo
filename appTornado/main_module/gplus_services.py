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
        if(not self.verifyToken(self.accessToken)): self.updateToken(self.refreshToken)
        else: print('Token not Updated')


    def updateToken(self,refreshToken):
        endpoint='https://accounts.google.com/o/oauth2/token'
        data={'client_id':'251325809315-67qlt3josiblkvkdcc54emu4qavb4m7f.apps.googleusercontent.com','client_secret':'40AC13yncUPj-KoNtURDx4aM','refresh_token':self.refreshToken,'grant_type':'refresh_token'}
        encodedData=urllib.urlencode(data)
        httpclient=AsyncHTTPClient()
        def handle_request(response):
            if response.error:
                print "Errors:", response.error
            else:
                self.accessToken=json.loads(response.body)['access_token']
                self.db.brands.update({'associated_accounts.google_accounts.page_id':self.client_id},{'$set':{'associated_accounts.google_accounts.$.accessToken':self.accessToken}},upsert=False)
        httpclient.fetch(endpoint, handle_request, method='POST', headers=None, body=encodedData)



    def verifyToken(self,token):
        url="https://www.googleapis.com/oauth2/v1/tokeninfo?access_token={0}".format(token)
        res=urllib.urlopen(url).read()
        response=json.loads(res)
        #print('==============')
        #print(response)
        if 'error' in response:
            return False
        elif 'expires_in' in response:
            if response['expires_in']<300:
                return False
        return True

    def runFirst(self):
        # First Fetch
        self.getClientCredentials()
        self.getProfile()
        self.getPeoples()
        return (self.client_id,self.user_id)

    def getProfile(self):
        resource_url='https://www.googleapis.com/plus/v1/people/{0}?access_token={1}'.format(self.client_id,self.accessToken)
        http_client=AsyncHTTPClient()
        http_client.fetch(resource_url,self.handle_profile_data)

    def getPeoples(self):
        resource_url='https://www.googleapis.com/plus/v1/people/{0}/people/{1}?access_token={2}'.format(self.client_id,'visible',self.accessToken)
        http_client=AsyncHTTPClient()
        #print(resource_url)
        #exit(0)
        http_client.fetch(resource_url,self.handle_people_data)

    def handle_profile_data(self,response):
        if response.error:
            print "Errorss:", response.error
        else:
            data=response.body
            profile=json.loads(data)
            profile['_id']=ObjectId()
            tot=self.db.gplus_user.find({'id':profile['id']}).count()
            if(tot==0):
                self.db.gplus_user.insert(profile)
                print('Profile Data for {0} update'.format(self.name))
            else:
                print('Profile Data Already Exist')

    def handle_people_data(self,response):
        if response.error:
            print("Errorsss:{0}".format(response.error))
        else:
            #print(response.body)
            data=response.body
            profile=json.loads(data)
            print(profile['totalItems'])
            nextPageToken=''
            totalItems=0
            if 'nextPageToken' in profile:
                nextPageToken=profile['nextPageToken']
            if 'totalItems' in profile:
                totalItems=profile['totalItems']
            if totalItems > 0:
                print('Hiii')
                peoples=(peoples for peoples in profile['items'])
                peopleFetcher=self.fetchPeople()
                peopleFetcher.next()
                for people in peoples:
                    peopleFetcher.send(people)

    def fetchPeople(self):
        resource_url='https://www.googleapis.com/plus/v1/people/{0}?access_token={1}'
        http_client=AsyncHTTPClient()
        while True:
            people=yield
            id=people['id']
            url=resource_url.format(id,self.accessToken)
            print(url)
            http_client.fetch(url,self.addFollowers)


    def addFollowers(self,response):
        if response.error:
            print "Errorss:", response.error
        else:
            data=response.body
            profile=json.loads(data)
            profile['_id']=ObjectId()
            profile['gplus_id']=self.user_id
            tot=self.db.gplus_circles.find({'id':profile['id']}).count()
            if(tot==0):
                self.db.gplus_circles.insert(profile)
                print('Circle Profile Data for {0} update'.format(profile['displayName']))
            else:
                print('Circle Profile Data Already Exist')










