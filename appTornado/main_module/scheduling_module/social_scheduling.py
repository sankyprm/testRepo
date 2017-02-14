__author__ = 'sibia-10'
import twython
import facebook
import json
import urllib
from datetime import datetime

from twitter_scheduling import TwitterScheduling
from facebook_scheduling import FacebookScheduling
from vk_scheduling import VkScheduling
from linkedin_scheduling import LinkedinScheduling

class EntrySchedulingHandler(object):

    def __init__(self,data):
        self.platform = data['platform']

    def getHandler(self):

        serviceName = self.platform+'Scheduling'
        cons = globals()[serviceName]
        print('CONS=====>',cons)
        cons1=cons()
        if cons:
            return cons1
