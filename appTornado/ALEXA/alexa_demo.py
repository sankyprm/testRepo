__author__ = 'soumik'

import datetime
import urllib
from bs4 import BeautifulSoup
import global_settings
import alexa_settings
import time
from datetime import timedelta

url = "www.anandabazar.com/"
data = {
    'domain_name' : url,
    'fetched_status' : '',
    'fetched_date' : '',
    'alexa_data' : {'UrlInfo' : dict(), 'TrafficHistory' : dict()}
}
UrlInfo_Data = {
                'RelatedLinks' : [],
                'Categories' : [],
                'Rank' : int(),
                'RankByCountry' : [],
                'UsageStats' : { 'UsageStatistic' : [], 'ContributingSubdomain' : [], 'daterange' : {'UsageStatistic' : {'from' : '','to' : ''},'ContributingSubdomain' : {'from' : '','to' : ''}}},
                'ContactInfo' : { 'PhoneNumbers' : [], 'OwnerName' : '','Email' : '','Streets' : [],'City' : '','Country' : ''},
                'AdultContent' : '',
                'Language' : {'Locale' : '','Encoding' : ''},
                'LinksInCount' : '',
                'SiteData' : {'Title' : '', 'Description' : '','OnlineSince' : ''}
                       }

TrafficHistory_data = {'Range' : '',
                       'Site' : '',
                       'Start' : '',
                       'History_Data' : []
                              }

action_list = ['UrlInfo','TrafficHistory']
#action_list = ['TrafficHistory']

def StrToDatetime(dt):

    d = datetime.datetime.strptime(dt,'%Y-%m-%d')
    return d

for each_action in action_list:
    print("each_action=========",each_action)

    if each_action == 'UrlInfo':
        print('Action is========== UrlInfo')

        response_group_list = ['RelatedLinks','Categories','Rank','RankByCountry','UsageStats','ContactInfo','AdultContent','Language','LinksInCount','SiteData']
        #response_group_list = ['SiteData']


        for each_response_group in response_group_list:

            if each_response_group == 'RelatedLinks':
                print('response group========',each_response_group)
                Action = str(each_action)
                Url = url
                ResponseGroup = each_response_group
                response_URL = alexa_settings.AlexaSettings(Action,Url,ResponseGroup)
                print(response_URL)
                xml_string = urllib.urlopen(response_URL).read().decode("utf-8")
                #print( xml_string)
                soup = BeautifulSoup(xml_string,"lxml-xml")
                #print(soup)
                related_link_data_list = []
                RelatedLinks_list = soup.find_all('RelatedLink')
                for each in RelatedLinks_list:
                    #print('DataUrl==============',each.find('DataUrl').get_text())
                    print('DataUrl==============',each.DataUrl.get_text().encode('utf-8'))
                    print('NavigableUrl=================',each.find('NavigableUrl').get_text().encode('utf-8'))
                    print('Title==========',each.find('Title').get_text().encode('utf-8'))
                    each_related_link_data = {'DataUrl' : each.DataUrl.get_text().encode('utf-8'),
                                              'NavigableUrl' : each.find('NavigableUrl').get_text().encode('utf-8'),
                                              'Title' : each.find('Title').get_text().encode('utf-8')}
                    related_link_data_list.append(each_related_link_data)
                    #break
                UrlInfo_Data['RelatedLinks'] = related_link_data_list
            elif each_response_group == 'Categories':
                print('response group========',each_response_group)
                Action = str(each_action)
                Url = url
                ResponseGroup = each_response_group
                response_URL = alexa_settings.AlexaSettings(Action,Url,ResponseGroup)
                print(response_URL)
                xml_string = urllib.urlopen(response_URL).read().decode("utf-8")
                #print( xml_string)
                soup = BeautifulSoup(xml_string,"lxml-xml")
                CategoryData_list = soup.find_all('CategoryData')
                categorical_list = []
                for each in CategoryData_list:
                    print(each.find('Title').get_text())
                    print(each.find('AbsolutePath').get_text())
                    each_category_data = {'Title' : each.find('Title').get_text(),
                                          'AbsolutePath' : each.find('AbsolutePath').get_text()}
                    categorical_list.append(each_category_data)
                UrlInfo_Data['Categories'] = categorical_list

            elif each_response_group == 'Rank':
                print('response group========',each_response_group)
                Action = str(each_action)
                Url = url
                ResponseGroup = each_response_group
                response_URL = alexa_settings.AlexaSettings(Action,Url,ResponseGroup)
                print(response_URL)
                xml_string = urllib.urlopen(response_URL).read().decode("utf-8")
                #print( xml_string)
                soup = BeautifulSoup(xml_string,"lxml-xml")
                rank = soup.find('Rank').get_text()
                print('rank============',rank)
                UrlInfo_Data['Rank'] = rank

            elif each_response_group == 'RankByCountry':
                print('response group========',each_response_group)
                Action = str(each_action)
                Url = url
                ResponseGroup = each_response_group
                response_URL = alexa_settings.AlexaSettings(Action,Url,ResponseGroup)
                print(response_URL)
                xml_string = urllib.urlopen(response_URL).read().decode("utf-8")
                #print( xml_string)
                soup = BeautifulSoup(xml_string,"lxml-xml")
                CountryRank_list = soup.find_all('Country')
                rank_by_country_list = []
                for each in CountryRank_list:
                    print(each.get('Code'))
                    print(each.find('Rank').get_text())
                    print(each.find('PageViews').get_text())
                    print(each.find('Users').get_text())
                    each_country_data = {'CountryCode' : each.get('Code'),
                                         'Rank' : each.find('Rank').get_text(),
                                         'PageViews' : each.find('PageViews').get_text(),
                                         'Users' : each.find('Users').get_text()
                                      }
                    rank_by_country_list.append(each_country_data)
                UrlInfo_Data['RankByCountry'] = rank_by_country_list

            elif each_response_group == 'UsageStats':
                print('response group========',each_response_group)
                Action = str(each_action)
                Url = url
                ResponseGroup = each_response_group
                response_URL = alexa_settings.AlexaSettings(Action,Url,ResponseGroup)
                print(response_URL)
                xml_string = urllib.urlopen(response_URL).read().decode("utf-8")
                #print( xml_string)
                soup = BeautifulSoup(xml_string,"lxml-xml")
                UsageStatistic_tag_list = soup.find_all('UsageStatistic')
                usage_stat_data_list = []
                for each_usage_stat in UsageStatistic_tag_list:
                    try:
                        #print('i m in try')
                        #print(each_usage_stat.find('TimeRange').find('Months').get_text())
                        each_usage_stat_data = {'TimeRange' : each_usage_stat.find('TimeRange').find('Months').get_text()+' months',
                                                'Rank' : { 'Value' : each_usage_stat.find('Rank').find('Value').get_text(), 'Delta' : each_usage_stat.find('Rank').find('Delta').get_text()},
                                                'Reach' :{'Rank' :{ 'Value' : each_usage_stat.find('Reach').find('Rank').find('Value').get_text(), 'Delta' : each_usage_stat.find('Reach').find('Rank').find('Delta').get_text()},
                                                          'PerMillion' : {'Value' : each_usage_stat.find('Reach').find('PerMillion').find('Value').get_text(), 'Delta' : each_usage_stat.find('Reach').find('PerMillion').find('Delta').get_text()}},
                                                'PageViews' : {'Rank' :{ 'Value' : each_usage_stat.find('PageViews').find('Rank').find('Value').get_text(), 'Delta' : each_usage_stat.find('PageViews').find('Rank').find('Delta').get_text()},
                                                               'PerMillion' : {'Value' : each_usage_stat.find('PageViews').find('PerMillion').find('Value').get_text(), 'Delta' : each_usage_stat.find('PageViews').find('PerMillion').find('Delta').get_text()},
                                                               'PerUser' : {'Value' : each_usage_stat.find('PageViews').find('PerUser').find('Value').get_text(), 'Delta' : each_usage_stat.find('PageViews').find('PerUser').find('Delta').get_text()}}
                                                }
                        #print(each_usage_stat_data)
                    except:
                        #print('i m in except')
                        each_usage_stat_data = {'TimeRange' : each_usage_stat.find('TimeRange').find('Days').get_text()+' days',
                                                'Rank' : { 'Value' : each_usage_stat.find('Rank').find('Value').get_text(), 'Delta' : each_usage_stat.find('Rank').find('Delta').get_text()},
                                                'Reach' :{'Rank' :{ 'Value' : each_usage_stat.find('Reach').find('Rank').find('Value').get_text(), 'Delta' : each_usage_stat.find('Reach').find('Rank').find('Delta').get_text()},
                                                          'PerMillion' : {'Value' : each_usage_stat.find('Reach').find('PerMillion').find('Value').get_text(), 'Delta' : each_usage_stat.find('Reach').find('PerMillion').find('Delta').get_text()}},
                                                'PageViews' : {'Rank' :{ 'Value' : each_usage_stat.find('PageViews').find('Rank').find('Value').get_text(), 'Delta' : each_usage_stat.find('PageViews').find('Rank').find('Delta').get_text()},
                                                               'PerMillion' : {'Value' : each_usage_stat.find('PageViews').find('PerMillion').find('Value').get_text(), 'Delta' : each_usage_stat.find('PageViews').find('PerMillion').find('Delta').get_text()},
                                                               'PerUser' : {'Value' : each_usage_stat.find('PageViews').find('PerUser').find('Value').get_text(), 'Delta' : each_usage_stat.find('PageViews').find('PerUser').find('Delta').get_text()}}
                                                }

                    usage_stat_data_list.append(each_usage_stat_data)
                UrlInfo_Data['UsageStats']['UsageStatistic'] = usage_stat_data_list

                contributing_subdomain_data_list = []
                ContributingSubdomain_tag_list = soup.find_all('ContributingSubdomain')
                for each_Subdomain in ContributingSubdomain_tag_list:
                    each_Subdomain_data = {'DataUrl' : each_Subdomain.find('DataUrl').get_text(),
                                           'TimeRange' : each_Subdomain.find('TimeRange').find('Months').get_text()+' months',
                                           'Reach' : each_Subdomain.find('Reach').find('Percentage').get_text(),
                                           'PageViews' : {'Percentage' : each_Subdomain.find('PageViews').find('Percentage').get_text(),'PerUser' : each_Subdomain.find('PageViews').find('PerUser').get_text()},
                                           }
                    contributing_subdomain_data_list.append(each_Subdomain_data)

                UrlInfo_Data['UsageStats']['ContributingSubdomain'] = contributing_subdomain_data_list
                #'UsageStats' : { 'UsageStatistic' : [], 'ContributingSubdomain' : [], 'daterange' : {'UsageStatistic' : {'from' : '','to' : ''},'ContributingSubdomain' : {'from' : '','to' : ''}}},
                UrlInfo_Data['UsageStats']['daterange']['UsageStatistic']['from'] =  StrToDatetime(time.strftime("%Y-%m-%d")) - timedelta(90)
                UrlInfo_Data['UsageStats']['daterange']['UsageStatistic']['to'] =  StrToDatetime(time.strftime("%Y-%m-%d"))
                UrlInfo_Data['UsageStats']['daterange']['ContributingSubdomain']['from'] =  StrToDatetime(time.strftime("%Y-%m-%d")) - timedelta(30)
                UrlInfo_Data['UsageStats']['daterange']['ContributingSubdomain']['to'] =  StrToDatetime(time.strftime("%Y-%m-%d"))

            elif each_response_group == 'ContactInfo':
                print('response group========',each_response_group)
                Action = str(each_action)
                Url = url
                ResponseGroup = each_response_group
                response_URL = alexa_settings.AlexaSettings(Action,Url,ResponseGroup)
                print(response_URL)
                xml_string = urllib.urlopen(response_URL).read().decode("utf-8")
                #print( xml_string)
                soup = BeautifulSoup(xml_string,"lxml-xml")
                phoneNumbers_list = soup.find_all('PhoneNumbers')
                phno_list = []
                for each in phoneNumbers_list:
                    phno_list.append(each.find('PhoneNumber').get_text())
                streets_list = soup.find_all('Streets')
                st_list = []
                for each in streets_list:
                    st_list.append(each.find('Street').get_text())
                UrlInfo_Data['ContactInfo']['PhoneNumbers'] = phno_list
                UrlInfo_Data['ContactInfo']['Streets'] = st_list
                UrlInfo_Data['ContactInfo']['OwnerName'] = soup.find('OwnerName').get_text()
                UrlInfo_Data['ContactInfo']['Email'] = soup.find('Email').get_text()
                UrlInfo_Data['ContactInfo']['City'] = soup.find('City').get_text()
                UrlInfo_Data['ContactInfo']['Country'] = soup.find('Country').get_text()

            elif each_response_group == 'AdultContent':
                print('response group========',each_response_group)
                Action = str(each_action)
                Url = url
                ResponseGroup = each_response_group
                response_URL = alexa_settings.AlexaSettings(Action,Url,ResponseGroup)
                print(response_URL)
                xml_string = urllib.urlopen(response_URL).read().decode("utf-8")
                #print( xml_string)
                soup = BeautifulSoup(xml_string,"lxml-xml")
                UrlInfo_Data['AdultContent'] = soup.find('AdultContent').get_text()

            elif each_response_group == 'Language':
                print('response group========',each_response_group)
                Action = str(each_action)
                Url = url
                ResponseGroup = each_response_group
                response_URL = alexa_settings.AlexaSettings(Action,Url,ResponseGroup)
                print(response_URL)
                xml_string = urllib.urlopen(response_URL).read().decode("utf-8")
                soup = BeautifulSoup(xml_string,"lxml-xml")
                UrlInfo_Data['Language']['Locale'] = soup.find('Locale').get_text()
                UrlInfo_Data['Language']['Encoding'] = soup.find('Encoding').get_text()

            elif each_response_group == 'LinksInCount':
                print('response group========',each_response_group)
                Action = str(each_action)
                Url = url
                ResponseGroup = each_response_group
                response_URL = alexa_settings.AlexaSettings(Action,Url,ResponseGroup)
                print(response_URL)
                xml_string = urllib.urlopen(response_URL).read().decode("utf-8")
                soup = BeautifulSoup(xml_string,"lxml-xml")
                UrlInfo_Data['LinksInCount'] = soup.find('LinksInCount').get_text()

            elif each_response_group == 'SiteData':
                print('response group========',each_response_group)
                Action = str(each_action)
                Url = url
                ResponseGroup = each_response_group
                response_URL = alexa_settings.AlexaSettings(Action,Url,ResponseGroup)
                print(response_URL)
                xml_string = urllib.urlopen(response_URL).read().decode("utf-8")
                soup = BeautifulSoup(xml_string,"lxml-xml")
                UrlInfo_Data['SiteData']['Title'] = soup.find('Title').get_text()
                UrlInfo_Data['SiteData']['Description'] = soup.find('Description').get_text()
                UrlInfo_Data['SiteData']['OnlineSince'] = soup.find('OnlineSince').get_text()

            else:
                pass

        #global_settings.db_conn.sociabyte.alexa_domain_details.insert(UrlInfo_Data)

    elif each_action == 'TrafficHistory':
        print('Action is========== TrafficHistory')
        Action = str(each_action)
        Url = url
        ResponseGroup = 'History'
        response_URL = alexa_settings.AlexaSettings(Action,Url,ResponseGroup)
        print(response_URL)
        xml_string = urllib.urlopen(response_URL).read().decode("utf-8")
        soup = BeautifulSoup(xml_string,"lxml-xml")

        traffic_history_data_list = []

        TrafficHistory_data['Range'] = soup.find('Range').get_text()
        TrafficHistory_data['Site'] = soup.find('Site').get_text()
        TrafficHistory_data['Start'] = StrToDatetime(soup.find('Start').get_text())

        historical_trafficDataList = soup.find_all('Data')

        for each_data in historical_trafficDataList:
            #print('counter===========',counter)
            '''print('Date=====',each_data.Date.get_text())
            print('PageViews.PerMillion=============',each_data.PageViews.PerMillion.get_text())
            print('PageViews.PerUser=============',each_data.PageViews.PerUser.get_text())
            print('Rank================',each_data.Rank.get_text())
            print('Reach.PerMillion=============',each_data.Reach.PerMillion.get_text())'''

            each_history_data = {'Date' : StrToDatetime(each_data.Date.get_text()),
                                 'PageViews' : {'PerMillion' : each_data.PageViews.PerMillion.get_text(), 'PerUser' : each_data.PageViews.PerUser.get_text()},
                                 'Rank' : each_data.Rank.get_text(),
                                 'Reach' : {'PerMillion' : each_data.Reach.PerMillion.get_text()}
                                }
            traffic_history_data_list.append(each_history_data)
        TrafficHistory_data['History_Data'] = traffic_history_data_list
    else:
        pass

data['fetched_status'] = 'fetched'
data['fetched_date'] = StrToDatetime(time.strftime("%Y-%m-%d"))
data['alexa_data']['UrlInfo'] = UrlInfo_Data
data['alexa_data']['TrafficHistory'] = TrafficHistory_data

global_settings.db_conn.sociabyte.alexa_domain_details.insert(data)