# -*- coding: utf-8 -*-
"""
Created on Wed Jun 29 11:44:15 2016

@author: april.liu
"""

from googleads import adwords


# Specify where to download the file here.
PATH = '/Users/april.liu/Documents/Search Term Report/keyword_api_test.csv'


def main(client, path):
  # Initialize appropriate service.
  report_downloader = client.GetReportDownloader(version='v201605')

  # Create report query.
  report_query = ('''SELECT CampaignName, AdGroupName,  Criteria, KeywordMatchType, 
                  Impressions, Clicks, Cost 
                  FROM KEYWORDS_PERFORMANCE_REPORT 
                  WHERE Status IN [ENABLED, PAUSED] and CampaignId = 431091500
                  DURING 20160101, 20160628''')

  with open(path, 'w') as output_file:
    report_downloader.DownloadReportWithAwql(
        report_query, 'CSV', output_file, skip_report_header=False,
        skip_column_header=False, skip_report_summary=False)

  print 'Report was downloaded to \'%s\'.' % path


if __name__ == '__main__':
  # Initialize client object.
  adwords_client = adwords.AdWordsClient.LoadFromStorage()
  adwords_client.SetClientCustomerId('494-535-2792')

  main(adwords_client, PATH)



import pandas as pd
import numpy as np
df = pd.read_csv('/Users/april.liu/Documents/Search Term Report/keyword_api_test.csv', header=1)
new_keyword = []
for i in df[u'Keyword']:
    m = ' '+i
    new_keyword.append(m)
df['new_keyword'] = np.array(new_keyword)
df = df.drop(u'Keyword', axis=1)
df.to_csv('/Users/april.liu/Documents/Search Term Report/Keyword report_Salt Lake City.csv', header=1, index=False)


from googleads import adwords


# Specify where to download the file here.
PATH = '/Users/april.liu/Documents/Search Term Report/Search term report_Salt Lake City.csv'


def main(client, path):
  # Initialize appropriate service.
  report_downloader = client.GetReportDownloader(version='v201605')

  # Create report query.
  report_query = ('''SELECT CampaignName, QueryMatchTypeWithVariant, Query, AdGroupName, 
                  Impressions, Clicks, Cost, Conversions
                  FROM SEARCH_QUERY_PERFORMANCE_REPORT 
                  WHERE CampaignId = 431091500
                  DURING 20160101, 20160628''')

  with open(path, 'w') as output_file:
    report_downloader.DownloadReportWithAwql(
        report_query, 'CSV', output_file, skip_report_header=False,
        skip_column_header=False, skip_report_summary=False)

  print 'Report was downloaded to \'%s\'.' % path


if __name__ == '__main__':
  # Initialize client object.
  adwords_client = adwords.AdWordsClient.LoadFromStorage()
  adwords_client.SetClientCustomerId('494-535-2792')

  main(adwords_client, PATH)



# coding: utf-8

# In[1]:

import pandas as pd



# In[2]:

from sqlalchemy import create_engine
from openpyxl import load_workbook


# In[3]:

engine_RS = create_engine('postgresql://april.liu:VVUamGKs71@10.0.7.23:5439/rmus_prod') 


# In[4]:

Listing_Info_Query = ''' select 
upper(city) ,property_type, upper(city)||property_type as "upper&property_type",
state_code, 
business_market_id, 
count(listing_id)
from edw.listing_dim
where search_status IN ('Active') and business_market_id IN (select
business_market_id
from edw.business_market_dim
where business_market_name = 'Salt Lake City') and list_price_amount > 250000
and (listing_date >= '2015-03-01' or listing_added_date >= '2015-03-01') 
and property_type in ('Condo/Co-op', 'Single Family Residential', 'Townhouse') 
group by  upper(city), state_code,  business_market_id, property_type

order by property_type, count(listing_id) desc ''' # City Name


# In[5]:

listing_info = pd.read_sql(Listing_Info_Query, con = engine_RS)


# In[6]:

Zip_Query = '''select 
zip ,property_type, zip||property_type as "upper&property_type",
state_code, 
business_market_id, 
count(listing_id)
from edw.listing_dim
where search_status IN ('Active') and business_market_id IN (select
business_market_id
from edw.business_market_dim
where business_market_name = 'Salt Lake City') and list_price_amount > 250000
and (listing_date >= '2015-03-01' or listing_added_date >= '2015-03-01') 
and property_type in ('Condo/Co-op', 'Single Family Residential', 'Townhouse') 
group by  zip, state_code,  business_market_id, property_type

order by property_type, count(listing_id) desc'''


# In[7]:

zip_info = pd.read_sql(Zip_Query, con = engine_RS)


# read keyword reporting

# In[8]:

keyword = pd.read_csv('/Users/april.liu/Documents/Search Term Report/Keyword report_Salt Lake City.csv',header=0,skiprows=0) #City Name
keyword = keyword[:-1]

# put all keywords in one list

# In[9]:

import re


# In[10]:

keyword_list =[]
for row in keyword['new_keyword']:
    row=row.split()
    keyword_list.append(row)


# remove '+' and space from keywords and put keywords into list

# In[11]:

kwd_list = []
for a in keyword_list:
    for i in a:
        eachkwd = re.sub(r'[^\w]','',i).lower().strip()
        if eachkwd not in kwd_list:
            kwd_list.append(eachkwd)


# read search term reporting

# In[12]:

df = pd.read_csv('/Users/april.liu/Documents/Search Term Report/Search term report_Salt Lake City.csv',header=1,skiprows=0) #City Name
df=df[:-1]


# In[13]:

search_term_list_split= []
search_term_list = []
search_term = df[u'Search term']


# In[14]:

for i in search_term:
    search_term_list.append(i)
    i = i.split()
    search_term_list_split.append(i)


# # create one word frequency dataframe

# In[15]:

def ngrams_1(input, n):
  input = input.split(' ')
  output = []
  for i in range(len(input)-n+1):
    output.append(input[i:i+n])
  return output


# In[16]:

def output_word(n):
    output_word = []
    for query in search_term_list:
        subquery = [' '.join(x) for x in ngrams_1(query, n)] 
        output_word.append(subquery)
    return output_word


# In[17]:

search_term_list_oneword = output_word(1)


# In[18]:

def frequency_dic(n):
    frequency = {}
    for m in output_word(n): 
        for keyword in m:
            if keyword not in frequency:
                frequency[keyword] = 1
            elif keyword in frequency:
                frequency[keyword] += 1
    return frequency


# add impressions as another column

# In[19]:

def total_impr_dic(n):
    total_impr = {}
    I = df['Impressions']
    impression_list = []
    for j in I:
        impression_list.append(j)
    impressioneachword = []
    for rownumber in range(len(impression_list)):
        for a in output_word(n)[rownumber]: #output_word(n)
            impressioneachword.append((a,impression_list[rownumber]))
    for item in impressioneachword:
        k,v = item
        if k not in total_impr:
            total_impr[k] = v
        elif k in total_impr:
            total_impr[k] += v
    return total_impr


# add clicks as another column

# In[20]:

def total_clicks_dic(n):
    total_clicks = {}
    C = df['Clicks']
    clicks_list = []
    for j in C:
        clicks_list.append(j)
    clickseachword = []
    for rownumber in range(len(clicks_list)):
        for a in output_word(n)[rownumber]: #output_word(n)
            clickseachword.append((a,clicks_list[rownumber]))
    for item in clickseachword:
        k,v = item
        if k not in total_clicks:
            total_clicks[k] = v
        elif k in total_clicks:
            total_clicks[k] += v
    return total_clicks


# add conversions as another column

# In[21]:

def total_conversions_dic(n):
    total_conversions = {}
    Conv = df['Conversions']
    conversions_list = []
    for j in Conv:
        conversions_list.append(j)
    conversionseachword = []
    for rownumber in range(len(conversions_list)):
        for a in output_word(n)[rownumber]: #output_word(n)
            conversionseachword.append((a,conversions_list[rownumber]))
    for item in conversionseachword:
        k,v = item
        if k not in total_conversions:
            total_conversions[k] = v
        elif k in total_conversions:
            total_conversions[k] += v
    return total_conversions


# In[22]:

def search_term_analysis_dataframe(n):
    if n == 1:
        frequency_new = {k:v for k,v in frequency_dic(n).items() if k not in kwd_list}
        frequency_dataframe = pd.DataFrame(frequency_new.items(), columns = ['word','frequency'])
        frequency_dataframe['total_impr'] = frequency_dataframe['word'].map(total_impr_dic(n))
        frequency_dataframe['total_clicks'] = frequency_dataframe['word'].map(total_clicks_dic(n))
        frequency_dataframe['total_conversions'] = frequency_dataframe['word'].map(total_conversions_dic(n))
        return frequency_dataframe.sort_values(by =['total_impr'],ascending=False)
    else:
        frequency_dataframe = pd.DataFrame(frequency_dic(n).items(),columns = ['word','frequency'])
        frequency_dataframe['total_impr'] = frequency_dataframe['word'].map(total_impr_dic(n))
        frequency_dataframe['total_clicks'] = frequency_dataframe['word'].map(total_clicks_dic(n))
        frequency_dataframe['total_conversions'] = frequency_dataframe['word'].map(total_conversions_dic(n))
        return frequency_dataframe.sort_values(by =['total_impr'],ascending=False)


# # write to excel

# In[23]:

book = load_workbook(r'C:\Users\april.liu\Documents\Search Term Report\Template\Search term report_Template.xlsx')
writer = pd.ExcelWriter(r'C:\Users\april.liu\Documents\Search Term Report\Template\Search term report_Template.xlsx',engine='openpyxl')
writer.book = book
writer.sheets = dict((ws.title,ws) for ws in book.worksheets)
search_term_analysis_dataframe(1).to_excel(writer,index=False,sheet_name='one_word_frequency', header=True)
search_term_analysis_dataframe(2).to_excel(writer,index=False,sheet_name='two_words_frequency', header=True)
search_term_analysis_dataframe(3).to_excel(writer,index=False,sheet_name='three_words_frequency', header=True)
listing_info.to_excel(writer,index=False,sheet_name='active listing city', header=True)
zip_info.to_excel(writer,index=False,sheet_name='active listing zip', header=True)
writer.save()


# In[ ]:



