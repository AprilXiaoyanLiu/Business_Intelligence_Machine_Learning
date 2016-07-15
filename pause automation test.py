# -*- coding: utf-8 -*-
"""
Created on Wed Jul 13 14:04:11 2016

@author: april.liu
"""
import psycopg2
from sqlalchemy import create_engine




patterns = [ "condos", "homes", "housing", "listings", "mls", "townhouses", "vintage", "prices", "houses", "open house", "real estate", "for sale"]
#print reduce(lambda s, pat:s.split(pat,1)[0], patterns, string)

#patterns = ["homes"]
states = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AS': 'American Samoa',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'GU': 'Guam',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MP': 'Northern Mariana Islands',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NA': 'National',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'PR': 'Puerto Rico',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VI': 'Virgin Islands',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming'
}

state_abbreviation = [v for v,l in states.items()]
state_abbreviation.append('-')


import re
iszip = re.compile('[0-9]')

import pandas as pd
df = pd.read_csv('/Users/april.liu/Documents/Expansion New Market/pause automation/test version_7.13.16.csv', header=1, skiprows=0)
df = df[[u'State',u'Ad group', u'Ad group ID']]
ad_group_name = df[u'Ad group']

property_type = []
classfication = []
city_name = []
for m in ad_group_name:
    iszipcheck = iszip.search(m)
    if iszipcheck is not None:
        classfication.append('zipcode')
    elif 'county' in m.lower():
        classfication.append('county') 
    else:
        classfication.append('city')
        
    if 'townhouse' in m.lower() or 'townhome' in m.lower() :
        property_type.append('townhouse')
    elif 'condo' in m.lower():
        property_type.append('condo')
    else:
        property_type.append('single family')
    
    for i in patterns:
        try:
            m = m.split(i,1)[1].strip()
        except IndexError:
            "index error"
    city_name.append(m)
    
    
    # exclude stateabbreviation and -
city_name_final = []
for n in city_name:
    for j in state_abbreviation:
        try:
            n = n.split(j,1)[0].strip()
        except IndexError:
            "index error"
    city_name_final.append(n)
    


df['city_name'] = city_name_final
df['classtype'] = classfication
df['property_type'] = property_type
df = df[:-2]




# check active_listings 

engine_RS = create_engine('postgresql://april.liu:VVUamGKs71@10.0.7.23:5439/rmus_prod') 

# Create Cursor and Connection to Redshift for Update
REDSHIFT_CONNECTION_STRING = 'dbname=rmus_prod user=april.liu password=VVUamGKs71 host=10.0.7.23 port=5439 sslmode=require'
redshift_conn = psycopg2.connect(REDSHIFT_CONNECTION_STRING)
redshift_conn.set_session(autocommit=True)
redshift_cursor = redshift_conn.cursor()
print 'Redshift Connection Success!'


# City_Name
active_listings = '''select lower(city) as city,
state_code,
business_market_id, 
property_type,
count(listing_id) as counts
from edw.listing_dim
where search_status IN ('Active') 
group by lower(city), state_code, property_type,business_market_id
order by count(listing_id) '''

# zipcode
zip_listings = '''select zip as zipcode,
state_code,
business_market_id, 
property_type,
count(listing_id) as counts
from edw.listing_dim
where search_status IN ('Active') 
group by zip, state_code, property_type,business_market_id
order by count(listing_id) '''

activelistings_city_dataframe = pd.read_sql(active_listings, con = engine_RS)
activelistings_zip_dataframe = pd.read_sql(zip_listings, con = engine_RS)

active_singlefamily_dict = (activelistings_city_dataframe[activelistings_city_dataframe['property_type'] == 'Single Family Residential'])[activelistings_city_dataframe['state_code'] == 'TN'].set_index('city')['counts'].to_dict()
active_condo_dict = (activelistings_city_dataframe[activelistings_city_dataframe['property_type'] == 'Condo/Co-op'])[activelistings_city_dataframe['state_code'] == 'TN'].set_index('city')['counts'].to_dict()
active_townhouse_dict = (activelistings_city_dataframe[activelistings_city_dataframe['property_type'] == 'Townhouse'])[activelistings_city_dataframe['state_code'] == 'TN'].set_index('city')['counts'].to_dict()


zip_singlefamily_dict = (activelistings_zip_dataframe[activelistings_zip_dataframe['property_type'] == 'Single Family Residential'])[activelistings_zip_dataframe['state_code'] == 'TN'].set_index('zipcode')['counts'].to_dict()
zip_condo_dict = (activelistings_zip_dataframe[activelistings_zip_dataframe['property_type'] == 'Condo/Co-op'])[activelistings_zip_dataframe['state_code'] == 'TN'].set_index('zipcode')['counts'].to_dict()
zip_townhouse_dict = (activelistings_zip_dataframe[activelistings_zip_dataframe['property_type'] == 'Townhouse'])[activelistings_zip_dataframe['state_code'] == 'TN'].set_index('zipcode')['counts'].to_dict()


# singlefamily
a_singlefamily_dict = {}
for x, y in active_singlefamily_dict.items():
    if x is not None:
        x = x.encode('ascii','ignore')
        a_singlefamily_dict[x] = y
    
df_single_family = df[df['property_type'] == 'single family']
df_single_family_city = df_single_family[df_single_family['classtype'] == 'city']


a_zip_singlefamily_dict = {}
for x, y in zip_singlefamily_dict.items():
    if x is not None:
        x = x.encode('ascii','ignore')
        a_zip_singlefamily_dict[x] = y
    
df_single_family_zip = df_single_family[df_single_family['classtype'] == 'zipcode']



'''
ad_group_id_singlefamily_dict = df_single_family.set_index('Ad group ID')['city_name'].to_dict()


from collections import defaultdict
adgroupid_sfam_dict = defaultdict(list)
adgroupid_sfam_dict = {}
for groupid, name in ad_group_id_singlefamily_dict.items():
    adgroupid_sfam_dict[name].append(groupid)

print adgroupid_sfam_dict'''

counts_singlefamily_dict = {}


for a in df_single_family_city['city_name'].unique():
    if a.lower() in a_singlefamily_dict:
        counts_singlefamily_dict[a] = a_singlefamily_dict[a.lower()]
    else:
        counts_singlefamily_dict[a] = 0

print counts_singlefamily_dict


counts_singlefamily_zip_dict = {}


for a in df_single_family_zip['city_name'].unique():
    if a.lower() in a_zip_singlefamily_dict:
        counts_singlefamily_zip_dict[a] = a_zip_singlefamily_dict[a.lower()]
    else:
        counts_singlefamily_zip_dict[a] = 0

print counts_singlefamily_zip_dict


'''pausing_city = []
df_single_family.to_dict()
for a in df_single_family['city_name'].unique():
    if a.lower() not in a_singlefamily_dict:
        pausing_city.append(a)

adgroupid_list = []
for each_city in pausing_city:
    adgroupidpause = adgroupid_sfam_dict[each_city]
    adgroupid_list.append(adgroupidpause)
        

print adgroupid_list
'''
#condo
a_condo_dict = {}
for x, y in active_condo_dict.items():
    if x is not None:
        x = x.encode('ascii','ignore')
        a_condo_dict[x] = y
        
a_zip_condo_dict = {}
for x, y in zip_condo_dict.items():
    if x is not None:
        x = x.encode('ascii','ignore')
        a_zip_condo_dict[x] = y        

df_condo = df[df['property_type'] == 'condo']
df_condo_city = df_condo[df_condo['classtype'] == 'city']
df_condo_zip = df_condo[df_condo['classtype'] == 'zipcode']

counts_condo_dict = {}

for a in df_condo_city['city_name'].unique():
    if a.lower() in a_condo_dict:
        counts_condo_dict[a] = a_condo_dict[a.lower()]
    else:
        counts_condo_dict[a] = 0

print counts_condo_dict

counts_condo_dict_zip = {}

for a in df_condo_zip['city_name'].unique():
    if a.lower() in a_zip_condo_dict:
        counts_condo_dict_zip[a] = a_zip_condo_dict[a.lower()]
    else:
        counts_condo_dict_zip[a] = 0

print counts_condo_dict_zip



df['active_listings'] = 0
df['active_listings'][(df['property_type'] == 'single family') & (df['classtype'] == 'city')] = df_single_family_city['city_name'].map(counts_singlefamily_dict)
df['active_listings'][(df['property_type'] == 'single family')& (df['classtype'] == 'zipcode')] = df_single_family_zip['city_name'].map(counts_singlefamily_zip_dict)
df['active_listings'][(df['property_type'] == 'condo') & (df['classtype'] == 'city')] = df_condo_city['city_name'].map(counts_condo_dict)
df['active_listings'][(df['property_type'] == 'condo') & (df['classtype'] == 'zipcode')] = df_condo_zip['city_name'].map(counts_condo_dict_zip)




df.to_csv('/Users/april.liu/Documents/Expansion New Market/pause automation/test version 2_7.13.16.csv', index=False)



