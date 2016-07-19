# -*- coding: utf-8 -*-
"""
Created on Tue Jun 28 16:03:39 2016

@author: april.liu
"""

import pandas as pd
import psycopg2
from sqlalchemy import create_engine




reference_table = pd.read_csv('/Users/april.liu/Documents/address campaigns/Core and Mid/Campaignname & Market Reference table.csv')
reference_table.columns = ['Campaign','market','vendor_campaign_id']


# Create Connection Engines to Stingray and Redshift 
engine_RS = create_engine('postgresql://USERNAME:PASSWORD@10.0.7.23:5439/rmus_prod') 

# Create Cursor and Connection to Redshift for Update
REDSHIFT_CONNECTION_STRING = 'dbname=rmus_prod user=USERNAME password=PASSWORD host=10.0.7.23 port=5439 sslmode=require'
redshift_conn = psycopg2.connect(REDSHIFT_CONNECTION_STRING)
redshift_conn.set_session(autocommit=True)
redshift_cursor = redshift_conn.cursor()
print 'Redshift Connection Success!'


# insert new active listing_ids into table
Insert_table_Query = ''' insert into rf_temp.sem_address_listings_new (
select l.listing_id, l.property_id, l.search_status,l.mls_status, l.property_type, l.list_price_amount, l.street_address, l.street_number, l.street_name, l.listing_date, 'active' as ad_group_status,
getdate() as insert_date, getdate() as update_date, l.business_market_id, b.business_market_name, l.city, l.state_code
from edw.listing_dim l left join edw.business_market_dim b on l.business_market_id = b.business_market_id
where l.search_status = 'Active' and b.is_active = 'Y'  and l.street_name is not null and l.street_number is not null
and l.property_type = 'Single Family Residential' and l.bank_owned_flag = 'N' and l.new_construction_flag = 'N' and l.redfin_listing_flag = 'Y' and b.market_tier in ('Mid', 'Core')and listing_id not in (select listing_id from rf_temp.sem_address_listings_new))
 '''
redshift_cursor.execute(Insert_table_Query)
redshift_conn.commit() 
 # update active 
 
Update_table_Query = '''update rf_temp.sem_address_listings_new
Set ad_group_status = 
(CASE when 
a.search_status = 'Active' and a.listing_id in (select listing_id from edw.listing_dim where search_status != 'Active') then 'paused' 
when a.search_status != 'Active' and a.listing_id in (select listing_id from edw.listing_dim where search_status = 'Active') then 'active' else a.ad_group_status end) , update_date = getdate(),search_status = b.search_status, mls_status = b.mls_status, add_or_not = (CASE when a.search_status <> 'Active'  and b.search_status <> 'Active' then 'N' end)
from rf_temp.sem_address_listings_new a inner join edw.listing_dim b on a.listing_id = b.listing_id
where a.search_status != b.search_status '''
redshift_cursor.execute(Update_table_Query)
redshift_conn.commit() 

Adgroups_Query = '''select distinct property_id as "Ad Group",
first_value(ad_group_status)
over (partition by property_id order by listing_date desc
rows between unbounded preceding and unbounded following) as "State", '0.01' as "Default max. CPC", business_market_name as "Market"
from rf_temp.sem_address_listings_new
where trunc(update_date) = current_date and add_or_not is null
''' 

Keywords_Query = '''select distinct property_id as "Ad Group", 
first_value(ad_group_status)
over (partition by property_id order by listing_date desc
rows between unbounded preceding and unbounded following) as "Ad Group Status", ' '||'+'||street_number ||' '||'+'||replace(street_name,' ',' +') as Keyword, 'broad' as "Match Type", '2' as "Max CPC", business_market_name as "Market"
from rf_temp.sem_address_listings_new
where trunc(update_date) = current_date and ad_group_status = 'active'
''' 

AdCopy_Query = '''select distinct property_id as "Ad Group", 
first_value(ad_group_status)
over (partition by property_id order by listing_date desc
rows between unbounded preceding and unbounded following) as "Ad Group Status", initcap(street_address) as "Ad", 
'View This Area Property.' as "Description Line 1", 'View Photos, Up-to-date Data & More' as "Description Line 2",
'Redfin.com' as "Display URL", 'https://www.redfin.com/'||state_code||'/'||replace(replace(city, ' ','-'),'.','')||'/home/'||property_id as "Final URL",
'Text Ad' as "Ad Type", business_market_name as "Market"
from rf_temp.sem_address_listings_new
where trunc(update_date) = current_date and ad_group_status = 'active'
 '''



adgroups_dataframe = pd.read_sql(Adgroups_Query, con = engine_RS)
Keywords_dataframe = pd.read_sql(Keywords_Query, con = engine_RS)
Adcopy_dataframe = pd.read_sql(AdCopy_Query, con = engine_RS)




adgroups_dataframe_new = pd.merge(adgroups_dataframe, reference_table, on='market')
keywords_dataframe_new = pd.merge(Keywords_dataframe, reference_table, on='market')
adcopy_dataframe_new = pd.merge(Adcopy_dataframe, reference_table, on='market')




adgroups_dataframe_final = adgroups_dataframe_new.drop(['market', 'vendor_campaign_id'], axis=1)
keywords_dataframe_final = keywords_dataframe_new.drop(['market','vendor_campaign_id'], axis=1)
adcopy_dataframe_final = adcopy_dataframe_new.drop(['market','vendor_campaign_id'],axis=1)

'''adgroups_dataframe_final = adgroups_dataframe_final.drop_duplicates()'''


adgroups_dataframe_final.to_csv('/Users/april.liu/Google Drive/BulkUploadAddressAdgroups.csv', index=False)
keywords_dataframe_final.to_csv('/Users/april.liu/Google Drive/BulkUploadAddressKeywords.csv', index=False)
adcopy_dataframe_final.to_csv('/Users/april.liu/Google Drive/BulkUploadAddressAds.csv', index=False)
