#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import gender_guesser.detector as gender
from ethnicolr import census_ln, pred_census_ln


# # Clean Data

# In[16]:


# import data
data = pd.read_csv("data.csv", encoding='cp1252')
levels = pd.read_csv("levels.csv")


# In[ ]:


# only keep voters who voted in person
data = data[data['voted_inperson'] != 0]


# In[5]:


# label data based off whether individual voted in the primary ina particular year
data.sort_values(by = ['state_id', 'election_yr'], inplace = True)
data.loc[:,'election_yr'] = [year+.1 if primary == 1 else year for year, primary in 
                             zip(data['election_yr'], data['voted_primary'])]


# In[6]:


# clean zip codes
data.loc[:, 'zip'] = data['zip'].apply(lambda x: x.replace("-","").replace(".","").replace("1ST H", "").replace("S ON RT", "").replace("OK", "") if type(x) == str else x)
data.loc[:, 'zip'] = data['zip'].apply(lambda x: 0 if x == "" else x)
data.loc[:, 'zip'] = data['zip'].apply(lambda x: int(x[:5]) if type(x) == str else int(x) if not pd.isnull(x) else np.nan)
data.loc[:, 'zip'] = data['zip'].apply(lambda x: np.nan if x < 1000 else x)


# In[7]:


data = data[data['zip'].apply(lambda x: x >= 43000 and x <= 45899 or 
                              x >= 6000 and x <= 6999 or 
                              x >= 73000 and x <= 74999)]


# In[8]:


# For all never-voters, find first eligible election
data_novote = data[data['voted'] == 0]
data_novote.loc[:,'first_elg'] = data_novote['birth_yr'].apply(lambda x: x + 18 if x%2 == 0 else x+19)


# In[9]:


data_voted = data[data['voted'] == 1]


# In[10]:


# Limit to maximum of 11 possible elections
data_voted = data_voted[data_voted['nth_election'] <= 11]
data_voted['first_elg'] = data_voted['first_election_yr']

data_voted.reset_index(drop = True, inplace = True)

# fix zip code issue
data_voted[data_voted['state_id'] == '496519']['zip'] = 45013


# In[11]:


data_voted = data_voted[~data_voted.duplicated(['state_id', 'election_yr'])]


# In[12]:


# find people who first voted when they were old, remove first time voters who are over 30
# plausibly not first-time voters because of voter registration updates
data_oldvote = data_voted[data_voted['age_at_first_election'].apply(lambda x: x >= 20 and x <= 30)]
data_oldvote.loc[:,'first_elg'] = data_oldvote['birth_yr'].apply(lambda x: x + 18 if x%2 == 0 else x+19)
# Define election count variables
data_oldvote.loc[:,'nth_election'] = 1 + (data_oldvote['election_yr'] - data_oldvote['first_elg'])/2
data_oldvote.loc[:,'nth_election'] = data_oldvote['nth_election'].apply(lambda x: int(x))


# In[13]:


# combine data on people who voted at least once
data_newvote = data_voted[data_voted['age_at_first_election'].apply(lambda x: x <= 20 or pd.isnull(x))]
data_voted_fin = pd.concat([data_oldvote, data_newvote])


# In[14]:


# turn data from long to wide
selcols = [cols for cols in data_voted_fin.columns if cols not in 
           ['nth_election', 'election_yr', 'presidential_election', 'voted_primary', 'filter_col']]
data_wide_voted = data_voted_fin.pivot(index=selcols, columns='nth_election',values='election_yr').reset_index()


# In[15]:


# process data on people who never voted
data_novote.drop(['election_yr', 'nth_election', 'voted_primary', 'presidential_election'], 
                 axis = 1, inplace = True)


# In[16]:


# concatenate tables, fill in missing columns and drop duplicates
data_wide = pd.concat([data_wide_voted, data_novote])
data_wide.drop_duplicates(inplace = True)


# In[17]:


# drop everything after 10th election
data_wide.drop(np.arange(11, 18, 1), axis = 1, inplace = True)


# In[18]:


# rename column names, drop voted column
data_wide = data_wide.set_index('state_id')
name_dict = dict(zip(pd.Series(range(10))+1, ['election ' + str(x) for x in pd.Series(range(10))+1]))
data_wide.rename(name_dict, axis = 1, inplace = True)
data_wide.columns.name = ''

data_wide.drop('voted', axis = 1, inplace = True)


# In[19]:


data_wide = data_wide[data_wide['election 1'].apply(lambda x: x < 2020 or pd.isnull(x))]
data_wide['election_cnt'] = 10 - data_wide[name_dict.values()].isna().sum(axis = 1)


# In[20]:


# add column for whether someone voted in primary: 1 if yes, 0 if no
for i in range(10):
    election_n = 'election ' + str(i+1)
    colname = 'vp ' + election_n
    data_wide.loc[:, colname] = data_wide[election_n].apply(lambda x: 
                                                            0 if round(x, 0) == round(x, 0) 
                                                            else (round(x, 1) - round(x, 0)) * 10 
                                                            if not pd.isnull(x) else np.nan)


# In[21]:


# make election year columns integers
for i in range(10):
    election_n = 'election ' + str(i+1)
    data_wide.loc[:, election_n] = data_wide[election_n].apply(lambda x: int(round(x)) if not pd.isnull(x) else np.nan)


# In[22]:


# adding columns for whether individuals voted in i-th election
# separating election year and voted column
for i in range(10):
    elect_year = 'election year ' + str(i+1)
    voted_election = 'election ' + str(i+1)
    data_wide.loc[:,elect_year] = data_wide['first_elg'].apply(lambda x: x+2*i if x+2*i < 2020 else np.nan) 
    data_wide.loc[:,voted_election] = 1 - data_wide[voted_election].isna()
    data_wide.loc[:,voted_election] = [np.nan if pd.isnull(year) else res 
                                       for year, res in zip(data_wide[elect_year],
                                                            data_wide[voted_election])]


# In[23]:


# first time someone was eligible to vote
data_wide['age_at_first_eligible'] = data_wide['election year 1'] - data_wide['birth_yr']


# In[24]:


data_cleaned = data_wide.convert_dtypes()


# In[25]:


data_cleaned = data_cleaned[data_cleaned['first_elg'] < 2020]
data_cleaned = data_cleaned[data_cleaned['age_at_first_eligible'] < 20]
data_cleaned = data_cleaned.reset_index()
data_cleaned = data_cleaned[~data_cleaned.duplicated('state_id')]


# # Guess Gender and Ethnicity

# In[26]:


d = gender.Detector()


# In[27]:


data_cleaned['gender'] = data_cleaned['first_name'].apply(lambda x: d.get_gender(u"{}".format(x.capitalize()), u'usa'))


# # Merge with Weather

# In[28]:


# Create dataframe with weather from all years
weather = pd.DataFrame()

for year in np.arange(1994, 2020, 2):
    wtemp = pd.read_csv('Data/Weather/' + str(year) + '.csv')
    wtemp['year'] = year
    weather = pd.concat([weather, wtemp])


# In[29]:


# calculate inverse distance weights 
for i in range(3):
    colname = 'station' + str(i+1) + '_wt'
    coldist = 'station' + str(i+1) + '_dist'
    weather[colname] = (1/weather[coldist])/(1/weather['station1_dist'] + 1/weather['station2_dist'] + 1/weather['station3_dist'])


# In[30]:


# find mean for each type of weather variable
for i in ['_prcp', '_snow', '_snwd', '_tmax', '_tmin']:
    colname = 'mean'+i
    stat1 = 'station1'+i
    stat2 = 'station2'+i
    stat3 = 'station3'+i
    weather[colname] = weather[stat1]*weather['station1_wt'] + weather[stat2]*weather['station2_wt']  + weather[stat3]*weather['station3_wt'] 


# In[31]:


# subset final weather data
weather_final = weather[['zipcode', 'year','mean_prcp','mean_snow','mean_snwd','mean_tmax','mean_tmin']]
weather_final = weather_final.convert_dtypes()


# In[32]:


# merge weather data with each election
for i in range(5):
    weather_temp = weather_final.copy()
    cols = 'election ' + str(i+1) + ' ' + weather_temp.columns[2:] 
    renamecols = dict(zip(weather_temp.columns[2:], cols))
    weather_temp.rename(renamecols, axis = 1, inplace = True)
    data_cleaned = pd.merge(data_cleaned, weather_temp, how = 'left', 
                            left_on = ['zip', 'election year ' + str(i+1)], right_on = ['zipcode', 'year'])


# In[33]:


# merge weather data with each election 
for i in range(5):     
    weather_temp = weather_final.copy()     
    cols = 'election ' + str(i+6) + ' ' + weather_temp.columns[2:]      
    renamecols = dict(zip(weather_temp.columns[2:], cols))     
    weather_temp.rename(renamecols, axis = 1, inplace = True)     
    data_cleaned = pd.merge(data_cleaned, weather_temp, how = 'left',                              
                            left_on = ['zip', 'election year ' + str(i+6)], right_on = ['zipcode', 'year'])


# In[34]:


data_cleaned.drop(['zipcode_y','zipcode_x', 'year_x', 'year_y'], axis = 1, inplace = True)


# In[35]:


newnames = dict(zip(data_cleaned.columns, [x.replace(" ", "_") for x in data_cleaned.columns]))
data_cleaned.rename(newnames, axis = 1, inplace = True)


# ## Merge with Housing Prices

# In[4]:


# import data
house = pd.read_csv("Data/Housing/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv")


# In[5]:


# select desired columns
cols = ['RegionID', 'SizeRank']
dates = [str(i) + "-11-30" for i in np.arange(2000, 2019, 2)]
cols.extend(dates)
houses = house[cols]


# In[6]:


house_cols = np.arange(2000, 2019, 2)
rename_dict = dict(zip(dates, house_cols))
houses.rename(rename_dict, axis = 1, inplace = True)


# In[10]:


houses_long = pd.melt(houses, id_vars=['RegionID', 'SizeRank'], value_vars = np.arange(2000, 2019, 2),
                      var_name = 'Year', value_name = 'Zillow Value')


# In[11]:


houses_long = houses_long.convert_dtypes()


# In[13]:


for i in range(10):
    # merge data
    houses_long_copy = houses_long.copy()
    houses_long_copy.rename(dict(zip(houses_long.columns[1:],houses_long.columns[1:]+"_"+str(i+1))),
                            axis = 1, inplace = True)
    data_cleaned = pd.merge(data_cleaned, houses_long_copy, how = 'left', 
                            left_on = ['zip', 'election_year_' + str(i+1)], 
                            right_on = ['RegionID', 'Year_' + str(i+1)])


# In[30]:


dropcols = ['RegionID_x', 'RegionID_y']
dropcols.extend(['Year_' + str(x+1) for x in np.arange(10)])


# In[33]:


data_cleaned.drop(dropcols, axis = 1, inplace = True)


# ## Data on Total Elgible Voters in Zip-Year

# In[2]:


#data_cleaned = data_cleaned.sample(100000)
data_cleaned = pd.read_csv("temp.csv", index_col = 0)
data_cleaned = data_cleaned.convert_dtypes()


# In[3]:


grouped = data_cleaned[['zip', 'first_elg']].value_counts().reset_index().sort_values(['zip', 'first_elg'])
grouped.rename({0:'count'}, axis = 1, inplace = True)


# In[4]:


starter_df = grouped.drop_duplicates('zip')
starter_dict = dict(zip(starter_df['zip'], starter_df['first_elg']))


# In[5]:


elg_dict = grouped.groupby('zip')[['first_elg',
                                   'count']].apply(lambda x: 
                                                   x.set_index('first_elg')).to_dict()
elg_dict = elg_dict['count']


# In[6]:


for zipcode in starter_df['zip']:
    fyear = starter_dict[zipcode]
    for year in np.arange(fyear+2, 2020, 2):
        pair = (zipcode, year)
        oldpair = (zipcode, year-2)
        if pair in elg_dict.keys():
            elg_dict[pair] = elg_dict[pair] + elg_dict[oldpair]
        else:
            elg_dict[pair] = elg_dict[oldpair]
        


# In[7]:


total_elg = pd.concat([pd.DataFrame(elg_dict.keys()),
                       pd.DataFrame(elg_dict.values())], axis = 1)
total_elg.columns = ['zip', 'year', 'count']
total_elg.sort_values(['zip', 'year'], inplace = True)


# ## Merge with Polling Density

# In[8]:


# import data
state_polling = pd.DataFrame()
for state in ['CT', 'OH', 'OK']:
    i = 1
    for year in np.arange(2012, 2020, 2):
        name = state + "_" + str(year) + ".csv"
        temp_df = pd.read_csv('Data/Polling/' + name)
        temp_df['election_year'] = year
        temp_df['zip_code'] = temp_df['address'].apply(lambda x: x[-5:] if not pd.isnull(x) else x)
        state_polling = pd.concat([state_polling, temp_df])
    i += 1
state_polling['zip_code'] = pd.to_numeric(state_polling['zip_code'], errors = "coerce")
state_polling['zip_code'] = state_polling['zip_code'].apply(lambda x: -x if x<0 else x)


# In[9]:


polling_density = state_polling.groupby(['zip_code', 'election_year'])['name'].count().reset_index()
polling_density = polling_density.convert_dtypes()


# In[10]:


polling_density_merged = pd.merge(polling_density, total_elg, left_on = ['zip_code', 'election_year'],
                                  right_on = ['zip', 'year']).drop(['zip', 'year'], axis = 1)


# In[11]:


polling_density_merged['density_per_1000'] = 1000 * polling_density_merged['name']/polling_density_merged['count']


# In[12]:


# merge density data with each election
for i in range(10):
    polling_temp = polling_density_merged.drop(['name', 'count'], axis = 1).copy()
    cols = 'election_' + str(i+1) + '_' + 'polling_density'
    renamecols = dict(zip(['density_per_1000'], [cols]))
    polling_temp.rename(renamecols, axis = 1, inplace = True)
    data_cleaned = pd.merge(data_cleaned, polling_temp, how = 'left', 
                            left_on = ['zip', 'election_year_' + str(i+1)], 
                            right_on = ['zip_code', 'election_year'])
data_cleaned.drop(['zip_code_x', 'election_year_x', 'zip_code_y', 'election_year_y'], axis = 1,
                  inplace = True)


# In[59]:


data_cleaned.to_csv('cleaned_data.csv')


# ## Adding Race

# In[ ]:


data_cleaned = census_ln(df, 'last_name')
selcols = ['pctwhite','pctblack','pctapi','pctaian','pct2prace','pcthispanic']
data_cleaned[selcols] = data_cleaned[selcols].replace("(S)", 0)
data_cleaned.drop('race', axis = 1, inplace = True)
for col in selcols:
    data_cleaned[col] = pd.to_numeric(df[col])
rowsums = data_cleaned[selcols].sum(axis = 1)/100
for col in selcols:
    data_cleaned[col] = data_cleaned[col]/rowsums


# In[60]:


data_cleaned.sample(100000).to_csv('sample.csv')

