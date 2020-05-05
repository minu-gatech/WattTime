#!/usr/bin/env python
# coding: utf-8

# # WattTime

# In[99]:


#import dependencies
import pandas as pd
import re


# ### ENTSO 

# In[100]:


# Reading csv file 'entso.csv'
df_entso = pd.read_csv('entso.csv',encoding="utf-8")
df_entso.head()


# In[102]:


# Cleaning the country column
df_entso['country'] = df_entso['country'].str.split(" ", n = 1, expand = True)[0]


# In[103]:


# Reading the data
df_entso.head()


# In[104]:


# Adding suffix to each column of dataframe
df_entso = df_entso.add_suffix('_entso')
df_entso_new = df_entso.copy()
df_entso_new.head()


# In[ ]:





# ### PLATTS

# In[108]:


# Reading csv file
df_platts = pd.read_csv('platts.csv',encoding="utf-8")
df_platts.head()


# In[110]:


# Renaming column names and converting them into lower case
df_platts.rename(columns = {'PLANT':'plant_name','UNIT':'unit_name','COMPANY':'company_name'},inplace=True) 
df_platts.columns= list(df_platts.columns.str.lower())
df_platts.columns


# In[111]:


# Adding suffix to each column of dataframe
df_platts = df_platts.add_suffix('_platts')
df_platts.head()


# In[113]:


#df_platts.columns
df_platts_new = df_platts[['plant_id_platts', 'unit_id_platts', 'plant_name_platts',
       'unit_name_platts', 'company_name_platts', 'unit_capacity_mw_platts',
       'unit_fuel_platts','country_platts' ]]


# In[115]:


df_platts_new.head()


# In[116]:


#df_platts_new['plant_id_platts'].value_counts()
#df_platts_new['plant_id_platts'].duplicated().any()


# In[117]:


# Checking datatype of column
df_platts_new['plant_id_platts'].dtype


# In[118]:


# Changing datatype of column from int to string
df_platts_new['plant_id_platts'] = df_platts_new['plant_id_platts'].astype('str')
df_platts_new['plant_id_platts'].dtype


# In[119]:





# ### GPPD

# In[125]:


# Reading csv file
df_gppd = pd.read_csv('gppd.csv',encoding="utf-8")
df_gppd.head(2)


# In[127]:


# Adding suffix to each column of dataframe
df_gppd = df_gppd.add_suffix('_gppd')
df_gppd.head(2)


# In[129]:


#df_gppd.columns


# In[130]:


# Selecting relevant columns from dataframe
df_gppd_new = df_gppd[['plant_id_gppd', 'plant_name_gppd','wepp_id_gppd', 'country_long_gppd', 
                       'plant_capacity_mw_gppd', 'plant_primary_fuel_gppd']]


# In[131]:


# Displaying dataframe
df_gppd_new.head()


# In[132]:


# Checking datatype of column
df_gppd_new['wepp_id_gppd'].dtype


# In[133]:


#df_gppd_new['wepp_id_gppd'].value_counts()>1
#df_gppd_new['wepp_id_gppd'].duplicated().any()


# In[261]:


# Filtering out rows having wepp_id as 'nan'
df_gppd_weppid_blank = df_gppd_new.loc[df_gppd_new['wepp_id_gppd']=='nan']
df_gppd_weppid_blank.head()


# In[142]:


# Converting datatype & Checking datatype of column
df_gppd_new['wepp_id_gppd'] = df_gppd_new['wepp_id_gppd'].astype('str')
df_gppd_new['wepp_id_gppd'].dtype


# In[143]:


# Filtering out the records with wepp_id column containing '|' symbol
df_gppd_new_mark = df_gppd_new.loc[df_gppd_new['wepp_id_gppd'].str.contains('\|',regex=True)]
df_gppd_new_mark.head()


# In[145]:


# Splitting the wepp_id column data into different columns
df_gppd_new_mark[['wepp_id_gppd_1','wepp_id_gppd_2','wepp_id_gppd_3']] = df_gppd_new_mark['wepp_id_gppd'].str.split("|",expand=True) 
df_gppd_new_mark.head()


# In[ ]:





# ### Merge - Platts & GPPD

# In[161]:


# Merging the dataframe where 'plant_id_platts' = 'wepp_id_gppd'
df_platts_gppd_combined = pd.merge(df_platts_new, df_gppd_new, left_on='plant_id_platts', right_on='wepp_id_gppd')
df_platts_gppd_combined.head()


# In[163]:


# Extracting relevant columns
df_platts_gppd_combined[['plant_id_platts','wepp_id_gppd','plant_name_platts','plant_name_gppd','unit_id_platts','plant_id_gppd']].head()


# In[164]:


# Merging the dataframe where 'plant_id_platts' = 'wepp_id_gppd_1'
df_platts_gppd_mark_1_combined = pd.merge(df_platts_new, df_gppd_new_mark, left_on='plant_id_platts', right_on='wepp_id_gppd_1')
df_platts_gppd_mark_1_combined.head(5)


# In[166]:


# Relevant Columns
df_platts_gppd_mark_1_combined[['plant_id_platts','wepp_id_gppd','wepp_id_gppd_1','plant_name_platts','plant_name_gppd','unit_id_platts','plant_id_gppd']].head(5)


# In[167]:


# Merging the dataframe where 'plant_id_platts' = 'wepp_id_gppd_2'
df_platts_gppd_mark_2_combined = pd.merge(df_platts_new, df_gppd_new_mark, left_on='plant_id_platts', right_on='wepp_id_gppd_2')
df_platts_gppd_mark_2_combined.head()


# In[169]:


# Relevant columns
df_platts_gppd_mark_2_combined[['plant_id_platts','wepp_id_gppd','wepp_id_gppd_2','plant_name_platts','plant_name_gppd','unit_id_platts','plant_id_gppd']].head()


# In[170]:


# Merging the dataframe where 'plant_id_platts' = 'wepp_id_gppd_3'
df_platts_gppd_mark_3_combined = pd.merge(df_platts_new, df_gppd_new_mark, left_on='plant_id_platts', right_on='wepp_id_gppd_3')
df_platts_gppd_mark_3_combined.head()


# In[172]:


# Relavant Columns
df_platts_gppd_mark_3_combined[['plant_id_platts','wepp_id_gppd','wepp_id_gppd_3','plant_name_platts','plant_name_gppd','unit_id_platts','plant_id_gppd']].head()


# In[ ]:





# In[ ]:


#### Merging the dataframes ###


# In[173]:


df1 = pd.DataFrame({'platts_unit_id':df_platts_gppd_combined['unit_id_platts'],
                  'gppd_plant_id':df_platts_gppd_combined['plant_id_gppd']})


# In[175]:


df2 = pd.DataFrame({'platts_unit_id':df_platts_gppd_mark_1_combined['unit_id_platts'],
                  'gppd_plant_id':df_platts_gppd_mark_1_combined['plant_id_gppd']})


# In[177]:


df3 = pd.DataFrame({'platts_unit_id':df_platts_gppd_mark_2_combined['unit_id_platts'],
                  'gppd_plant_id':df_platts_gppd_mark_2_combined['plant_id_gppd']})


# In[179]:


df4 = pd.DataFrame({'platts_unit_id':df_platts_gppd_mark_3_combined['unit_id_platts'],
                  'gppd_plant_id':df_platts_gppd_mark_3_combined['plant_id_gppd']})


# In[181]:


# Concatinate the four dataframes 
df = pd.concat([df1,df2,df3,df4])


# In[183]:


# Resetting the index
df.reset_index(inplace=True,drop = True)


# In[184]:


df.head(10)


# In[185]:


# Saving the file
# df.to_csv("platts_gppd.csv")


# In[ ]:





# ### Merging (Platts, GPPD) with Enfso

# In[ ]:





# In[186]:


# Concatening dataframes
df_final = pd.concat([df_platts_gppd_combined,df_platts_gppd_mark_1_combined,df_platts_gppd_mark_2_combined,df_platts_gppd_mark_3_combined],sort=False)
df_final.head()


# In[ ]:





# In[218]:


# Matching plant id's of enfso with (Platts & GPPD [wepp id != 'nan'])

# Emptry lists
list_entso = []
list_platts = []
list_gppd = []

# Iterating through dataframe
for index1, row1 in df_entso_new.iterrows():
    
    pn_entso = row1['plant_name_entso'].lower()
    
    # Iterating through dataframe
    for index2, row2 in df_final.iterrows():
        
        pn_gppd = row2['plant_name_gppd'].lower()
        pn_platts = row2['plant_name_platts'].lower()
        
            
        if (pn_gppd.find(pn_entso) == -1): 
            pass 
        else: 
            #print(pn_entso, ",",pn_gppd)
            #print(row1['unit_id_entso'], ",",row2['plant_id_gppd'],",",row2['unit_id_platts'])
            list_entso.append(row1['unit_id_entso'])
            list_platts.append(row2['unit_id_platts'])
            list_gppd.append(row2['plant_id_gppd'])
            

               


# In[219]:


# Creating a dictionary
data_dict = {'entso_unit_id':list_entso, 'platts_unit_id':list_platts, 'gppd_plant_id':list_gppd}


# In[319]:


# Creating a dataframe
mapped_data_1 = pd.DataFrame(data_dict)
mapped_data_1.head()


# In[320]:


# Saving mapped data into csv file
mapped_data_1.to_csv("mapping1.csv",index=False)


# In[ ]:





# In[ ]:





# In[ ]:


#### Matching 'nan' wepp id records of GPPD with platts data ####


# In[271]:


# Creating empty lists
list_platts_2 = []
list_gppd_2 = []
list_powerplant_name_platts = []
list_powerplant_name_gppd = []
list_country_gppd =[]
list_fuels_platts= []
list_fuels_gppd = []

# Iterating through rows
for index1, row1 in df_platts_new.iterrows():
    
    pn_platts = row1['plant_name_platts'].lower()
    country_platts = row1['country_platts'].lower()
    
    # Iterating through rows
    for index2, row2 in df_gppd_weppid_blank.iterrows():
        
        #pn_gppd = row2['plant_name_gppd'].lower()
        pn_gppd = row2['plant_name_gppd'].lower()
        country_gppd = row2['country_long_gppd'].lower()
        
        if(pn_platts == pn_gppd):
            if(country_platts == country_gppd):
                print("\n  ",pn_platts, ",",pn_gppd)
                print(row1['unit_id_platts'],",",row2['plant_id_gppd'],"-",row1['country_platts'],",",row2['country_long_gppd'])
                list_platts_2.append(row1['unit_id_platts'])
                list_gppd_2.append(row2['plant_id_gppd'])
                list_powerplant_name_platts.append(row1['plant_name_platts'])
                list_powerplant_name_gppd.append(row2['plant_name_gppd'])
                list_country_gppd.append(row2['country_long_gppd'])
                list_fuels_platts.append(row1['unit_fuel_platts'])
                list_fuels_gppd.append(row2['plant_primary_fuel_gppd'])
                
        else:
            if (pn_gppd.find(pn_platts) == -1): 
                pass
            else: 
                if(country_platts == country_gppd):
                    print("\n  ",pn_platts, ",",pn_gppd)
                    print(row1['unit_id_platts'],",",row2['plant_id_gppd'],"-",row1['country_platts'],",",row2['country_long_gppd'])
                    list_platts_2.append(row1['unit_id_platts'])
                    list_gppd_2.append(row2['plant_id_gppd'])
                    list_powerplant_name_platts.append(row1['plant_name_platts'])
                    list_powerplant_name_gppd.append(row2['plant_name_gppd'])
                    list_country_gppd.append(row2['country_long_gppd'])
                    list_fuels_platts.append(row1['unit_fuel_platts'])
                    list_fuels_gppd.append(row2['plant_primary_fuel_gppd'])
   

               


# In[281]:


list_fuels_gppd


# In[282]:


# Creating a dictionary
data_dict_2 = {'unit_id_platts':list_platts_2, 'plant_id_gppd':list_gppd_2, 'plant_name_platts':list_powerplant_name_platts, 
              'plant_name_gppd':list_powerplant_name_gppd,'country_long_gppd':list_country_gppd,'unit_fuel_platts':list_fuels_platts,
              'plant_primary_fuel_gppd':list_fuels_gppd}


# In[290]:


# Creating a dataframe
mapped_data2 = pd.DataFrame(data_dict_2)
mapped_data2.head(10)


# In[285]:


# Saving mapped data into csv file
#mapped_data2.to_csv("mapping_extra_long.csv",index=False)


# In[ ]:





# In[ ]:





# In[323]:


# Matching plant id's of enfso with (Platts & GPPD [wepp id == 'nan'])

# Emptry lists
list_entso_nan = []
list_platts_nan = []
list_gppd_nan = []

# Iterating through dataframe
for index1, row1 in df_entso_new.iterrows():
    
    pn_entso = row1['plant_name_entso'].lower()
   
    # Iterating through dataframe
    for index2, row2 in mapped_data2.iterrows():
        
        pn_gppd = row2['plant_name_gppd'].lower()
        pn_platts = row2['plant_name_platts'].lower()
            
        if (pn_gppd.find(pn_entso) == -1): 
            pass 
        else: 
            #print(pn_entso, ",",pn_gppd)
            #print(row1['unit_id_entso'], ",",row2['plant_id_gppd'],",",row2['unit_id_platts'])
            list_entso_nan.append(row1['unit_id_entso'])
            list_platts_nan.append(row2['unit_id_platts'])
            list_gppd_nan.append(row2['plant_id_gppd'])
            

        if (pn_entso.find(pn_gppd) == -1): 
            pass 
        else: 
            #print(pn_entso, ",",pn_gppd)
            #print(row1['unit_id_entso'], ",",row2['plant_id_gppd'],",",row2['unit_id_platts'])
            list_entso_nan.append(row1['unit_id_entso'])
            list_platts_nan.append(row2['unit_id_platts'])
            list_gppd_nan.append(row2['plant_id_gppd'])
            
                 


# In[ ]:





# In[315]:


# Creating a dictionary
data_dict_nan = {'entso_unit_id':list_entso_nan, 'platts_unit_id':list_platts_nan, 'gppd_plant_id':list_gppd_nan}


# In[316]:


# Creating a dataframe
mapped_data_nan = pd.DataFrame(data_dict_nan)
mapped_data_nan.head()


# In[317]:


# Saving mapped data into csv file
mapped_data_nan.to_csv("mapping2.csv",index=False)


# In[ ]:





# In[321]:


# Concatening final dataframes
df_final_mapped = pd.concat([mapped_data_1,mapped_data_nan],sort=False)
df_final_mapped.head()


# In[322]:


# Saving final mapped data into csv file
df_final_mapped.to_csv("mapping.csv",index=False)


# In[ ]:





# In[ ]:


## END ##


# In[ ]:




