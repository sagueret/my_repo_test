# -*- coding: utf-8 -*-
"""
Created on Mon Mar 15 13:37:15 2021

@author: gueret
"""

'''
E-PRTR : filtering data of lower austria industry to vienna surroudings
'''
#%%
import pandas as pd

#%%
# Import E-PRTR file for Lower Austria from Excel 
my_file=pd.read_excel('Facilities_lower_austria.xlsx')
my_facility_list=my_file['Column9'].to_list() # Store facility localities in list

#NUTS3 Info file for Lower Austria
nuts3_full=pd.read_excel('NUTS3Raumgliederungen_Stand_2020_01.xlsx', sheet_name=0)
#NUTS3 Info file for Wien Umland
nuts3_surr=pd.read_excel('NUTS3Raumgliederungen_Stand_2020_01.xlsx', sheet_name=1)


my_locality_list=nuts3_surr['Unnamed: 1'].tolist()# List of localities (gemeinden) in Wien Umland
my_locality_list_extd=nuts3_full['Unnamed: 1'].tolist()#List of localities (gemeinden) in Lower Austria

# Filtered file from E-PRTR on the basis of localities from Wien Umland
my_filtered_df=my_file[my_file['Column9'].isin(my_locality_list)]#.reset_index(drop= True)

# Filtered file from E-PRTR on the basis of localities from Lower Austria
my_filtered_df2=my_file[my_file['Column9'].isin(my_locality_list_extd)]



#List of "ghost" localities present in E-PRTR but nevertheless filtered out on the basis
#of localities from Lower Austria
ghost_loc= [loc for loc in my_facility_list if loc not in my_filtered_df2['Column9'].tolist()]
ghost_loc=list(set(ghost_loc))
extd_loc=['Baumgarten an der March', 'Dürnrohr', 'Fischamend-Dorf', 'Kollersdorf'
          ,'Mannswörth', 'Pischelsdorf','Tulln an der Donau']


## Filtered file from E-PRTR on the basis of the ghost localities 
my_filtered_df3=my_file[my_file['Column9'].isin(ghost_loc)]
my_filtered_df4=my_file[my_file['Column9'].isin(extd_loc)]

#Export to CSV 
#my_filtered_df3['Column9'].sort_values().drop_duplicates().to_csv(r'H:\UNCNET\Data collection\E-PRTR\Ghost localities.txt',index=False)

#Export to Excel
#my_filtered_df.to_excel('Facilities_vienna_surrounding.xlsx',index=False)
my_filtered_df4.to_excel(r'H:\UNCNET\Data collection\E-PRTR\added_facilities.xlsx',index=False)
