# -*- coding: utf-8 -*-
"""
Author : Guéret Samuel

Data loading routine from database
"""
#%%
import sys
sys.path.insert(0,'H:\\Fabian_work\\GitReposFabian\\GAINS Python Toolboxes\\')
import numpy as np
from pyGAINS_structure import *
from pyGAINS_direct import *
from pyGAINS_quality_control import *

#matplotlib inline
import matplotlib
#%%
#Configuration of output directories
from os import *
my_path = os.getcwd()#outdir_root = 'P:\\tap.model\\2019_WB_NCI\\v01\\DataChecks\\'
outdir_root = my_path + '\\completeness_checks\\'
outdir_AD = outdir_root + 'AD\\'
outdir_CS = outdir_root + 'CS\\'
outdir_em = outdir_root + 'emissions\\'
outdir_cost = outdir_root + 'costs\\'
create_subdirectory(outdir_AD)
create_subdirectory(outdir_CS)
create_subdirectory(outdir_em)
create_subdirectory(outdir_cost)

# Set parameters
my_DB = 'GAINS_DW4'
scen_base = 'PRIMES_2020_REF_v3c' # From EUCLIMIT 2020 GROUP
#scen_base = 'WEO2018_NPS_CLE_CH4c' #'ECLIPSE_V6b_CLE_base' --> not yet implemented ? YES but need to use the Scen ID and not Label !
#scen_base = 'ASEAN_baseline_test'
#scen_base = 'WEO2019_CAS_SDS_CLE_SA'


my_years = [2015]
first_year = 2010
last_year = 2050
#my_cost_set = 'EURO_2015'
my_cost_set = 'Local_2007'
polls = ['SO2','NOX','NH3','VOC','N2O'] # PM will be added later anyway
my_PM_fracs = ['PM_2_5', 'PM_BC', 'PM_OC']

#my_partition = 'South Asia Assessment 3'
my_partition = 'EU28 Work 1'
#print (partition_map(my_partition).head(28))
      
# Automatically defined from the choice of pollutants
my_em_polls = polls + my_PM_fracs
my_cost_polls = polls + ['PM']

# Automatically defined from the partition
my_region_groups = groups_from_partition(my_partition)
my_regions = regions_from_partition(my_partition)
aust_region = [my_regions[my_regions.index("AUST_WHOL")]]

# Load from Database
base_AD = AD_load_by_regions_schema(scen_base, my_regions, my_DB)
base_ASF = ASF_FACTOR_load_by_regions_schema(scen_base, my_regions, my_DB)
base_CS = CS_load_by_regions_schema(scen_base, my_regions, my_DB)
df_EFs = EF_load_by_regions_schema(scen_base, my_regions, my_DB)
       
base_UCOST = UNIT_COST_load_schema(scen_base, my_regions, my_cost_set, my_years, my_DB)

# This cell also defines the "expanded activity data" using the act-sec-factors
my_AD = AD_load_by_regions_schema(scen_base,aust_region,my_DB)
my_ASF = ASF_FACTOR_load_by_regions_schema(scen_base, aust_region, my_DB)
my_CS = CS_load_by_regions_schema(scen_base, aust_region, my_DB)
my_df_EFs = EF_load_by_regions_schema(scen_base, aust_region, my_DB)


ext_AD= AD_load_by_regions_ext(scen_base, aust_region) # With labels
my_AD_extd = expand_AD(my_AD, my_ASF)



#%%
'''
#################################
########UNCNET PART##############
#################################
'''
n_poll=['NH3','NOX','N2O']
q=ext_AD.copy()
#q2 = q[(q['YEAR'] == 2015) | (q['YEAR'] == 2020)]# for several years filtering
q = q[q['YEAR'] == 2015]
q = q.drop(['SCENARIO','PATH_ABB'],1)
my_filtered_poll_AD=filter_poll(q,n_poll)
my_filtered_poll_AD=add_sa_poll(my_filtered_poll_AD,n_poll)
my_filtered_poll_AD=add_unit(my_filtered_poll_AD)
my_filtered_poll_AD.to_excel(r'C:\Users\gueret\Jupyterlab\My .py scripts\N_related_AD.xlsx')

my_filtered_poll_AD_2=filter_poll_2(q,n_poll) ## Without sec-act combinations related to no pollutants at all
my_filtered_poll_AD_2=add_sa_poll(my_filtered_poll_AD_2,n_poll)
my_filtered_poll_AD_2=add_unit(my_filtered_poll_AD_2)
my_filtered_poll_AD_2=my_filtered_poll_AD_2.drop(['REGION'],1)

my_filtered_no_poll_AD=filter_no_poll(q)
#q2 = q.pivot_table(index = ['SECTOR','ACTIVITY'], columns = 'REGION', values = 'VALUE',aggfunc='first').reset_index()
q_agr = q[q['SECTOR'].isin(agr_sec_aust)] # only for Austrian relevant agricultural sectors
#q_agr_ext = q[q['SECTOR'].isin(agr_sec)] # for all agri sectors
q_waste = q[q['SECTOR'].isin(all_waste)]
q_procs = q[q['SECTOR'].isin(all_procs)]



df2=db_item_act_sec_tech_to_poll.copy()

df_nh3_agr2=df2[(df2['SECTOR'].isin(agr_sec_aust))&(df2['POLLUTANT']=='NH3')]
df_nh3_agr2_1=df_nh3_agr2.drop_duplicates(subset='SECTOR',keep='first',inplace=False)# drop duplicates 

df=db_item_act_sec_to_poll.copy()
df_nh3=df[df['POLLUTANT']=='NH3'] # List of items related to NH3 pollutant
df_nh3_agr=df_nh3[df_nh3['SECTOR'].isin(agr_sec_aust)] # List of items related to NH3 pollutant and livestock-related sectors
df_nh3_waste=df_nh3[df_nh3['SECTOR'].isin(all_waste)]
df_nh3_procs=df_nh3[df_nh3['SECTOR'].isin(all_procs)]

df_n2o=df[df['POLLUTANT']=='N2O'] # List of items related to N2O pollutant
df_n2o_agr=df_n2o[df_n2o['SECTOR'].isin(agr_sec_aust)]
df_n2o_waste=df_n2o[df_n2o['SECTOR'].isin(all_waste)]
df_n2o_procs=df_n2o[df_n2o['SECTOR'].isin(all_procs)]

df_nox=df[df['POLLUTANT']=='NOX']
df_nox_agr=df_nox[df_nox['SECTOR'].isin(agr_sec_aust)]
df_nox_waste=df_nox[df_nox['SECTOR'].isin(all_waste)]
df_nox_procs=df_nox[df_nox['SECTOR'].isin(all_procs)]

df_n=df[(df['POLLUTANT']=='NH3')|(df['POLLUTANT']=='NOX')|(df['POLLUTANT']=='N2O')]
df_n_agr=df_n[df_n['SECTOR'].isin(agr_sec_aust)]
df_n_procs=df_n[df_n['SECTOR'].isin(all_procs)]




### To possibly systematize defining a function. 
## GOAL : Filtering a given dataframe according to a given set of pollutants of interest

q_procs_N=q_procs.drop(['REGION','YEAR'],1).reset_index()
q_procs_N=q_procs_N.drop(['index'],1)
for r in  range (len(q_procs['SECTOR'])) :
    poll = s_pollutants(q_procs['SECTOR'].iloc[r]) # Access the r'th cell
    if poll : # Check if list is not empty
        if  list_diff(n_poll,poll) == n_poll : # If pollutants of a given sector are neither NH3,NOX nor N2O
            q_procs_N=q_procs_N.drop([r],0)


'''
add_sa_poll function created for the below purpose
'''
nh3=[]
n2o=[]
nox=[]
unit=[]
q_procs_N=q_procs_N.reset_index()
q_procs_N=q_procs_N.drop(['index'],1)
for r in range (len(q_procs_N['SECTOR'])) :
    poll = s_pollutants(q_procs_N['SECTOR'].iloc[r])
    my_activity = q_procs_N['ACTIVITY'].iloc[r]
    my_sector = q_procs_N['SECTOR'].iloc[r]
    unit.append(sa_unit(my_sector,my_activity))
    if 'NH3' in poll : 
        nh3.append(1)
    else :
        nh3.append(0)
    if 'N2O' in poll : 
        n2o.append(1)
    else :
        n2o.append(0)
    if 'NOX' in poll : 
        nox.append(1)
    else :
        nox.append(0)
q_procs_N['UNIT']=unit           
q_procs_N['NH3']=nh3   
q_procs_N['N2O']=n2o
q_procs_N['NOX']=nox
#q_procs_N.to_excel(r'C:\Users\gueret\Jupyterlab\My .py scripts\N_related_processes_AD.xlsx')

''' NB : inconsistency noticed between db_ACT_SEC_ALL units and those retrieved from downloaded AD templates from
### the web interface for the ACTIVITY-SECTOR Combination : NOF - IND_FOOD_COD and NOF-IND_PAP_COD 
# --> result from downloaded templates taken
'''
            
#q_procs_nox = q_procs[for r in q_procs['SECTOR'].map(s_pollutants)] # .map access each entry of given 
#column with a function as arg                    
# ~ sign for contrary condition in dataframe filtering 
#for index,row in matrix_nv_km.iterrows() : # Other way to access each entry of a given column
 #   print(row["FUEL"])

# # for block commenting : select the section and type CTRL +1
'''
For string documentation
'''
#%%
### For activity-sector factors such as milk cow yield or waste shares 

r=my_ASF.copy()
r = r[r['YEAR'] == 2015]
r = r.drop(['SCENARIO','PATH_ABB'],1)
r_agr = r[r['SECTOR'].isin(agr_sec)]
r_waste = r[r['SECTOR'].isin(all_waste)]
r_procs= r[r['SECTOR'].isin(all_procs)]
my_filtered_poll_ASF_2=filter_poll_2(r,n_poll) ## Without sec-act combinations related to no pollutants at all
my_filtered_poll_ASF_2=add_sa_poll(my_filtered_poll_ASF_2,n_poll)
my_filtered_poll_ASF_2=add_unit(my_filtered_poll_ASF_2)
my_filtered_poll_ASF_2=my_filtered_poll_ASF_2.drop(['REGION','COMMENTS'],1)
my_filtered_no_poll_ASF=filter_no_poll(r)

''' NB: missing ASF for SECTORS IND_FOOD_NOC and IND_PAP_NOC
Duplicated values for all MSW shares related to Activity NOF (useless value as urban/rural 
split present)
'''
#%%
''' Download data from database using SQL statements
''' 
#%% 
''' CONTROL STRATEGIES 
''' 

my_years = [2015]
n_poll=['NH3','NOX','N2O']
my_CS = CS_load_by_regions_schema(scen_base, aust_region, my_DB)
my_CS = my_CS[my_CS['YEAR'] == 2015]
#my_CS = my_CS.drop(['SCENARIO','CON_STRAT'],1)
my_CS_agr = my_CS[my_CS['SECTOR'].isin(agr_sec)]

em_base_slim = slim_emission_calc(emission_calc(base_AD, my_CS_agr, df_EFs, base_ASF, aust_region, n_poll, my_years))
#em_base_slim=em_base_slim