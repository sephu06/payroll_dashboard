#%%
###############
#########1)Qpay reading startofMonth instead of pay_period
#######2)Terrier payperiod is one month prior to start of month
######3)No Process_type considered for  Addition
######4)ISFMP and previousmonth not present and current month present considered for Addition for Qpay
########ISFMP  and only 1 count for each employee code is considered for Terrier
#######rejoinee cases were found in Terrier which affects newJoinee count sometimes
#######################################################################
##########connect to QMR_DASHBOARD####
###48558
#%%

#from config.headcount import DB_CONFIG
from datetime import datetime
import mysql.connector
import pandas as pd
import warnings
import datetime
import numpy as np
import config_headcount
from config_headcount import *
import schedule
import time
from datetime import datetime
warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', None)
#%%
#from sqlalchemy import create_engine
#pd.set_option('display.max_columns', None)
from sqlalchemy import create_engine
conn = mysql.connector.connect(user=config_headcount.User, database=config_headcount.Database,
                               password=config_headcount.Password,
                               host=config_headcount.Host,
                               port=config_headcount.Port)
cursor = conn.cursor()
query = "select * from Payroll_Datamart.QMR_Dashboard where StartOfMonth >= '2025-01-01'"
df_payroll= pd.read_sql(query, con=conn)
df_payroll.columns
#%%
##identifying previous month and previous to previous month using python
from datetime import datetime, timedelta
current_date = datetime.now()

def get_previous_month(date):
    first_day_of_current_month = date.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    return last_day_of_previous_month

# Function to get the month before the previous month
def get_month_before_previous(date):
    first_day_of_previous_month = get_previous_month(date).replace(day=1)
    last_day_of_month_before_previous = first_day_of_previous_month - timedelta(days=1)
    return last_day_of_month_before_previous

# Get the previous month and before the previous month
previous_month = get_previous_month(current_date)
month_before_previous = get_month_before_previous(current_date)
Previous_month =previous_month.strftime("%B %Y")
Previous_month
#%%
Before_previous_month=month_before_previous.strftime("%B %Y")
Before_previous_month
#%%
PayMon=Previous_month 
PayMon
#####convert previous month and month_before_previous to datetime and date being hardcoded as 01
#%%
pMon=month_before_previous.strftime('%Y-%m-01 00:00:00')
pMon
#%%
cMon=previous_month.strftime('%Y-%m-01 00:00:00')
cMon
#%%
###################change required############
#######cMon is current month########
#######pMon is previous month########
#######PAyMon is payperiod month######
#cMon='2025-01-01 00:00:00'###'cMon is current month'///previous_month###
#pMon='2024-12-01 00:00:00'###'pMon is previous month'///month_before_previous#####
######in Qpay PAymon=========CMON########################
#PayMon='January 2025'###'cMon is words' ##########
#########################
#%%
######null coloumns created as terrier data has extra coloumn and some of Qpay data has also no matching cols
df_payroll['Employee_Current_Status'] = pd.Series([])
df_payroll['Employee_Name'] = pd.Series([])
df_payroll['Company_Name'] = pd.Series([])
df_payroll['Zone'] = pd.Series([])
df_payroll['Sector'] = pd.Series([])
df_payroll['Addition'] = pd.Series([])
df_payroll['Payroll_Run_date']=pd.Series([])
df_payroll['New_Deletion']=pd.Series([])
#%%
data=df_payroll['Pay_Period'].unique()
data
df_payroll.columns
df_payroll
df_payroll['New_Deletion']='present'
df_payroll['New_Deletion']
#%%
not_present_current_mon=df_payroll[(df_payroll['StartOfMonth'] == pMon) & (df_payroll['StartOfMonth'] != cMon) & df_payroll['Employee_Code'].isin(df_payroll[df_payroll['StartOfMonth'] == pMon]['Employee_Code']) & ~df_payroll['Employee_Code'].isin(df_payroll[df_payroll['StartOfMonth'] == cMon]['Employee_Code'])]
#not_present_july['New Deletion'] = np.random.choice(list(availability), len(not_present_july))
#not_present_july['New Deletion']  = not_present_july['New Deletion'] .map(availability)
not_present_current_mon
mon_Qpay3=not_present_current_mon
#%%
new_rows_current_mon= pd.DataFrame({'Entity_Name':mon_Qpay3['Entity_Name'] , 
'Company_Code': mon_Qpay3['Company_Code'],
'Employee_Code':mon_Qpay3['Employee_Code'], 
'Gender':mon_Qpay3['Gender'], 
'Gender1':mon_Qpay3['Gender1'],
'Pay_Period':PayMon, 
'Hiring_Status':mon_Qpay3['Hiring_Status'],
'Segment_Name':mon_Qpay3['Segment_Name'], 
'ISFMP':'', 
'New Addition':mon_Qpay3['New Addition'],
'DATE OF JOINING':mon_Qpay3['DATE OF JOINING'], 
'Age':mon_Qpay3['Age'],
'StartOfMonth':cMon, 
'PT State':mon_Qpay3['PT State'], 
'Work Location':mon_Qpay3['Work Location'],
'PF':mon_Qpay3['PF'], 
'ESI':mon_Qpay3['ESI'], 
'PT':mon_Qpay3['PT'], 
'EDLIC':mon_Qpay3['EDLIC'], 
'LWF':mon_Qpay3['LWF'], 
'UAN_Type':mon_Qpay3['UAN_Type'], 
'Region_Name':mon_Qpay3['Region_Name'],
'Employee_Current_Status':mon_Qpay3['Employee_Current_Status'],
'Employee_Name':mon_Qpay3['Employee_Name'],
'Company_Name':mon_Qpay3['Company_Name'], 
'Zone':mon_Qpay3['Zone'],
'Sector':mon_Qpay3['Sector'], 
'Last_Working_Day':mon_Qpay3['Last_Working_Day'], 
'Payroll_Run_date':mon_Qpay3['Payroll_Run_date'], 
'New_Deletion':'not_present',
'Group_Name':mon_Qpay3['Group_Name'],
'WBS_Name':mon_Qpay3['WBS_Name'], 
'Sap_Customer_Code':mon_Qpay3['Sap_Customer_Code']
})
#%%
new_rows_current_mon
#%%

#############extra column##############
#
#%%
present_Cmon=df_payroll[(df_payroll['StartOfMonth'] == cMon) & (df_payroll['StartOfMonth'] != pMon) & df_payroll['Employee_Code'].isin(df_payroll[df_payroll['StartOfMonth'] == cMon]['Employee_Code']) & ~df_payroll['Employee_Code'].isin(df_payroll[df_payroll['StartOfMonth'] == pMon]['Employee_Code'])]
present_Cmon
present_Cmon['Addition'] = 1
#%%
#present_mar=df_payroll[(df_payroll['StartOfMonth'] == '2024-03-01 00:00:00') & (df_payroll['StartOfMonth'] != '2024-02-01 00:00:00') & df_payroll['Employee_Code'].isin(df_payroll[df_payroll['StartOfMonth'] == '2024-03-01 00:00:00']['Employee_Code']) & ~df_payroll['Employee_Code'].isin(df_payroll[df_payroll['StartOfMonth'] == '2024-02-01 00:00:00']['Employee_Code'])]
#present_mar
#present_mar['Addition'] = 1
#%%
#merged_df = pd.merge(present_mar,df_payroll[['Employee_Code', 'Addition']], on='Employee_Code', how='right', suffixes=('', '_mar'))
#merged_df.loc[(merged_df['ISFMP'] == 0) & (merged_df['Addition_mar'] == 1), 'Addition'] = 1
#merged_df.drop(columns='Addition_mar', inplace=True)
#df_payroll['Addition'] = merged_df['Addition']
#%%
merged_df = pd.merge(present_Cmon,df_payroll[['Employee_Code', 'Addition']], on='Employee_Code', how='right', suffixes=('', '_CMon'))
merged_df.loc[(merged_df['ISFMP'] == 0) & (merged_df['Addition_CMon'] == 1), 'Addition'] = 1
merged_df.drop(columns='Addition_CMon', inplace=True)
df_payroll['Addition'] = merged_df['Addition']
#%%
df_payroll['Addition'] = df_payroll['Addition'].fillna(0)
#%%
df_mismatch =df_payroll[(df_payroll['Addition'] == 1) & (df_payroll['StartOfMonth'] == cMon)].shape[0]
df_mismatch
#%%
#filtered_df_Qpay = df_payroll.loc[(df_payroll['StartOfMonth'] == '2024-03-01 00:00:00') | (df_payroll['StartOfMonth'] == '2024-03-01 00:00:00')]
filtered_df_Qpay=df_payroll.loc[(df_payroll['StartOfMonth'] == cMon)]
#%%
filtered_df_Qpay.to_csv('satish.csv')
#%%
filtered_df_Qpay.columns
#%%
df_payroll.columns
#%%
merged_df=pd.concat([filtered_df_Qpay,new_rows_current_mon]).reset_index(drop=True)
merged_df
#%%
#merged_df.to_csv('ttt.csv')
#%%
new_rows_current_mon
#%%
filtered_df_Qpay
#%%
merged_df = merged_df.reindex(columns=['Entity_Name', 'Company_Code', 'Employee_Code', 'Gender', 'Gender1',
       'Pay_Period', 'Hiring_Status', 'Segment_Name', 'ISFMP', 'New Addition',
       'DATE OF JOINING', 'Age', 'StartOfMonth', 'PT State', 'Work Location',
       'PF', 'ESI', 'PT', 'EDLIC', 'LWF', 'UAN_Type', 'Region_Name',
       'Last_Working_Day', 'Employee_Current_Status',
       'Employee_Name', 'Company_Name', 'Zone', 'Sector', 'Payroll_Run_date',
       'New_Deletion', 'Designation_Name','ProcessType','Addition','Group_Name',
       'WBS_Name', 'Sap_Customer_Code'])
new_column_names={'New Addition':'New_Addition',
                  'DATE OF JOINING':'DATE_OF_JOINING',
                  'PT State':'PT_State',
                  'Work Location':'Work_Location',
                  'Last_Working_Day':'LWD'
                  }
merged_df.rename(columns=new_column_names,inplace=True)
#%%
merged_df.to_csv('data.csv')
#%%
merged_df.columns
#%%
try:
    
     engine = create_engine(f'mysql+pymysql://user:password@10.225.220.5/Payroll_Datamart')
     merged_df.to_sql("QMR.Terrier_QPAY_Quaterly",engine,index=False,if_exists='append')

except Exception as e:
  print(e)
# %%
df_payroll
# %%
df_payroll.columns
# %%
#############
###implementing schedular to run the code on previous month  18th evening