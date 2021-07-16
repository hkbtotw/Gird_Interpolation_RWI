from h3 import h3
from datetime import datetime, date, timedelta
from math import radians, cos, sin, asin, sqrt
from geopandas import GeoDataFrame
from shapely.geometry import Polygon, mapping
import pyproj    #to convert coordinate system
from csv_join_tambon import Reverse_GeoCoding
from Credential import *
import numpy as np
import os
import ast
import pandas as pd
import pickle
import glob
from sys import exit
import warnings
import requests
from decimal import Decimal
from tqdm import *

warnings.filterwarnings('ignore')

#enable tqdm with pandas, progress_apply
tqdm.pandas()

start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))

  
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def GetH3hex(lat,lng,h3_level):
    return h3.geo_to_h3(lat, lng, h3_level)

def is_number(x):
    try:        
        xx=Decimal(x)
        #print('isnum--',x,' :: ',type(x))
        if(xx.is_nan()==True):
            #print(' yes ',x)
            return 0
        else:
            return x
    except:
        return 0

def Average_Around_CenterGrid(dfGrid,hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 2)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)
    dfHex=dfHex.merge(dfGrid, on='hex_id', how='left')
    #print(' 1 --- ',dfHex)
    dfHex=dfHex[dfHex['_merge']=='both'].copy().reset_index(drop=True)
    #print(' 2 --- ',dfHex)
    dfSum=dfHex.mean()
    #print(dfSum['rwi'])
    #print(' Sum --- ',dfSum[1])   
    del dfHex, hexagons1, kRing
    return dfSum['rwi']

def Read_H3_Grid_Lv8_Province_PAT(province):
    #print('------------- Start ReadDB -------------', province)
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_tad

    cursor = conn.cursor()

    sql="""
            SELECT  [hex_id]
                     ,[Latitude]
                     ,[Longitude]
                     ,[population]
                     ,[population_youth]
                     ,[population_elder]
                     ,[population_under_five]
                     ,[population_515_2560]
                     ,[population_men]
                     ,[population_women]
                     ,[geometry]
                     ,[p_name_t]
                     ,[a_name_t]
                     ,[t_name_t]
                     ,[s_region]
                     ,[prov_idn]
                     ,[amphoe_idn]
                     ,[tambon_idn]
                     ,[DBCreatedAt]
              FROM [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province_PAT]
              where p_name_t= N'"""+str(province)+"""'
        """

    dfout=pd.read_sql(sql,conn)    
    #print(len(dfout.columns),' :: ',dfout.columns)
    #print(dfout)    
    del conn, cursor, sql
    #print(' --------- Reading End -------------')


    return dfout

def Get_Province_H3_Grid_Lv8_Province_PAT():
    #print('------------- Start ReadDB -------------', province)
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_tad

    cursor = conn.cursor()

    sql="""
            SELECT  distinct(p_name_t)
              FROM [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province_PAT]              
        """

    dfout=pd.read_sql(sql,conn)    
    #print(len(dfout.columns),' :: ',dfout.columns)
    #print(' province ===> ',dfout)    
    del conn, cursor, sql
    #print(' --------- Reading End -------------')


    return dfout

def Write_data_to_database(df_input, province, conn1):
    print('------------- Start WriteDB -------------')
    #df_write=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)
   
	## ODBC Driver 17 for SQL Server
    

    #- Delete all records from the table    
    #sql="""SELECT * FROM TSR_ADHOC.dbo.S_FCT_AG_SALEOUT"""
    sql="""delete FROM [TSR_ADHOC].[dbo].[H3_Grid_RWI_Lv8_Province] where p_name_t='"""+str(province)+"""' """
    
    cursor=conn1.cursor()
    cursor.execute(sql)
    conn1.commit()


    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[H3_Grid_RWI_Lv8_Province](	
        [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[rwi]
      ,[geometry]
      ,[p_name_t]
      ,[a_name_t]
      ,[t_name_t]
      ,[s_region]
      ,[prov_idn]
      ,[amphoe_idn]
      ,[tambon_idn]
      ,[DBCreatedAt]
	)     
    values(?,?,?,?,?,
    ?,?,?,?,?,
    ?,?,?)""", 
    row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['rwi']
      ,row['geometry']
      ,row['p_name_t']
      ,row['a_name_t']
      ,row['t_name_t']
      ,row['s_region']
      ,row['prov_idn']
      ,row['amphoe_idn']
      ,row['tambon_idn']
      ,row['DBCreatedAt']	   
     ) 
    conn1.commit()

    cursor.close()
    print('------------Complete WriteDB-------------')

########################################################################################################
######  Input ----  ####################################################################################
# SQL connection for writing data to database
conn = connect_tad

# level 7 covers approx 1 km2
h3_level_large=7   
h3_level_small=8

# working directory
current_path=os.getcwd()
print(' -- current directory : ',current_path)  # Prints the current working directory
boundary_path=current_path+'\\boundary_data\\'
input_path=current_path+'\\'

# input filename
#input_name='test_นนทบุรี_shapefile_32647_PAT.csv'
rwi_name='tha_relative_wealth_index.csv'      #### facebook relative wealth index , version 2021-04-21
output_name='interpolate_rwi.csv'
continue_name='complete.csv'

#######################################################################################################

#### 1. Read rwi location data
dfIn=pd.read_csv(current_path+'\\data\\'+rwi_name)
dfIn.rename(columns={'latitude':'Latitude','longitude':'Longitude'},inplace=True)
#print(len(dfIn),' ======= ',dfIn.head(10))

#### Find province name
dfIn=Reverse_GeoCoding(dfIn)
dropList=['geometry','index_right', 'p_code', 'a_code', 't_code', 'prov_idn','amphoe_idn', 'tambon_idn', 'area_sqm', 'BS_IDX']
dfIn.drop(columns=dropList,inplace=True)
#print(dfIn.columns, ' ======= ',dfIn)

## find hex_id
dfIn['hex_id']=dfIn.progress_apply(lambda x: GetH3hex(x['Latitude'],x['Longitude'],h3_level_large),axis=1)
#print(dfIn.columns, ' ======= ',dfIn)

###### 2.   USe name rwi file but select province 
provinceList=list(Get_Province_H3_Grid_Lv8_Province_PAT()['p_name_t'].unique())
#provinceList=provinceList[:3]
#print(type(provinceList),' list : ',provinceList)
print(' Original -- left :  ',len(provinceList))

###### Check provinces not being processed in case of previous incomplete runs.
try:
    dfContinue=pd.read_csv(current_path+'\\'+continue_name)    
    continueList=list(dfContinue['province'].unique())
    print(' complete list : ', continueList)
except:
    continueList=[]

provinceList=[x for x in provinceList if x not in continueList]
print(' continue -- left :  ',len(provinceList))
if(len(provinceList)==0):
    print(' ----- No more provinces left to work on ------ ')
    exit()


completeList=[]+continueList
#province='นนทบุรี'
province_bar=tqdm(provinceList)
for province in province_bar:    
    
    province_bar.set_description("Processing %s" % province)
    #### Read province Lv 8 data
    #dfGrid=pd.read_csv(current_path+'\\data\\'+input_name)
    #dfGrid.drop(columns=['Unnamed: 0'],inplace=True)

    ### Find Lv 7 hex_id for each Lv 8 hex_id
    dfGrid=Read_H3_Grid_Lv8_Province_PAT(province)
    dfGrid['hex_id_lv7']=dfGrid.progress_apply(lambda x: GetH3hex(x['Latitude'],x['Longitude'],h3_level_large),axis=1)
    #print(len(dfGrid),' === Grid ==== ',dfGrid.head(10))

    ### Select province
    dfDummy=dfIn[dfIn['p_name_t']==province].copy().reset_index(drop=True)
    #print(dfDummy.columns, ' ======= ',dfDummy)

    dfagg=pd.DataFrame(dfDummy.groupby(by=['hex_id'])['rwi'].mean()).reset_index()
    dfagg.rename(columns={'hex_id':'hex_id_lv7'},inplace=True)
    #print(' ==> ',dfagg,' -- ',type(dfagg))

    #### Merge dfGrid with dfagg with hex_id_lv7
    dfGrid=dfGrid.merge(dfagg, how='left', on='hex_id_lv7',indicator=True)
    dfGrid['rwi']=dfGrid.apply(lambda x: is_number(x['rwi']),axis=1)    # Convert NaN to 0
    #print(len(dfGrid),' === Merge ==== ',dfGrid.head(10))

    dfA=dfGrid[dfGrid['rwi']!=0].copy().reset_index(drop=True)
    #print(len(dfA),' A ==> ',dfA)
    dfCheck=dfGrid[dfGrid['rwi']==0].copy().reset_index(drop=True)
    #dfCheck=dfCheck.head(5)
    dfCheck['rwi']=dfCheck.progress_apply(lambda x:Average_Around_CenterGrid(dfGrid,x['hex_id'], h3_level_small),axis=1)
    #print(len(dfCheck),' Check ==> ',dfCheck)

    dfA=dfA.append(dfCheck, ignore_index=True)
    #print(len(dfA),' A ==> ',dfA)
    dfA['DBCreatedAt']=nowStr
    includeList=['hex_id','Latitude','Longitude','rwi','geometry','p_name_t','a_name_t','t_name_t','s_region','prov_idn','amphoe_idn','tambon_idn','DBCreatedAt']
    dfA=dfA[includeList].copy()
    #print(' --> ',dfA.dtypes)

    completeList.append(province)

    ### Output data
    pd.DataFrame(completeList,columns=['province']).to_csv('complete.csv')
    dfA.to_csv(province+'_'+output_name)
    Write_data_to_database(dfA, province,conn)

conn.close()
del dfIn, dfDummy, dfGrid, dfagg, dfA, dfCheck
###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')