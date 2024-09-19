import requests
import pandas as pd
import base64
from pandas.io.json import json_normalize
import json
# from geopy.geocoders import Nominatim
# import folium
apidomain = "https://0509.callidusondemand.com/api/v2/"
payments = "salesTransactions"
expand = "?expand=transactionAssignments,eventType&"
filter = '$filter=genericAttribute29 eq "CEB" and eventType/eventTypeId eq "Sales Actual"'
skip = "&skip=0&top=100"
#processingunit = "processingUnits"
headers = {
    'authorization': "Basic UmVzdEFkbWluOlBlcmYwcm1AMQ==",
    'content-type': "application/json",
    'Accept': "application/json",
    'cache-control': "no-cache"
    }
def pipelinejob():
    response = requests.request("GET",apidomain+payments+expand+filter,headers=headers)
    response.encoding = 'utf-8'
    # results = json.loads(response.text)
    # #print(results)
    j = response.json()
    print(j)
    # df1=pd.DataFrame()
    df = pd.DataFrame(j)
    # for i in df:
    #     df1 = df1.append({'transactionAssignments': i}, ignore_index=True)
    # df.to_csv('file3.csv')
    

    for x in j['salesTransactions']:
        print(x['transactionAssignments'][0]['positionName'])
    
    # # # json_str = json.dumps(results["salesTransactions"])
    df = pd.DataFrame(results)
    # # df.to_csv('file2.csv')
    
    ##this is method 2
    # print(df[df.columns[2]])
    df_columns=df[df.columns[2]]
    df_columns.to_csv('df_columns.csv')
    
    #this is method 3
    # print(df.loc[:,["transactionAssignments"]])
    # df_loc=df.loc[:,["transactionAssignments"][0]]
    # df_loc.to_csv('df_loc.csv')
    
    
    ##this is method 4
    # print(df[df.columns[0]])
    # df_column_name=df[df["transactionAssignments"]]
    # df_column_name.to_csv('df_column_name.csv')
    
     ##this is method 5
    # df_list_of_dics = df_loc.get("positionName")
    # print(df_list_of_dics)
    
    # #this is method 6
    # for i in df_loc:
    #     print (i[0]["positionName"])
 
    # this is method 7
    # print('Method 7')
    # for x in df:
    #     for key in x:
    #         print(x[key])
            
    # df = pd.DataFrame.from_records(technologies)
    
    # print (df["transactionAssignments"])
    # for x in df:
    #     print (x["transactionAssignments"]["positionName"])
    # print(df.loc[:,["transactionAssignments"]])
    
    df = df[['genericAttribute29','genericAttribute30','genericAttribute31','salesTransactionSeq','transactionAssignments','value',
         'eventType','compensationDate']]
    
    # print(df)
    df['compensationDate'] = df['compensationDate'].str[0:10]
    df1 = df['value']
    df1 = pd.json_normalize(df1)
    df1 = df1.rename(columns={"unitType.name": "Currency"})
    df1 = df1[['value','Currency']]
    df2 = df['transactionAssignments']
    
    print(df2)
    #df2 = df2.replace("[", "")
    #df2 = pd.json_normalize(df['transactionAssignments'])
    #df2 = pd.json_normalize(df2, 'positionName')
    # print(df2)
    df2.to_csv('file1.csv')
    #df2 = pd.json_normalize(df2['period'])
    #df2 = df2.rename(columns={"logicalKeys.name": "Period"})
    df5 = df['genericAttribute30'].unique()
    df4 = df['genericAttribute31'].unique()
    # print(df5)
    # print(df4)
    abc = pd.concat([df, df1], axis=1)
    # print(df2)
    # geolocator = Nominatim(user_agent="my_user_agent")
    city = df4     ##GA31  
    country = df5  ##GA30
    # loc = geolocator.geocode(city+','+ country)
    # print("latitude is :-" ,loc.latitude,"\nlongtitude is:-" ,loc.longitude)
pipelinejob()