## import necessary libraries
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import requests
import json
import pandas as pd
import string
import random
import hashlib


#################################### establish connection to database################################################
cred_info = open('snowCred.json')
credentials = json.load(cred_info)
ctx = snowflake.connector.connect(
    user=credentials['user'],
    password=credentials['password'],
    account=credentials['account'],
    warehouse = 'SPARKNETWORK'
    )
cs = ctx.cursor()

cs.execute('USE ROLE SYSADMIN')
cs.execute('DROP DATABASE IF EXISTS RAW_SPARKNERWORK CASCADE')
cs.execute('CREATE DATABASE RAW_SPARKNERWORK')
cs.execute('USE DATABASE RAW_SPARKNERWORK')
cs.execute('USE SCHEMA PUBLIC')




############################################## function to get data from api end point ##############################################
def get_api_data(url):
    response = requests.get(url)
    responsejson = json.loads(response.text)
    return response.text, responsejson


user_url = "https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users"
userResult,userResultJson = get_api_data(user_url)
message_url = "https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages"
messageResult,messageResultJson = get_api_data(message_url)






############################################ function to pseudonymize column########################################################################
salt = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
key = {}

def pseudonymize(colname):
    if colname not in key:
        sha3 = hashlib.sha3_512()
        data = salt + colname
        sha3.update(data.encode('utf-8'))
        hexdigest = sha3.hexdigest()
        key[colname] = hexdigest
        return hexdigest
    else:
        return key[colname]






########################################################## Processing raw data of USER ####################################################################

#take data related to user
user_profile = pd.json_normalize(userResultJson,  meta= [ ['profile', 'gender'], ['profile', 'isSmoking'], ['profile', 'profession'],['profile', 'income'] ])

#drop unnecessary columns
user_profile_raw = user_profile.drop(columns = {'subscription'})

#add audit columns
user_profile_raw['ingestion_date'] = pd.to_datetime('now',utc=True)
user_profile_raw['source'] = 'mockapi/user'

# change data type
user_profile_dict = {'createdAt': str, 
                     'updatedAt': str, 
                     'firstName': str, 
                     'lastName': str, 
                     'address': str, 
                     'city': str,
                     'country': str, 
                     'zipCode': str, 
                     'email': str, 
                     'birthDate': str, 
                     'id': str, 
                     'profile.gender': str,
                     'profile.isSmoking': bool, 
                     'profile.profession': str, 
                     'profile.income': float,       
                     'ingestion_date': str, 
                     'source': str
                }
user_profile_raw = user_profile_raw.astype(user_profile_dict)

# cahnge datetime columns to date time formate
user_profile_raw[['createdAt','updatedAt','birthDate', 'ingestion_date']]= user_profile_raw[['createdAt','updatedAt','birthDate', 'ingestion_date']].apply(pd.to_datetime)


#rearrange columns
user_profile_raw = user_profile_raw[['id', 'createdAt', 'updatedAt', 'firstName', 'lastName', 'address', 'city','country', 'zipCode', 'email', 'birthDate', 'profile.gender','profile.isSmoking', 'profile.profession', 'profile.income','ingestion_date', 'source']]
user_profile_raw = user_profile_raw.rename(columns = {'id': 'USER_ID'})


### push user_profile_raw to snowflake table
cs.execute('CREATE OR REPLACE TABLE USERRAW ( USER_ID STRING NOT NULL,"createdAt" TIMESTAMP, "updatedAt" TIMESTAMP, "firstName" STRING, "lastName" STRING, "address" STRING,"city" STRING, "country" STRING, "zipCode" STRING, "email" STRING, "birthDate" TIMESTAMP, "profile.gender" STRING,"profile.isSmoking" BOOLEAN, "profile.profession" STRING, "profile.income" STRING,"ingestion_date" TIMESTAMP, "source" STRING, PRIMARY KEY (USER_ID))')
write_pandas(ctx,user_profile_raw,table_name="USERRAW")

print("user_profile_raw ok")








#################################################################### Processing raw data of Subscription  #########################################################################

# get necessary data for subscription table
df_subs_raw = pd.json_normalize(userResultJson, record_path =['subscription'], meta= ['id'])

#add audit columns
df_subs_raw['ingestion_date'] = pd.to_datetime('now',utc=True)
df_subs_raw['source'] = 'mockapi/subs'

# change data type
subs_raw_convert_dict = {'createdAt': str, 
                'startDate': str, 
                'endDate': str, 
                'status': str, 
                'amount': float, 
                'id': str, 
                'ingestion_date': str,
                'source': str
                }

df_subs_raw = df_subs_raw.astype(subs_raw_convert_dict)

#convert date time columns to date time format
df_subs_raw[['createdAt','startDate','endDate','ingestion_date']]= df_subs_raw[['createdAt','startDate','endDate','ingestion_date']].apply(pd.to_datetime, utc = True)

##rearrange and rename columns
df_subs_raw = df_subs_raw[['id', 'createdAt', 'startDate', 'endDate', 'status', 'amount', 'ingestion_date', 'source']]
df_subs_raw = df_subs_raw.rename(columns= {'id':'USER_ID'})

### push df_subs_raw to snowflake table
cs.execute('CREATE OR REPLACE TABLE SUBSCRIPTIONSRAW( USER_ID STRING NOT NULL, "createdAt" TIMESTAMP, "startDate" TIMESTAMP, "endDate" TIMESTAMP, "status" STRING, "amount" FLOAT, "ingestion_date" TIMESTAMP, "source" STRING, FOREIGN KEY (USER_ID) REFERENCES USERRAW(USER_ID))')
write_pandas(ctx,df_subs_raw,table_name="SUBSCRIPTIONSRAW")

print("df_subs_raw ok")






################################################################# processing raw data of MESSAGE #########################################################################

#get data for message table
df_message_raw = pd.json_normalize(messageResultJson)[['id', 'createdAt','receiverId' , 'senderId']]

#add audit columns
df_message_raw['ingestion_date'] = pd.to_datetime('now',utc=True)
df_message_raw['source'] = 'mockapi/messages'

# change data type
message_raw_convert_dict = {'id': str, 
                'createdAt': str, 
                'receiverId': str, 
                'senderId': str,
                'ingestion_date': str,
                'source': str
                }

df_message_raw = df_message_raw.astype(message_raw_convert_dict)
df_message_raw[['createdAt', 'ingestion_date']]= df_message_raw[['createdAt', 'ingestion_date']].apply(pd.to_datetime, utc = True)
df_message_raw = df_message_raw.rename(columns= {'id':'MESSAGE_ID'})

### push df_message_raw to snowflake table
cs.execute('CREATE OR REPLACE TABLE MESSAGESRAW( MESSAGE_ID STRING NOT NULL, "createdAt" TIMESTAMP, "receiverId" STRING, "senderId" STRING, "ingestion_date" TIMESTAMP, "source" STRING, PRIMARY KEY (MESSAGE_ID), FOREIGN KEY ("receiverId") REFERENCES USERRAW(USER_ID), FOREIGN KEY ("senderId") REFERENCES USERRAW(USER_ID))')
write_pandas(ctx,df_message_raw,table_name="MESSAGESRAW")

print('df_message_raw ok')






########################################################### Data transformation #####################################################################

### create sqlalcjemy engine 
url = URL(
    user=credentials['user'],
    password=credentials['password'],
    account=credentials['account'],
    warehouse='SPARKNETWORK',
    database='RAW_SPARKNERWORK',
    schema='PUBLIC',
    role = 'SYSADMIN'
)
engine = create_engine(url)
connection = engine.connect()




######################################################### Transform USER table ##################################################################

USERRAW_query = '''
select USER_ID, "createdAt", "updatedAt","city", "country", "zipCode", "email", "birthDate", "profile.gender", "profile.isSmoking", "profile.income"
from USERRAW
'''

df_user_raw = pd.read_sql(USERRAW_query, connection)

#get age
now = pd.to_datetime('now', utc=True)
df_user_raw['birthDate']= df_user_raw['birthDate'].apply(pd.to_datetime, utc=True)
df_user_raw['Age'] =  df_user_raw['birthDate'].apply(lambda x : int((now-x).days/365))

#get email domain name
df_user_raw['domain.name'] = df_user_raw['email'].str.extract('@(\w.+)', expand=True)

#drop birthdate, and email columns
df_user_raw = df_user_raw.drop(columns={'birthDate', 'email'})

#pseudonymize user_id
df_user_raw['user_id'] = df_user_raw['user_id'].map(pseudonymize)
df_user_raw = df_user_raw.rename(columns= {'user_id': 'USER_ID'})

df_user_raw[['createdAt', 'updatedAt']]= df_user_raw[['createdAt', 'updatedAt']].apply(pd.to_datetime, utc = True)






######################################################### Transform SUBSCRIPTION table##################################################################
SUBSCRIPTION_query = '''
select USER_ID, "createdAt", "startDate", "endDate", "status", "amount"
from SUBSCRIPTIONSRAW
'''

df_subscription_raw = pd.read_sql(SUBSCRIPTION_query, connection)

#pseudonymize user_id
df_subscription_raw['user_id'] = df_subscription_raw['user_id'].map(pseudonymize)
df_subscription_raw = df_subscription_raw.rename(columns= {'user_id': 'USER_ID'})

df_subscription_raw[['createdAt', 'startDate', 'endDate']]= df_subscription_raw[['createdAt', 'startDate', 'endDate']].apply(pd.to_datetime, utc = True)







######################################################### Transform MESSAGE table##################################################################

MESSAGESRAW_query = '''
select MESSAGE_ID, "createdAt", "receiverId", "senderId"
from MESSAGESRAW
'''

df_message_raw = pd.read_sql(MESSAGESRAW_query, connection)

#pseudonymize user_id
df_message_raw['receiverId'] = df_message_raw['receiverId'].map(pseudonymize)
df_message_raw['senderId'] = df_message_raw['senderId'].map(pseudonymize)

df_message_raw = df_message_raw.rename(columns= {'message_id': 'MESSAGE_ID'})
df_message_raw[['createdAt']]= df_message_raw[['createdAt']].apply(pd.to_datetime, utc = True)







############################################################### Push final data set to gold layer ################################################## 

cs.execute('USE ROLE SYSADMIN')
cs.execute('DROP DATABASE IF EXISTS GOLD_SPARKNERWORK CASCADE')
cs.execute('CREATE DATABASE GOLD_SPARKNERWORK')
cs.execute('USE DATABASE GOLD_SPARKNERWORK')
cs.execute('USE SCHEMA PUBLIC')


cs.execute('CREATE OR REPLACE TABLE USER(USER_ID STRING NOT NULL, "createdAt" TIMESTAMP, "updatedAt" TIMESTAMP,"city" STRING, "country" STRING, "zipCode" STRING, "profile.gender" STRING, "profile.isSmoking" BOOLEAN, "profile.income" FLOAT, "Age" INTEGER, "domain.name" STRING, PRIMARY KEY(USER_ID))')
write_pandas(ctx,df_user_raw,table_name="USER")

print(" Finall USER pushed")

cs.execute('CREATE OR REPLACE TABLE SUBSCRIPTION(USER_ID STRING NOT NULL, "createdAt" TIMESTAMP, "startDate" TIMESTAMP, "endDate" TIMESTAMP, "status" STRING, "amount" FLOAT, FOREIGN KEY (USER_ID) REFERENCES USER(USER_ID))')
write_pandas(ctx,df_subscription_raw,table_name="SUBSCRIPTION")

print(" Finall SUBSCRIPTION pushed")

cs.execute('CREATE OR REPLACE TABLE MESSAGES( MESSAGE_ID STRING NOT NULL, "createdAt" TIMESTAMP, "receiverId" STRING, "senderId" STRING, PRIMARY KEY (MESSAGE_ID), FOREIGN KEY ("receiverId") REFERENCES USER(USER_ID), FOREIGN KEY ("senderId") REFERENCES USER(USER_ID))')
write_pandas(ctx,df_message_raw,table_name="MESSAGES")

print(" Finall MESSAGES pushed")