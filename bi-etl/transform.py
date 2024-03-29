import os
import csv
import pandas as pd
import numpy as np
import requests
import urllib.request, urllib.error
import json
from collections import defaultdict

df = pd.read_csv('extracted.csv')

print(list(df.columns.values))

# transform date & time to year, quarter, month, week, day & hour
# print(df['date'].values[0])
# print(df['time'].values[0])
df.date = pd.to_datetime(df.date, errors='coerce')
df = df.dropna(subset=['date'])
df.time = pd.to_datetime(df.time, format="%H:%M:%S", errors='coerce')
df = df.dropna(subset=['time'])
df['year'] = pd.DatetimeIndex(df['date']).year
df['quarter'] = pd.PeriodIndex(df.date, freq='Q')
df['month'] = df.date.apply(lambda x: x.strftime('%B-%Y')) 
df['week'] = df['date'].dt.to_period('W-SAT').apply(lambda r: r.start_time).dt.date
df['day'] = df.date.dt.day_name()
df['hour'] = df.time.dt.hour
# print(df['year'].values[0])
# print(df['quarter'].values[0])
# print(df['month'].values[0])
# print(df['week'].values[0])
# print(df['day'].values[0])
# print(df['hour'].values[0])

# get location from an IP using geolocation-db.com
def getLocation(ip):
    url = "https://geolocation-db.com/jsonp/"+ip
    try:
        res = urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        # Return code error (e.g. 404, 501, ...)
        print('HTTPError: {}'.format(e.code))
    except urllib.error.URLError as e:
        # Not an HTTP-specific error (e.g. connection refused)
        print('URLError: {}'.format(e.reason))
    else:
        # 200
        data = res.read().decode()
        data = data.split("(")[1].strip(")")
        return [json.loads(data)['country_name'], json.loads(data)['city']]

# print(df['c-ip'].values[11])
ips = []
countries = []
cities = []
dcountry = {}
dcity = {}
ips1 = []
filetypes = []
ostypes = []
browsertypes = []
l = 1
# drop rows with empty values in column c-ip
df['c-ip'].str.strip().replace('', np.nan, inplace=True)
df.dropna(subset=['c-ip'], inplace=True)
for index, row in df.iterrows():
   print("processing row number "+str(l))
   l = l + 1
   # transform IP into country & city 
   data = row['c-ip']
   ips.append(data)
   if data not in ips1:
    ips1.append(data)
    print("calling geolocation API for " + row['c-ip'])
    response = getLocation(row['c-ip'])
    if response != None:
            tcountry = ''
            tcity = ''
            if str(response[0]) == "None":
                tcountry = 'NA'
            else:
                tcountry = response[0]
            if str(response[1]) == "None":
                tcity = 'NA'
            else:
                tcity = response[1]
            countries.append(tcountry)
            cities.append(tcity)
            dcountry[data] = tcountry
            dcity[data] = tcity
    else:
            countries.append('NA')
            cities.append('NA')
            dcountry[data] = 'NA'
            dcity[data] = 'NA'
   else:
       countries.append(dcountry[data])
       cities.append(dcity[data])
   # transform cs-uri-stem to file type
   data1 = row['cs-uri-stem']
   afiletype = str.split(data1, '.')
   if len(afiletype) < 2:
       filetype = 'NA'
   else:
       filetype = afiletype[1]
   filetypes.append(filetype)
   # transform cs(User-Agent) to OS
   data2 = row['cs(User-Agent)']
   if 'Windows' in data2:
        ostypes.append('Windows')
   elif 'Macintosh' in data2:
       ostypes.append('Macintosh')
   elif 'yandex' in data2 or 'crawler' in data2 or 'yahoo' in data2 or 'baidu' in data2 or 'bot' in data2:
       ostypes.append('Known robots')
   elif data2 == '-': 
       ostypes.append('OS unknown')
   else:
       ostypes.append('Other')
   # transform cs(User-Agent) to browser
   if 'Firefox' in data2:
       browsertypes.append('Firefox')
   elif 'MSIE' in data2:
       browsertypes.append('MSIE')
   elif 'Safari' in data2:
       browsertypes.append('Safari')
   elif 'Yandex' in data2:
       browsertypes.append('Yandex')
   elif 'Baiduspider' in data2:
       browsertypes.append('Baiduspider')
   elif 'msnbot' in data2:
       browsertypes.append('msnbot')
   elif 'Netscape' in data2:
       browsertypes.append('Netscape')
   elif 'Sogou' in data2:
       browsertypes.append('Sogou')
   elif 'panscient' in data2:
       browsertypes.append('panscient')
   else:
       browsertypes.append('Other')

locations = {'ip': ips, 'country': countries, 'city': cities}
df_ips = pd.DataFrame(locations, columns = ['ip', 'country', 'city'])
df['ip'] = df_ips['ip']
df['country'] = df_ips['country']
df['city'] = df_ips['city']
# print(df['ip'].values[11])
# print(df['country'].values[11])
# print(df['city'].values[11])   

files = {'filetype' :filetypes, 'os' :ostypes, 'browser' :browsertypes}
df_requests = pd.DataFrame(files, columns = ['filetype', 'os', 'browser'])
df['uri'] = df['cs-uri-stem']
df['filetype'] = df_requests['filetype']
df['os'] = df_requests['os']
df['browser'] = df_requests['browser']
# print(df['cs-uri-stem'].values[12])
# print(df['uri'].values[12])
# print(df['filetype'].values[12])
# print(df['cs(User-Agent)'].values[12])
# print(df['os'].values[12])
# print(df['browser'].values[12])

df['statuscode'] = df['sc-status']
df['timetaken'] = df['time-taken']
# print(df['statuscode'].values[12])
# print(df['timetaken'].values[12])
# print(list(df.columns.values))

# drop old columns from the dataframe
df = df.drop(['date', 'time', 's-ip', 'cs-method', 'cs-uri-stem', 'cs-uri-query', 's-port', 'cs-username', 'c-ip', 'cs(User-Agent)', 'sc-status', 'sc-substatus', 'sc-win32-status', 'time-taken'], axis=1)
print(list(df.columns.values))
df.to_csv('transformed.csv', index=None)
# print(list(df.columns.values))
