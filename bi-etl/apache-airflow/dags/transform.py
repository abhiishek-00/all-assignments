import pandas as pd
import numpy as np
import urllib.request, urllib.error
import json
import os

# get location from an IP using geolocation-db.com
def getLocation(ip):
    url = "https://geolocation-db.com/jsonp/"+ip
    try:
        res = urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        # return code error (e.g. 404, 501, ...)
        print('HTTPError: {}'.format(e.code))
    except urllib.error.URLError as e:
        # not an HTTP-specific error (e.g. connection refused)
        print('URLError: {}'.format(e.reason))
    else:
        # 200
        data = res.read().decode()
        data = data.split("(")[1].strip(")")
        return [json.loads(data)['country_name'], json.loads(data)['city']]

def transformData():
    df = pd.read_csv('/opt/airflow/dags/extracted.csv')
    print(list(df.columns.values))

    # transform date & time to year, quarter, month, week, day & hour
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
    ids = []
    dateids = []
    clientids = []
    requestids = []

    for index, row in df.iterrows():
        print("processing row number "+str(l))
        ids.append(l)
        dateids.append("d"+str(l))
        clientids.append("c"+str(l))
        requestids.append("r"+str(l))
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

    files = {'filetype' :filetypes, 'os' :ostypes, 'browser' :browsertypes, 'id' :ids}
    df_requests = pd.DataFrame(files, columns = ['filetype', 'os', 'browser', 'id'])
    df['uri'] = df['cs-uri-stem']
    df['filetype'] = df_requests['filetype']
    df['os'] = df_requests['os']
    df['browser'] = df_requests['browser']
    df['id'] = df_requests['id']

    df['statuscode'] = df['sc-status']
    df['timetaken'] = df['time-taken']

    # drop old columns from the dataframe
    df = df.drop(['date', 'time', 's-ip', 'cs-method', 'cs-uri-stem', 'cs-uri-query', 's-port', 'cs-username', 'c-ip', 'cs(User-Agent)', 'sc-status', 'sc-substatus', 'sc-win32-status', 'time-taken'], axis=1)
    # move id to 1st column
    first_column = df.pop('id') 
    df.insert(0, 'id', first_column)
    print(df.head(1))
    df.to_csv('/opt/airflow/dags/transformed.csv', index=None)

    # create csvs for relational DB
    dates = {'id': dateids}
    df_etldatetable = pd.DataFrame(dates, columns = ['id'])
    df_etldatetable['year'] = df['year']
    df_etldatetable['quarter'] = df['quarter']
    df_etldatetable['month'] = df['month']
    df_etldatetable['week'] = df['week']
    df_etldatetable['day'] = df['day']
    df_etldatetable['hour'] = df['hour']
    print(list(df_etldatetable.columns.values))
    print(df_etldatetable.head(1))
    df_etldatetable.to_csv('/opt/airflow/dags/etldatetable.csv', index=None)
    locations = {'id': clientids, 'ip': ips, 'country': countries, 'city': cities}
    df_eltclienttable = pd.DataFrame(locations, columns = ['id', 'ip', 'country', 'city'])
    print(list(df_eltclienttable.columns.values))
    print(df_eltclienttable.head(1))
    df_eltclienttable.to_csv('/opt/airflow/dags/eltclienttable.csv', index=None)
    files = {'id': requestids, 'filetype' :filetypes, 'os' :ostypes, 'browser' :browsertypes}
    df_etlrequesttable = pd.DataFrame(files, columns = ['id', 'filetype', 'os', 'browser'])
    df_etlrequesttable['uri'] = df['uri']
    second_column = df_etlrequesttable.pop('uri') 
    df_etlrequesttable.insert(1, 'uri', second_column)
    print(list(df_etlrequesttable.columns.values))
    print(df_etlrequesttable.head(1))
    df_etlrequesttable.to_csv('/opt/airflow/dags/etlrequesttable.csv', index=None)
    df_etlmaintable = df.drop(['year', 'quarter', 'month', 'week', 'day', 'hour', 'ip', 'country', 'city', 'uri', 'filetype', 'os', 'browser'], axis=1)
    df_etlmaintable['dateid'] = df_etldatetable['id']
    df_etlmaintable['clientid'] = df_eltclienttable['id']
    df_etlmaintable['requestid'] = df_etlrequesttable['id']
    print(list(df_etlmaintable.columns.values))
    print(df_etlmaintable.head(1))
    df_etlmaintable.to_csv('/opt/airflow/dags/etlmaintable.csv', index=None)

def checkIfTransformCSVPresent():
    if os.path.isfile('/opt/airflow/dags/transformed.csv'):
        return True
    else:
        transformData()
