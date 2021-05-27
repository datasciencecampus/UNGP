import requests
import csv
import pandas as pd
import time
from datetime import datetime
import os

# read env variables
if os.environ
EE_token = os.environ["EE_token"]
EE_url = os.environ["EE_url"]
numAttempts = int(os.environ["numAttempts"])   # how many times it tried to get get the data before giving up. With every attempt data for a longer period is extracted
delayBetweenAttempts = int(os.environ["delayBetweenAttempts"])   # measured in seconds
delayBetweenPings = int(os.environ["delayBetweenPings"])   # measured in seconds -> how often in normal times it should retrieve data

assert (len(EE_token) >= 30)
assert (len(EE_url) >= 30)
assert (numAttempts >= 5)
assert (delayBetweenAttempts >= 0)
assert (delayBetweenPings >= 20)

#test print - to be removed
print ("EE_token:", EE_token, " EE_url:", EE_url, " numAttempts:",numAttempts, " delayBetweenAttempts:", delayBetweenAttempts, " delayBetweenPings:", delayBetweenPings)

step=0
stopFL=False
startFL=True
sleepEnabledFL = True
errorCount=0
token = "2b0118bd-a63f-4bdd-8c47-8d2ce40b40d2"
csv.register_dialect('pipes', lineterminator = '\r\n', delimiter=',')

while False and (not stopFL) and errorCount < 900:
    time.sleep(0.7)
    if  sleepEnabledFL: time.sleep(118)
    step = step + 1
    reqStr = "{}/gws/wfs?authkey={}&service=WFS&version=1.1.0\
        &request=GetFeature&outputformat=csv&typeName=exactAIS:LVI\
        &cql_filter=ts_insert_utc>=dateFormat('yyyyMMddHHmmss',currentDate('-PT{}M{}S'))".format(EE_url, EE_token, 5 if startFL else 2, delayBetweenAttempts*errorCount+15 )
    try:
        resp = requests.get(reqStr, timeout=(5,10))
    except requests.exceptions.Timeout:
        #  set up for a retry, or continue in a retry loop
        errorCount = errorCount + 1
        sleepEnabledFL = False
        print ("Timeout waiting for response!, Attempts:",errorCount)
    except requests.exceptions.TooManyRedirects:
        # Tell the user
        print ("Too many redirects,  Attempts:", errorCount)
        errorCount = errorCount + 1
        sleepEnabledFL = False
    except requests.exceptions.RequestException as e:
        # catastrophic error. retry up to the number of Max retries
        errorCount = errorCount + 1
        print("Request exception:", e , " Attempt:", errorCount)
        sleepEnabledFL = False
    except:
        errorCount = errorCount + 1
        print("Unknown error! Attempt:", errorCount)
        sleepEnabledFL = False
    else:
        # in case of no errors extraction of data may begin
        if resp.status_code != 200:
            # This means something went wrong and although the request was received OK the server is complining.
            print('error in response, status code: {}'.format(resp.status_code))
            errorCount = errorCount + 1
            sleepEnabledFL = True
        else:
            #everything is fine so get extracting
            csv_reader = csv.reader(resp.text.splitlines(), dialect = 'pipes' )
            headr = next(csv_reader, None)
            if startFL :
                df=pd.DataFrame([x for x in csv_reader], columns= headr)
                startFL = False
            else:
                df=df.append(pd.DataFrame([x for x in csv_reader], columns= headr)).drop_duplicates()
            dflen = len(df)
            print(step,":",dflen)
            #reset error handling parameters
            sleepEnabledFL = True
            errorCount = 0
            if dflen > 300000:
                dateTimeObj = datetime.now()
                df.to_csv("/data/API/shipUpdates{}_{}_{}_{}_{}.csv"
                          .format(dateTimeObj.year,
                                  dateTimeObj.month,
                                  dateTimeObj.day,
                                  dateTimeObj.hour,
                                  dateTimeObj.minute))
                startFL = True



