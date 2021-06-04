import requests
import csv
import pandas as pd
import time
from datetime import datetime
import os
import subprocess

# check and read env variables
MANDATORY_ENV_VARS = ["EE_token", "EE_url", "numAttempts", "delayBetweenAttempts", "delayBetweenPings", "numLinesInCSV","debugLevel"]

for var in MANDATORY_ENV_VARS:
    if var not in os.environ:
        raise EnvironmentError("Failed because {} is not set.".format(var))

EE_token = os.environ["EE_token"]
EE_url = os.environ["EE_url"]
numAttempts = int(os.environ["numAttempts"])   # how many times it tries to get the data before giving up. With every attempt data for a longer period is extracted
delayBetweenAttempts = int(os.environ["delayBetweenAttempts"])   # measured in seconds
delayBetweenPings = int(os.environ["delayBetweenPings"])   # measured in seconds -> how often in normal times it should retrieve data
numLines = int(os.environ["numLinesInCSV"])
debugLevel = int(os.environ["debugLevel"])


# making sure that they make sense
assert (len(EE_token) >= 30)
assert (len(EE_url) >= 30)
assert (numAttempts >= 5)
assert (delayBetweenAttempts >= 0)
assert (delayBetweenPings >= 20)
assert (numLines >= 5000)

# testing only  - to be removed
if debugLevel >= 2:
    print("EE_token:", EE_token, " EE_url:", EE_url, " numAttempts:",numAttempts, " delayBetweenAttempts:",
           delayBetweenAttempts, " delayBetweenPings:", delayBetweenPings, "debugLevel", debugLevel)

# init
step = 0
stopFL = False
startFL = True
startNewDF = True
sleepEnabledFL = True
errorCount = 0

csv.register_dialect('pipes', lineterminator='\r\n', delimiter=',')

while  (not stopFL) and errorCount < numAttempts:

    if sleepEnabledFL:
        time.sleep(delayBetweenPings)
        step = step + 1
    else:
        time.sleep(delayBetweenAttempts)


    reqStr="{}/gws/wfs?authkey={}&service=WFS&version=1.1.0\
            &request=GetFeature&outputformat=csv&typeName=exactAIS:LVI\
            &cql_filter=ts_insert_utc>=dateFormat('yyyyMMddHHmmss',currentDate('-PT{}M{}S'))"\
            .format(EE_url, EE_token, 5 if startFL else 0, delayBetweenPings + delayBetweenAttempts*errorCount+15)

    if debugLevel > 0:
        print(reqStr)

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
            # This means something went wrong and although the request was received OK the server is complaining.
            print('error in response, status code: {}'.format(resp.status_code))
            errorCount = errorCount + 1
            sleepEnabledFL = True
        else:

            # everything is fine so get extracting

            try:

                csv_reader = csv.reader(resp.text.splitlines(), dialect='pipes')
                headr = next(csv_reader, None)
                if startNewDF :
                    df=pd.DataFrame([x for x in csv_reader], columns=headr)
                    startNewDF = False
                else:
                    df=df.append(pd.DataFrame([x for x in csv_reader], columns=headr)).drop_duplicates()
                startFL = False
                dflen = len(df)
                print(step,":",dflen)

                # reset error handling parameters
                sleepEnabledFL = True
                errorCount = 0
                if dflen > numLines:
                    dateTimeObj = datetime.now()
                    filePath = "/data/API/shipUpdates{}_{}_{}_{}_{}.csv"\
                        .format(dateTimeObj.year,
                                dateTimeObj.month,
                                dateTimeObj.day,
                                dateTimeObj.hour,
                                dateTimeObj.minute)
                    df.to_csv(filePath)
                    print("Saved to file:", filePath)
                    # save to s3
                    subprocess.Popen('aws s3 --profile alexS3 cp {}  s3://ungp-poc/API/updatesTest/'.format(filePath), shell = True)
                    print('{} Saved to S3'.format(filePath))
                    startNewDF = True

            except:
                print("Error in trying to extract the csv from requests or create DF! Skipping the chunk and trying the next one.")

