import http.client
import urllib.request, urllib.parse, urllib.error, base64
from io import BytesIO
import pandas as pd
import requests
import os
import time

class PjmData:

    def __init__(self):

        """ Set the headers with the provided API key """
        self.API_KEY = 'subscription-key=a1b7873797054a1493126b03e6f5db37'
        self.BASE_URL = 'https://api.pjm.com/api/v1/'

    def _do_pull(self, endpoint_url, **kwargs):

        """ Change directories to the folder than will store all of the data """
        save_dirs = {"act_sch_interchange?": '/Users/schredder/PycharmProjects/schredder/data/tie_flows'}
        os.chdir(save_dirs[endpoint_url])

        """ Set the params for the data pull """
        params = {
            'download': True,
            'startRow': 1}

        """ Make start and end dates if they are in the kwargs """
        if 'start_date' in kwargs:

            start_date = pd.to_datetime(kwargs.get('start_date'))

            if 'end_date' not in kwargs:
                end_date = start_date # +pd.to_timedelta('1d')
            else:
                end_date = pd.to_datetime(kwargs.get('end_date'))

            params.update({'datetime_beginning_ept': f'{start_date:%Y-%m-%dT00:00}to{end_date:%Y-%m-%dT23:59:59}'})

            ## If the start date is more than 2 years in the past, change to non active meta data
            if start_date <= pd.to_datetime('today') - pd.to_timedelta('630d'):
                params.update({'isActiveMetadata=': False})
            else:
                params.update({'isActiveMetadata=': True})

        params = ''.join([f'{k}={v}&' for k, v in params.items()])
        url = f'{self.BASE_URL}{endpoint_url}{params}&{self.API_KEY}'

        save_name = f"{start_date:%Y%m%d}_{endpoint_url.replace('?', '')}.csv"

        if save_name in os.listdir():
            return

        try:
            response = requests.get(url)
            i = 0
            while response.status_code == 429:
                print(i)
                time.sleep(10)
                response = requests.get(url)
                i += 1

        except Exception as err:
            print(err)

            return

        try:
            data = pd.DataFrame.from_dict(response.json())
        except:
            print(response)
            return



        for col in data.columns:
            if 'datetime' in col:
                data.loc[:, col] = pd.to_datetime(data[col])

        data.to_csv(save_name, index=None)
        print(start_date)
        return data

    def tie_flows(self, **kwargs):

        endpoint_url = "act_sch_interchange?"

        data = self._do_pull(endpoint_url, **kwargs)
        if data is None:
            return
        else:
            self.tie_flow_data = data

pjm_data = PjmData()

for date in pd.date_range('01-01-2014', pd.to_datetime('today'), freq='d'):
    pjm_data.tie_flows(start_date=date)