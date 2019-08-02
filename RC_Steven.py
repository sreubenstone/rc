import requests
import configuration
import json
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile


record1 = {
    "record_id": 63454353453,
    "redcap_repeat_instrument": "energy_drinks",
    "drink": 3,
    "first_name": "Jon",
    "last_name": "Hamper"
}

records = [record1]


class STEVEN_RC(object):

    def __init__(self, **kwargs):
        self.url = kwargs.get("url")
        output_format = kwargs.get("format")
        if output_format is not None and output_format in ["csv", "json"]:
            self.format = output_format
        else:
            self.format = "json"

    def fetch_data_dictionary(self, token):
        payload = {
            "token": token,
            "content": "metadata",
            "format": self.format,
            "returnFormat": self.format,
        }
        response = requests.post(self.url, data=payload)
        if response.status_code != 200:
            raise Exception(
                "REDCap responded with status code {0}: {1}".format(
                    response.status_code, response.text
                )
            )
        return response.json()

    def fetch_project_info(self, token):
        payload = {
            "token": token,
            "content": "project",
            "format": self.format,
            "returnFormat": self.format,
        }
        r = requests.post(self.url, data=payload)
        if r.status_code != 200:
            raise Exception(
                "REDCap responded with status code {0}: {1}".format(
                    r.status_code, r.text
                )
            )
        return r.json()

    def _df_to_dicts(self, df):
        """
        Turns a DataFrame into a list of dict objects for keys that have a non-empty value
        :param df:      DataFrame - input DataFrame
        :return:        list - list of dict objects
        """
        data = list()
        cols = df.columns
        for row in df.itertuples(index=False):
            temp = dict()
            for i in range(len(cols)):
                if row[i] != '':
                    temp[cols[i]] = row[i]

            data.append(temp)

        return data

    def import_records(self, token, records):
        payload = {
            "token": token,
            "content": "record",
            "format": self.format,
            "type": "flat",
            "overwriteBehavior": "overwrite",
            "forceAutoNumber": "true",
            "data": json.dumps(records),
            "dateFormat": "MDY",
            "returnContent": "count",
            "returnFormat": self.format,
        }
        r = requests.post(self.url, data=payload)
        if r.status_code not in [200, 400]:
            raise Exception(
                "REDCap responded with status code {0}: {1}".format(
                    r.status_code, r.text
                )
            )
        return r.json()

    def import_records_excel(self, token, file):
        df = pd.read_excel(file)
        df.fillna("", inplace=True)
        dict = self._df_to_dicts(df)
        print(dict)
        payload = {
            "token": token,
            "content": "record",
            "format": self.format,
            "type": "flat",
            "overwriteBehavior": "overwrite",
            "forceAutoNumber": "true",
            "data": json.dumps(dict),
            "dateFormat": "MDY",
            "returnContent": "count",
            "returnFormat": self.format,
        }
        r = requests.post(self.url, data=payload)
        if r.status_code not in [200, 400]:
            raise Exception(
                "REDCap responded with status code {0}: {1}".format(
                    r.status_code, r.text
                )
            )
        return r


# Testing
rc = STEVEN_RC(
    url='https://redcaptest.mskcc.org/api/', format='json')


response = rc.import_records_excel(
    configuration.redcap_token, '/Users/stevenreubenstone/Desktop/test_data.xlsx')


print(response.text)
