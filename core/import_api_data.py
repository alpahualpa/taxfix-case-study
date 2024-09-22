# Author: Nikita Silin
# Task: Taxfix Case Study
# Purpose: Loading and anonymising API data

import numpy as np
import pandas as pd
import requests
import sqlite3
from datetime import date as d
import time


# function to get rows_count quantity of rows from API
def request_fakeapi(rows_count=1000, retry=3):

    api_url = ('https://fakerapi.it/api/v2/persons?_quantity='
               + str(rows_count)
               + '&_gender=XXX&_birthday_start=1900-01-01')

    # api connection
    try:
        while retry > 0:
            retry -= 1
            r = requests.get(api_url)

        # connection check
            connection_code = r.status_code
            if (connection_code != 200) and (retry != 0):
                print('---API connection issue, waiting for 10 seconds and retry')
                time.sleep(10)
                continue
            elif (connection_code != 200) and (retry == 0):
                raise Exception('API connection error: ' + str(connection_code))
            else:
                r_json = r.json()
                if 'data' in r_json.keys():
                    r_data = r.json()['data']
                    return r_data
                else:
                    raise Exception('Wrong API response format: No data included')

    except Exception as err:
        print(f"Next error occurred: {err}")
        raise


# function to get all data required from API
def loop_from_fakeapi(ks=30):
    print('--Data request: Started')
    return_list = []

    for i in range(ks):
        i_list = request_fakeapi()
        return_list += i_list
        print('---Loaded ' + str(i+1) + 'K out of ' + str(ks) + 'K')

    print('--Data request: Finished')
    return return_list


# function to pack dataframe from list
def pack_dataframe(input_list):
    print('--Dataframe creation: Started')
    df = pd.json_normalize(input_list)
    print('--Dataframe creation: Finished')
    return df


# function to anonymise and generalise data
def anonymise_dataframe(input_df, columns_to_mask):
    print('--Data anonymisation: Started')

    # masking of the sensitive columns
    for column_name in columns_to_mask:
        input_df[column_name] = '****'

    # cleaning emails to keep only domain name
    input_df['email'] = input_df['email'].str.replace(r'.+@', '', regex=True)

    # parsing and generalisation of persons age
    input_df = input_df.assign(age = lambda x: (
        ((pd.to_datetime(d.today()) - pd.to_datetime(x['birthday'])) / np.timedelta64(365, 'D')//10)*10
        )
    )

    input_df['age'] = input_df['age'].astype(int).astype(str) + '-' + (input_df['age'].astype(int) + 10).astype(str)

    print('--Data anonymisation: Finished')
    return input_df


# general function to get and prepare API data
def get_data_from_fakeapi():
    sensitive_columns = [
        'firstname',
        'lastname',
        'phone',
        'gender',
        'website',
        'image',
        'address.street',
        'address.streetName',
        'address.buildingNumber',
        'address.city',
        'address.zipcode',
        'address.latitude',
        'address.longitude'
    ]

    data_listed = loop_from_fakeapi()

    data_dataframed = pack_dataframe(data_listed)

    data_anonymised = anonymise_dataframe(data_dataframed, sensitive_columns)

    return data_anonymised


# function to save dataframe to the table
def load_into_database(input_df):
    print('--Loading to database: Started')
    conn = sqlite3.connect("persons.db")

    cur = conn.cursor()
    cur.execute('''DROP TABLE IF EXISTS persons''')
    conn.commit()

    input_df.to_sql(name='persons', con=conn)

    conn.commit()

    conn.close()
    print('--Loading to database: Finished')


# general function to load data from API and save in database
def data_importer():
    print('-Data import: Started')
    api_df = get_data_from_fakeapi()

    load_into_database(api_df)
    print('-Data import: Finished')


if __name__ == '__main__':
    data_importer()
