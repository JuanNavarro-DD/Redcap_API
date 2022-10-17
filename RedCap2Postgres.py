"""
 # @ Author: Juan Navarro
 # @ Create Time: 17-10-2022 13:59:01
 # @ Modified by: Juan Navarro
 # @ Modified time: 17-10-2022 13:59:40
 # @ Description: Takes data from redcap and insert it into a postgres DB in AWS RDS
 """


import requests
import pandas as pd
import psycopg2
import json

with open("database.json") as db_file:
    db_info = json.load(db_file)


with open("token.txt") as token_do:
    token = token_do.readlines()[0]

data = {
    "token": token,
    "content": "record",
    "action": "export",
    "format": "json",
    "type": "flat",
    "csvDelimiter": "",
    "rawOrLabel": "raw",
    "rawOrLabelHeaders": "raw",
    "exportCheckboxLabel": "false",
    "exportSurveyFields": "false",
    "exportDataAccessGroups": "false",
    "returnFormat": "json",
}
r = requests.post("https://redcapdemo.vanderbilt.edu/api/", data=data)

if r.status_code == 200:
    data = pd.DataFrame(r.json())

    connection = psycopg2.connect(**db_info)
    cursor = connection.cursor()

    data2insert = [
        (row.lab_id, row.first_name, row.last_name, int(row.age.zfill(2)))
        for _, row in data.iterrows()
    ]
    values = str(data2insert).replace("[", "").replace("]", "")

    sql = f'INSERT INTO "Test_schema"."Test_table" VALUES {values} ON CONFLICT DO NOTHING;'
    cursor.execute(sql)
    connection.commit()
    cursor.close()
    connection.close()
