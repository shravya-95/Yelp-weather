# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import boto3 
import botocore
import pandas as pd
from cassandra.cluster import Cluster
import os

def get_file(year):    
    ACCESS_KEY="ASIAXB57SYSJBGW44H5S"
    SECRET_KEY="VjvPJIF13R8kC00n8yZbLs4LNxrC6bZddwygdzp2"
    SESSION_TOKEN="FwoGZXIvYXdzENv//////////wEaDBR7+U4bjycY24/ebCK/AXxaTW4J5/c1K7Qkej4XP3/J/HDPZiReoWYyJFNqmTc4lbt4yzQFDAS2v8RBAMcP2Lts2KtpJ2xfCuWC3VN9gQkh0fVItqZ6YPFYnnVCoUHQdoDqg5mXayW68LDNEnJBhsr8JoyiOGN1jHQhIA44xQzuyLskl6dS3z/hdSVwMCBQB30B4zXh88QJ0cKWil7cdhRnRXdy6vAf9KaG0E0UVnul2VtNAF64ZQlR+3FwJ6Fbqhv5BykTqFQrBHzklBlCKIX+xfMFMi1lfeNIy7hOSjbWLSEL6juGwZi/OD3KsGMfjy2dQBbFNiSsW502XdnIF3cB3NQ="
    session = boto3.Session(    
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN,
    )
    s3=session.resource('s3')
    BUCKET_NAME='noaa-ghcn-pds'
    #year='2018'
    KEY='csv/'+year+'.csv'
    try:
        s3.Bucket(BUCKET_NAME).download_file(KEY, 'yearly/'+year+'.csv')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
    return year+'.csv'

def grab_row_and_insert2(file,codes,session):
    upsert="UPDATE day_cat SET cat = '{}' WHERE code='{}' AND day='{}'"
    cols=["code","date","tag","value","gar1","gar2","gar3","gar4"]
    for df in pd.read_csv(file, names=cols,header = None, chunksize=100000):
            #for all rainy days with PRCP>990
            for i,r in df.loc[(df["tag"]=='TMAX') & (df["value"]>300),("code","date")].iterrows():
                if r[0] in codes:
                    day=str(r[1])
                    day=day[:4]+"-"+day[4:6]+"-"+day[6:]
                    session.execute(upsert.format('H',r[0],day))
            for i,r in df.loc[(df.tag=='AWND') & (df.value>32) ,("code","date")].iterrows():
                if r[0] in codes:
                    day=str(r[1])
                    day=day[:4]+"-"+day[4:6]+"-"+day[6:]
                    session.execute(upsert.format('W',r[0],day))
            for i,r in df.loc[(df.tag=='SNOW') & (df.value>127) ,("code","date")].iterrows():
                if r[0] in codes:
                    day=str(r[1])
                    day=day[:4]+"-"+day[4:6]+"-"+day[6:]
                    session.execute(upsert.format('S',r[0],day))
            for i,r in df.loc[(df.tag=='PRCP') & (df.value>990) ,("code","date")].iterrows():
                if r[0] in codes:
                    day=str(r[1])
                    day=day[:4]+"-"+day[4:6]+"-"+day[6:]
                    session.execute(upsert.format('R',r[0],day))
    os.remove(file)
def get_codes(file):
    codes={}
    with open(file) as fp:
        for line in fp:
            line=line.split(",")
            codes[line[0]]=True
    return codes

cluster = Cluster()
session = cluster.connect('bigdata')
for year in range(2010,2018):
    grab_row_and_insert2(get_file(year),get_codes('<path>'),session)