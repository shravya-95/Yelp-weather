# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
from get_codes import get_codes
from grab_row_and_insert import grab_row_and_insert
import boto3 
import botocore
import pandas as pd
from cassandra.cluster import Cluster
import os

def get_file(year):    
    ACCESS_KEY=""
    SECRET_KEY=""
    SESSION_TOKEN=""
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


cluster = Cluster()
session = cluster.connect('bigdata')
for year in range(2010,2018):
    grab_row_and_insert(get_file(year),get_codes('<path>'),session)
