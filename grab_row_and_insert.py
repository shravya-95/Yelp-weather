import pandas as pd
from cassandra.cluster import Cluster
import os

def grab_row_and_insert(file,codes,session):
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
