import pandas as pd
import operator
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('yelp_weather')

def join_review_business_weather(rb_file,session):
    cat_weather={}
    query="SELECT * FROM day_cat"
    weather= pd.DataFrame(session.execute(query, timeout=None)._current_rows)
    weather.columns=["code","day","cat"]
    weather['day'] = weather['day'].astype(str)
    rbdf=pd.read_csv(rb_file,header=0)
    #weather=pd.read_csv(weather_file,header=0)
    final=rbdf.join(weather.set_index(["code","day"]),on=["city_code","date"],how='inner')
    return final
def get_day_for_each_cat(final):
    cat_weather={}
    for index,row in final.iterrows():
        j=row["categories"]
        res = j.strip("']['").split("', '") 
        c=row["cat"]
        #if not math.isnan(c):
        for i in res:


            #print (c)
            #print (i)
            try:
                cat_weather[i][c]+=1
            except KeyError:
                cat_weather[i]={'R':0,'S':0,'W':0,'H':0}
                cat_weather[i]['R']+=1
    max_cat={}
    for i in cat_weather:
        max_cat[i]=max(cat_weather[i].items(), key=operator.itemgetter(1))[0]
    return max_cat
    
def persist_dic(dic,session):
	upsert="UPDATE busi_cat_day SET day = '{}' WHERE cat='{}'"
	for i in dic:
		session.execute(upsert.format(dic[i],i))

final=join_review_business_weather('business_review_joined.csv',session)
dic=get_day_for_each_cat(final)
persist_dic(dic,session)
