import json
import pandas as pd

dat=[]
file = "//Users//mkr4014//Desktop//Data for DB//yelp_dataset//Data//review.json"
#Open file line by line to reduce pressure on memory
with open(file) as f:
    for line in f:
        dat.append(json.loads(line))

df = pd.DataFrame(data=dat)

#Retrieve only required columns
df = df[['review_id','business_id','date']]

#Convert Date-time to date only to match with format in Weather data
df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

df.to_csv("//Users//mkr4014//Desktop//Data for DB//yelp_dataset//Data//New_review.csv",index=False)
