import pandas as pd

#joining businesses mapped to new categories to modified reviews to get intermediate dataset 
business_new_file = "//Users//mkr4014//Desktop//Data for DB//yelp_dataset//Data//new_business_newCat.json"
new_review_file = "//Users//mkr4014//Desktop//Data for DB//yelp_dataset//Data//New_review.csv"
nb = pd.read_json(business_new_file,lines=True)
nr = pd.read_csv(new_review_file)
rbj = nr.join(nb.set_index('business_id'),on='business_id')

rbj.to_csv("//Users//mkr4014//Desktop//Data for DB//yelp_dataset//Data//business_review_joined.csv",index=False)
print("Finished Joining!")
