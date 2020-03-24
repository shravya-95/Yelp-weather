from cassandra.cluster import Cluster
import numpy as np
import pandas as pd


if __name__ == "__main__":

    catdict ={}
    catlist = ['Food','Shopping', 'Home Services', 'Beauty & Spas', 'Health & Medical', 'Bars', 'Local Services', 'Nightlife', 'Automotive', 'Professional Services', 'Arts & Entertainment', 'Hotels & Travel', 'Financial Services', 'Flowers & Gifts', 'Clothing & accessories', 'Electronics', 'Community Service/Non-Profit', 'Public Services & Government', 'Laundry Services', 'Transportation', 'Lawyers', 'Electricians', 'Religious Organizations', 'Miscelleanous']
    catlist2 = ['food', 'shopping', 'home_services','beauty_and_spas', 'health_and_medical', 'bars','local_services', 'nightlife', 'automotive','professional_services', 'arts_and_entertainment','hotels_and_travel', 'financial_services','flowers_and_gifts', 'clothing_and_accessories','electronics','community_service_non_profit','public_services_and_government', 'laundry_services', 'transportation', 'lawyers', 'electricians' , 'religious_organizations', 'miscellaneous']
    count = 1
    for cat in catlist:
        catdict[cat] = count
        count = count+1
    print("Categories:")
    print(catdict)
    try:
        x =int(input("Choose the number of category 1:"))-1
        cat12 = catlist2[x]
        cat1 = catlist[x]
    except IndexError:
        print("Invalid Number chosen. Retry the program!")
        exit()
    
    try:
        y = int(input("Choose the number of category 2:"))-1
        cat22 = catlist2[y]
        cat2 = catlist[y]
    except IndexError:
        print("Invalid Number chosen. Retry the program!")
        exit()
    cluster = Cluster(['127.0.0.1'],port=9042)
    session = cluster.connect('yelp_weather',wait_for_all_pools=True)
    query = "SELECT "+cat12+","+cat22+" from user_category;"
    df = pd.DataFrame(list(session.execute(query)))
    a =np.array(df[cat12]!=0).astype(int)
    b = np.array(df[cat22]!=0).astype(int)
    denom=np.sum(a)
    nume = np.sum(a*b)
    probability = nume*100/denom
    print("The probability of reviewing "+cat2+" if "+cat1+" is reviewed by the user is: "+str(np.round(probability,decimals=2))+"%")
