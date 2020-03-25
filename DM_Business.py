import json
import operator
from cassandra.cluster import Cluster
import csv
import pandas as pd


# assigning parent categories/ new categories to existing businesses
# validating for businessed which don't belong to any category ("None")
# Assigning category "Miscellaneous" (imputing) for missing categories i.e. "None" cases
def getNewCategories(categories, categoriesDict):
    newCat = []
    if categories is not None:
        for cat in categories.split(','):
            cat = cat.lstrip()
            if cat in categoriesDict:
                if filterSpecialChar(categoriesDict.get(cat)) not in newCat:
                    newCat.append(filterSpecialChar(categoriesDict.get(cat)))
    else:
        newCat.append("miscelleanous")
    return newCat


def filterSpecialChar(cat):
    cat = cat.replace("&", "and")
    cat = cat.replace("/", " ")
    cat = cat.replace("-", "")
    cat = cat.lower()
    return cat


def denormalizeData(file1):
    cluster = Cluster()
    session = cluster.connect('yelp_weather')

    # making a new category lookup dictionary
    df = pd.read_csv("/home/ec2-user/yelpdata/categories.csv")
    categoriesDict = {}
    size_df = (df['category']).size
    for i in range(0, size_df):
        categoriesDict[df.iloc[i]['category']] = filterSpecialChar(df.iloc[i]['new Category'])


    # populating all new categories to a list
    new_cat_list = (df.iloc[0:32, 4]).tolist()
    filter_cat_list = []
    for cat in new_cat_list:
        new_cat = filterSpecialChar(cat)
        filter_cat_list.append(new_cat)

    # extracting data necessary for the business model and populating the DB
    queryMain = "INSERT INTO business(business_id,name,latitude,longitude,stars,review_count,address"
    with open(file1) as business:
        business_data = json.load(business)
        for busi in business_data:
            values = []
            business_id = busi['business_id']
            latitude = busi['latitude']
            longitude = busi['longitude']
            stars = busi['stars']
            review_count = busi['review_count']
            name = busi['name']
            address = busi['address'] + " " + busi['city'] + " " + busi['state'] + " " + busi['postal_code']
            values.append(business_id)
            values.append(name)
            values.append(latitude)
            values.append(longitude)
            values.append(stars)
            values.append(review_count)
            values.append(address)
            categories = busi['categories']
            newCatg = getNewCategories(categories, categoriesDict)
            print(business_id, latitude, longitude, stars, review_count, name, categories, address, newCatg)
            subQuery = ""
            for new_cat in newCatg:
                subQuery = subQuery + ",cat_" + new_cat
                values.append("Yes")
            query = queryMain + subQuery.replace(" ", "_") + ') VALUES (' + ','.join(['?'] * (len(newCatg) + 7)) + ')'
            prepared = session.prepare(query)
            session.execute(prepared, values)


if __name__ == '__main__':
    file1 = '/home/ec2-user/yelpdata/business_modified.json'
    denormalizeData(file1)
