import json
import operator
import csv 
import pandas as pd 

# assigning parent categories/ new categories to existing businesses
# validating for businesses which don't belong to any category ("None")
# Assigning category "Miscellaneous" (imputing) for missing categories i.e. "None" cases
def getNewCategories(categories,categoriesDict):
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
    cat = cat.replace("&","and")
    cat = cat.replace("/"," ")
    cat = cat.replace("-","")
    cat = cat.lower()
    return cat
    
def denormalizeData(infile,outfile):
    
    # making a new category lookup dictionary
    df = pd.read_csv("/Users/mkr4014/Desktop/Data for DB/categories.csv")
    categoriesDict = {}
    size_df = (df['category']).size
    for i in range(0, size_df):
        categoriesDict[df.iloc[i]['category']] = filterSpecialChar(df.iloc[i]['new Category'])
        
    # populating all new categories to a list
    new_cat_list = (df.iloc[0:32,4]).tolist()
    filter_cat_list = []
    for cat in new_cat_list:
        new_cat = filterSpecialChar(cat)
        filter_cat_list.append(new_cat)
    
    
    business_data = []
    with open(infile) as business:
        for line in business:
            business_data.append(json.loads(line))
        for busi in business_data:
            categories = busi['categories']
            newCatg = getNewCategories(categories,categoriesDict)
            busi['categories'] = newCatg
    bd = pd.DataFrame(data=business_data)
    bd = bd[['business_id','categories','city_code']]
    bd.to_json(outfile,orient='records',lines=True)
    print(bd.head())
    
file1 = "//Users//mkr4014//Desktop//Data for DB//yelp_dataset//Data//new_business.json"
file2 = "//Users//mkr4014//Desktop//Data for DB//yelp_dataset//Data//new_business_newCat.json"
denormalizeData(file1,file2)
