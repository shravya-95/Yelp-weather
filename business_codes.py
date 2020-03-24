import pandas as pd
import numpy as np

business_file = "//Users//mkr4014//Desktop//Data for DB//yelp_dataset//Data//business.json"

#cities and their weather station codes extracted from weather data
city_codes = "//Users//mkr4014//Desktop//Data for DB//yelp_dataset//code_city.csv"
business = pd.read_json(business_file,lines=True)
codes = pd.read_csv(city_codes)
cities_in_busi = list(business['city'])
cities_in_codes = list(codes['City'])[2:]
codes_in_codes = list(codes['Code'])[2:]
code =[] 
for city in cities_in_busi:
    try:
        code.append(codes_in_codes[cities_in_codes.index(city.upper())])
    except ValueError:
        code.append('NA') #if no match found, use 'NA' for code
code = np.array(code)
business["city_code"] = code #add code column
business = business[['business_id','categories','city_code']] #modify business to retrieve only required columns 
business.to_json(r"//Users//mkr4014//Desktop//Data for DB//yelp_dataset//Data//new_business.json",orient='records',lines=True)
