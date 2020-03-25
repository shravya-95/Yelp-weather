from haversine import haversine, Unit
from cassandra.cluster import Cluster
import pandas as pd
import gmplot


def plot(latitude, longitude, input_latitude, input_longitude):
    latitude_list = latitude
    longitude_list = longitude
    gmap3 = gmplot.GoogleMapPlotter(input_latitude, input_longitude, 13)
    gmap3.scatter(latitude_list, longitude_list, '# FF0000', size=40, marker=False)
    gmap3.plot(latitude_list, longitude_list, 'cornflowerblue', edge_width=2.5)
    gmap3.draw("/home/ec2-user/yelpdata/map.html")


def validate_greaterThan4(df):
    if df.shape[0] > 4:
        return True
    else:
        return False


def validate_emptyDF(df):
    return df.empty


# 2nd round of filter(distance based) on the filtered data retrieved from database
def top5bs(filter_data, input_latitude, input_longitude, input_distance, input_review_count):
    points = []
    latitude = []
    longitude = []
    for data in filter_data:
        lat = data[3]
        lon = data[4]
        dest = (lat, lon)
        src = (input_latitude, input_longitude)
        dist = haversine(src, dest, unit=Unit.MILES)
        if dist < input_distance:  # checks if the calculated distance is less than the input given by user
            value = []
            value.append(data[3])  # latitude
            value.append(data[4])  # longitude
            value.append(data[1])  # business name
            value.append(data[2])  # address
            value.append(data[5])  # stars
            value.append(data[6])  # review count
            points.append(value)
    df = pd.DataFrame(points, columns=['Lat', 'Lon', 'BusinessName', 'Address', 'Stars', 'ReviewCount'])
    check1 = validate_emptyDF(df)
    if check1 == False:
        df = df.loc[(df['ReviewCount'] >= input_review_count)]
        check2 = validate_greaterThan4(df)
        if check2 == True:
            df = df.sort_values(by=['Stars'], ascending=False).head(5)
            latitude = (df.iloc[:, 0]).values.tolist()
            longitude = (df.iloc[:, 1]).values.tolist()
            plot(latitude, longitude, input_latitude, input_longitude)
            print(df)
        else:
            print("Try another input combination!!, Results returned less than 5 after review count filter!!")
    else:
        print("Try another input combination!! Result after distance filtering is null!!")


# creates index if it doesn't already exist on the category we choose to filter from
def create_index(input_category):
    cluster = Cluster()
    session = cluster.connect('yelp_weather')
    query = "CREATE INDEX IF NOT EXISTS " + input_category + "_idx ON business (cat_" + input_category + ")"
    print(query)
    prepared = session.prepare(query)
    session.execute(prepared)


# filtering business on the basis of category
def filter_category(input_category, input_review_count):
    cluster = Cluster()
    session = cluster.connect('yelp_weather')
    create_index(input_category)
    query = "SELECT business_id, name, address, latitude, longitude, stars, review_count FROM business WHERE cat_" + input_category + "='Yes'"
    print(query)
    prepared = session.prepare(query)
    session.execute(prepared)
    result = session.execute(prepared)
    return result


if __name__ == '__main__':
    input_latitude = float(input("Enter latitude: "))  # 35.26274
    input_longitude = float(input("Enter longitude: "))  # -81.12589
    input_distance = float(input("Enter distance in miles: "))  # 0.50
    input_category = str(input("Enter category: "))  # food
    input_review_count = int(input("Review count: "))  # 12
    filter_data = filter_category(input_category, input_review_count)
    filter_data_check = filter_data
    top5bs(filter_data, input_latitude, input_longitude, input_distance, input_review_count)

