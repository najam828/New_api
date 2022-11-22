from flask import Flask
from flask import render_template, request
from flask_apscheduler import APScheduler
import pandas as pd
from math import cos, asin, sqrt
import numpy as np
import pyodbc
import time
class Config:
    SCHEDULER_API_ENABLED = True
app = Flask(__name__)
schedular = APScheduler()
schedular.init_app(app)
schedular.start()
# def save_data():
#     save_csv()
#     print('Job 1 executed')
# @schedular.task('cron',id='job_1',minute = 10, misfire_grace_time=3600)
# def save_data():
#     save_csv()
#     print('Job 1 executed')
# INTERVAL_TASK_ID = 'interval-task-id'
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=KHI-ITECK-GIS10\SQLEXPRESS;'
                      'Database=Service;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
def load():
    global df
    # conn = pyodbc.connect('Driver={SQL Server};'
    #                   'Server=KHI-ITECK-GIS10\SQLEXPRESS;'
    #                   'Database=Service;'
    #                   'Trusted_Connection=yes;')
    # cursor = conn.cursor()
    cursor.execute("SELECT Name,Latitude,Longitude,Street,City,Province FROM Points WITH (NOLOCK);")
    df = cursor.fetchall()
    a = []
    b = []
    c = []
    d = []
    e = []
    f = []
    for i in df:
        a.append(i[0])
        b.append(i[1])
        c.append(i[2])
        d.append(i[3])
        e.append(i[4])
        f.append(i[5])
    # val = [a,b,c,d,e,f]
    value = {}
    value['name'] = a
    value['latitude'] = b
    value['longitude'] = c
    value['street'] = d
    value['city'] = e
    value['province'] = f
    df = pd.DataFrame(value)
    return df
def distance(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295
    hav = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p)*cos(lat2*p) * (1-cos((lon2-lon1)*p)) / 2
    di = 12742 * asin(sqrt(hav))
    return di
def closest(data, lat,long):
    p = min(data.values, key=lambda p: distance(lat,long,p[1],p[2]))
    return p , round(distance(p[1],p[2],lat,long),1)
def calculator(df,latitude,longitude):
    # coordinates = str(input('Coordinates: '))
    # check = ','
    # if check in coordinates:
    #     data = coordinates.split(',')
    # else:
    #     data = coordinates.split(', ')
    # lat,long = float(data[0]),float(data[1])
    lat = float(latitude)
    long = float(longitude)
    val = 0.0005
    lat_a = lat - val
    lon_a = long - val
    lat_b = lat + val
    lon_b = long + val
    start_time = time.time()
    test = df[(df['latitude']>lat_a)&(df['latitude']<lat_b)&(df['longitude']>lon_a)&(df['longitude']<lon_b)]
    if test.empty:
        for j in range(0,25):
            lat_a = lat_a - val
            lon_a = lon_a - val
            lat_b = lat_b + val
            lon_b = lon_b + val
            test = df[(df['latitude']>lat_a)&(df['latitude']<lat_b)&(df['longitude']>lon_a)&(df['longitude']<lon_b)]
            # print(j)
            if test.empty:
                continue
            else:
                break
    if test.empty:
        print('not found')
        print("--- %s seconds ---" % (time.time() - start_time)," Process complete")
    else:
        # print(test)
        poi , dist= closest(test,lat,long)
        if len(poi[0]) >= 25:
            print("{0} km away from {1}.".format(dist,poi[0]))
        else:
            if (poi[0] is not np.nan)&(poi[3] is np.nan)&(poi[4] is np.nan)&(poi[5] is np.nan):
                print("{0} km away from {1}.".format(dist,poi[0]))
            elif (poi[0] is not np.nan)&(poi[3] is not np.nan)&(poi[4] is np.nan)&(poi[5] is np.nan):
                print("{0} km away from {1}, {2}.".format(dist,poi[0],poi[3]))
            elif (poi[0] is not np.nan)&(poi[3] is np.nan)&(poi[4] is not np.nan)&(poi[5] is np.nan):
                print("{0} km away from {1}, {2}.".format(dist,poi[0],poi[4]))
            elif (poi[0] is not np.nan)&(poi[3] is np.nan)&(poi[4] is np.nan)&(poi[5] is not np.nan):
                print("{0} km away from {1}, {2}.".format(dist,poi[0],poi[6]))
            elif (poi[0] is not np.nan)&(poi[3] is not np.nan)&(poi[4] is not np.nan)&(poi[5] is np.nan):
                print("{0} km away from {1}, {2}, {3}.".format(dist,poi[0],poi[3],poi[4]))
            elif (poi[0] is not np.nan)&(poi[3] is not np.nan)&(poi[4] is np.nan)&(poi[5] is not np.nan):
                print("{0} km away from {1}, {2}, {3}.".format(dist,poi[0],poi[3],poi[5]))
            elif (poi[0] is not np.nan)&(poi[3] is np.nan)&(poi[4] is not np.nan)&(poi[5] is not np.nan):
                print("{0} km away from {1}, {2}, {3}.".format(dist,poi[0],poi[4],poi[5]))
            elif (poi[0] is not np.nan)&(poi[3] is not np.nan)&(poi[4] is not np.nan)&(poi[5] is not np.nan):
                print("{0} km away from {1}, {2}, {3}, {4}.".format(dist,poi[0],poi[3],poi[4],poi[5]))
        print("--- %s seconds ---" % (time.time() - start_time)," Process complete")
def add_row(row):
    global df
    new_row = {}
    keys = ['name','latitude','longitude','addr:street','addr:city','addr:province']
    for i in keys:
        if i == 'name':
            new_row[i] = row[0]
        elif i == 'latitude':
            new_row[i] = row[1]
        elif i == 'longitude':
            new_row[i] = row[2]
        elif i == 'addr:street':
            new_row[i] = row[3]
        elif i == 'addr:city':
            new_row[i] = row[4]
        elif i == 'addr:province':
            new_row[i] = row[5]
    value = pd.DataFrame(new_row,index = [0])
    print(value)
    df = pd.concat([df,value], axis = 0,ignore_index = True)
    print(df.tail())
    return df
def add_data(row):
    # conn = pyodbc.connect('Driver={SQL Server};'
    #                   'Server=KHI-ITECK-GIS10\SQLEXPRESS;'
    #                   'Database=Service;'
    #                   'Trusted_Connection=yes;')
    # cursor = conn.cursor()
    update = row
    cursor.execute("""SELECT TOP 1 id FROM Points  
        ORDER BY id DESC;""")
    value = cursor.fetchall()
    values = value[0][0]+1
    cursor.execute("""
        INSERT INTO Points (id, Name, Latitude, Longitude, Street, City, Province)
        VALUES(?,?,?,?,?,?,?); """,
        values,str(update[0]),update[1],update[2],str(update[3]),str(update[4]),str(update[5]))
    cursor.commit()
    print(value)
def delete_data(val):
    cursor.execute("DELETE FROM Points WHERE Name = (?)",val)
def delete_csv(val):
    drop = df[df['name'] == val].index
    df.drop(drop,inplace=True)
# schedular.add_job(id = 'job_1',func=save_csv, trigger = 'cron', minute = 5)
@app.route("/")
def nav():
    return render_template('test.html')
@app.route("/load")
def home():
    global data
    data = load()
    return "data loaded"
@app.route("/results", methods = ["GET"])
def get_result():
    if request.method == "GET":
        latitude = request.args.get('lat')
        longitude = request.args.get('long')
        result = calculator(data,latitude,longitude)
        print(result)
        return 'successful'
@app.route("/form", methods = ["GET" , "POST"])
def add_value():
    if request.method == "POST":
        name = request.form.get("Name")
        latitude = request.form.get("Latitude")
        latitude = request.form.get("Longitude")
        street = request.form.get("Street")
        city = request.form.get("City")
        province = request.form.get("Province")
        value = [name,latitude,latitude,street,city,province]
        add_data(value)
        add_row(value)
        # return 'value added'
    return render_template('form.html')
@app.route("/find")
def find():
    return render_template('find.html')
@app.route("/search")
def search():
    global val
    Name = request.args.get('Name')
    # value = df['name'].values
    cursor.execute('SELECT Name FROM Points WITH (NOLOCK);')
    value = cursor.fetchall()
    # search = [x for x in value if x[0].startswith(Name)]
    search = [x for x in value if x[0] == Name]
    if len(search) > 0:
        # val = df[df['name'] == search]
        val = df[df['name'] == search[0][0]]
        val = val.to_dict('index')
            # df = [z for z in data.values if z[0] == search[0]]
            # df = sorted(data.values, key=lambda p: (if p[0] == search[0]))
        # elif len(search) == 1:
        #     delete_data(search)
    else:
        val = 'not found'
    return render_template('data.html',val=val)
@app.route("/delete", methods = ["GET","POST"])
def delete():
    dat = val
    if request.method == "POST":
        value = request.form.get('data')
        value = int(value)
        print(value)
        print(type(value))
        val_1 = dat[value]
        print(value,val_1['name'])
        delete_data(val_1['name'])
        delete_csv(val_1['name'])
    return render_template('delete.html',dat=dat)
        



# @app.route("/save_data")
# def save_data():
#     save_csv()
#     return 'csv saved'


# @app.route('/')
