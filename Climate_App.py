#------------------------------------
#Fervis Lauan               2017-Oct
#API App using SQLAlchemy-ORM
#------------------------------------
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy.sql import select
from sqlalchemy import desc

from datetime import date
from datetime import timedelta

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime as datetime

from flask import Flask, jsonify

engine = create_engine('mysql://b5kj3n966clep7oc:wae1501lpo2yoilq@ehc1u4pmphj917qf.cbetxkdyhwsb.us-east-1.rds.amazonaws.com:3306/et3812lmc7w8mzbn', echo=False)
Base = automap_base()
Base.prepare(engine, reflect=True)
Base.classes.keys()

measurements_tb = Base.classes.measurements
stations_tb=Base.classes.stations

session = Session(engine)

app = Flask(__name__)

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start date <br/>"
        f"/api/v1.0/start date/end date"
    )

@app.route("/api/v1.0/precipitation")
def precip():
    #Get the most active station
    qry1=select([measurements_tb.station,func.count(measurements_tb.station).label("rec_count")]).group_by(measurements_tb.station).order_by(desc("rec_count"))
    most_actv=engine.execute(qry1).first()

    #Select the last 12 months of precipitation
    qry2 = session.query(func.max(measurements_tb.measure_date)).\
            filter(measurements_tb.station==most_actv[0]).all()
    mxdt_list=[x[0] for x in qry2]         
    mxdt=mxdt_list[0]
    start_dt=datetime.datetime.strptime(str(mxdt), '%Y-%m-%d').date() - timedelta(days=365)
    end_dt=datetime.datetime.strptime(str(mxdt), '%Y-%m-%d').date()
    

    precip_12mon = session.query(measurements_tb).filter(measurements_tb.station==most_actv[0],measurements_tb.measure_date>=start_dt,measurements_tb.measure_date<=end_dt).order_by(measurements_tb.measure_date).all()
    
    precip_lst = []
    for row in precip_12mon:
        precip_dict = {}
        precip_dict["station"] = row.station
        precip_dict["measure_date"] = row.measure_date
        precip_dict["precip"] = row.precip        
        precip_lst.append(precip_dict)

    return jsonify(precip_lst)

@app.route("/api/v1.0/stations")
def station():
    stations = session.query(stations_tb).all()

    station_lst = []
    for row in stations:
        station_dict = {}
        station_dict["station"] = row.station
        station_dict["station_name"] = row.station_name
        station_dict["latitude"] = row.latitude
        station_dict["longitude"] = row.longitude
        station_dict["elevation"] = row.elevation
        station_lst.append(station_dict)

    return jsonify(station_lst)




@app.route("/api/v1.0/tobs")
def tobs():
    #Get the most active station
    qry1=select([measurements_tb.station,func.count(measurements_tb.station).label("rec_count")]).group_by(measurements_tb.station).order_by(desc("rec_count"))
    most_actv=engine.execute(qry1).first()

    #Select the last 12 months of precipitation
    qry2 = session.query(func.max(measurements_tb.measure_date)).\
            filter(measurements_tb.station==most_actv[0]).all()
    mxdt_list=[x[0] for x in qry2]         
    mxdt=mxdt_list[0]
    start_dt=datetime.datetime.strptime(str(mxdt), '%Y-%m-%d').date() - timedelta(days=365)
    end_dt=datetime.datetime.strptime(str(mxdt), '%Y-%m-%d').date()

    tobs_12mon = session.query(measurements_tb).filter(measurements_tb.station==most_actv[0],measurements_tb.measure_date>=start_dt,measurements_tb.measure_date<=end_dt).order_by(measurements_tb.measure_date).all()
    
    tobs_lst = []
    for row in tobs_12mon:
        tobs_dict = {}
        tobs_dict["station"] = row.station
        tobs_dict["measure_date"] = row.measure_date        
        tobs_dict["tobs"] = row.tobs
        tobs_lst.append(tobs_dict)

    return jsonify(tobs_lst)


@app.route("/api/v1.0/<start>")
def startonly(start):
    
    
    start_dt=datetime.datetime.strptime(start, '%Y-%m-%d').date()
        
    qry2 = session.query(func.min(measurements_tb.tobs).label("mintemp"),func.avg(measurements_tb.tobs).label("avgtemp"),func.max(measurements_tb.tobs).label("maxtemp")).\
            filter(measurements_tb.measure_date>=start_dt).all()
        
    temp_lst=[]
    for row in qry2:
        temp_dict={}   
        temp_dict["Min Temperature"]=row.mintemp
        temp_dict["Average Temperature"]=str(row.avgtemp)
        temp_dict["Max Temperature"]=row.maxtemp
        temp_lst.append(temp_dict)

    return jsonify(temp_lst)


@app.route("/api/v1.0/<start>/<end>")
def startend(start,end):

    #Calculate start and end dates    
    start_dt=datetime.datetime.strptime(start, '%Y-%m-%d').date()
    end_dt=datetime.datetime.strptime(end, '%Y-%m-%d').date()
    
    #Get the most active station
    #qry=select([measurements_tb.station,func.count(measurements_tb.station).label("rec_count")]).\
    #group_by(measurements_tb.station).order_by(desc("rec_count"))
    #most_actv=engine.execute(qry).first()
    
    qry2 = session.query(func.min(measurements_tb.tobs).label("mintemp"),func.avg(measurements_tb.tobs).label("avgtemp"),func.max(measurements_tb.tobs).label("maxtemp")).\
            filter(measurements_tb.measure_date>=start_dt,measurements_tb.measure_date<=end_dt).all()
        
    temp_lst=[]
    for row in qry2:
        temp_dict={}   
        temp_dict["Min Temperature"]=row.mintemp
        temp_dict["Average Temperature"]=str(row.avgtemp)
        temp_dict["Max Temperature"]=row.maxtemp
        temp_lst.append(temp_dict)

    return jsonify(temp_lst)

if __name__ == '__main__':
    app.run(debug=True)