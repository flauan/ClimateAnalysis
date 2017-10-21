

```python
#-----------------------------------
#Fervis Lauan                2017-10
#Data Analysis Using SQLAlchemy-ORM
#-----------------------------------
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

```


```python
engine = create_engine('mysql://b5kj3n966clep7oc:wae1501lpo2yoilq@ehc1u4pmphj917qf.cbetxkdyhwsb.us-east-1.rds.amazonaws.com:3306/et3812lmc7w8mzbn', echo=False)
```


```python
Base = automap_base()
Base.prepare(engine, reflect=True)
Base.classes.keys()
```




    ['measurements', 'stations']




```python
measurements_tb = Base.classes.measurements
stations_tb=Base.classes.stations
```


```python
session = Session(engine)
```

# Precipitation Analysis


```python
#Get the most active station
qry=select([measurements_tb.station,func.count(measurements_tb.station).label("rec_count")]).\
    group_by(measurements_tb.station).order_by(desc("rec_count"))
    
most_actv=engine.execute(qry).first()

#Select the last 12 months of precipitation
qry2 = session.query(func.max(measurements_tb.measure_date)).\
        filter(measurements_tb.station==most_actv[0]).all()
mxdt_list=[x[0] for x in qry2]         
mxdt=mxdt_list[0]
start_dt=datetime.datetime.strptime(str(mxdt), '%Y-%m-%d').date() - timedelta(days=365)
end_dt=datetime.datetime.strptime(str(mxdt), '%Y-%m-%d').date()
print(str(start_dt)+"    "+str(end_dt))

#Run Query
precip_12mon = session.query(measurements_tb).filter(measurements_tb.station==most_actv[0],measurements_tb.measure_date>=start_dt,measurements_tb.measure_date<=end_dt).order_by(measurements_tb.measure_date).all()

precip=[]
for row in precip_12mon:      
    precip.append((row.measure_date,int(row.precip*100),row.precip))    
                  
precip_df = pd.DataFrame(precip, columns=["Measure Date","Precipitation","decPrecip"])
precip_df.head()   
```

    2016-08-18    2017-08-18
    




<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Measure Date</th>
      <th>Precipitation</th>
      <th>decPrecip</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2016-08-18</td>
      <td>0</td>
      <td>0.00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2016-08-19</td>
      <td>31</td>
      <td>0.31</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2016-08-20</td>
      <td>13</td>
      <td>0.13</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2016-08-21</td>
      <td>8</td>
      <td>0.08</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2016-08-22</td>
      <td>231</td>
      <td>2.32</td>
    </tr>
  </tbody>
</table>
</div>




```python
plt.clf()
fig, ax = plt.subplots(figsize=(6,6))    
ax.plot(precip_df["Measure Date"],precip_df["Precipitation"],'b')
ax.set_xlabel("Date")
ax.set_ylabel("Precipitation")
ax.set_title("Precipitation Analysis from the most active Station")
fig.tight_layout()
plt.show()

```


    <matplotlib.figure.Figure at 0x2080375dcf8>



![png](output_7_1.png)


# Station Analysis


```python
#Number of stations
tot_station = session.query(func.count(stations_tb.station)).first()
print(tot_station[0])
```

    9
    


```python
#List the stations and observation counts in descending order
qry1=select([measurements_tb.station,func.count(measurements_tb.station).label("rec_count")]).\
            group_by(measurements_tb.station).order_by(desc("rec_count"))

result=engine.execute(qry1).fetchall()
for i in range(len(result)):  
      print(result[i])

```

    ('USC00519281', 2772)
    ('USC00513117', 2696)
    ('USC00519397', 2685)
    ('USC00519523', 2572)
    ('USC00516128', 2484)
    ('USC00514830', 1937)
    ('USC00511918', 1932)
    ('USC00517948', 683)
    ('USC00518838', 342)
    


```python
#Highest observation
high_obs=engine.execute(qry1).first()
print(high_obs)
```

    ('USC00519281', 2772)
    


```python
#Get most active station
qry=select([measurements_tb.station,func.count(measurements_tb.station).label("rec_count")]).\
group_by(measurements_tb.station).order_by(desc("rec_count"))   
most_actv=engine.execute(qry).first()

#Select the last 12 months of precipitation
qry2 = session.query(func.max(measurements_tb.measure_date)).\
        filter(measurements_tb.station==most_actv[0]).all()
mxdt_list=[x[0] for x in qry2]         
mxdt=mxdt_list[0]
start_dt=datetime.datetime.strptime(str(mxdt), '%Y-%m-%d').date() - timedelta(days=365)
end_dt=datetime.datetime.strptime(str(mxdt), '%Y-%m-%d').date()
print(str(start_dt)+"    "+str(end_dt))

tobs_12mon = session.query(measurements_tb).filter(measurements_tb.station==most_actv[0],measurements_tb.measure_date>=start_dt,measurements_tb.measure_date<=end_dt).order_by(measurements_tb.measure_date).all()

qry=select([measurements_tb.tobs]).order_by(measurements_tb.tobs)
result=engine.execute(qry).fetchall()    
tobs_list=[x[0] for x in result] 
```

    2016-08-18    2017-08-18
    


```python
plt.clf()
fig, ax = plt.subplots(figsize=(6,6))  
ax.hist(tobs_list,bins=12)
ax.set_ylabel("Frequency")
ax.set_title("Tobs Distribution from the most active Station")
fig.tight_layout()
plt.show()
```


    <matplotlib.figure.Figure at 0x20803c6f240>



![png](output_13_1.png)


# Temperature Analysis


```python
def calc_temps(p_start_dt,p_end_dt):
    #Calculate start and end dates    
    start_dt=datetime.datetime.strptime(p_start_dt, '%Y-%m-%d').date() - timedelta(days=365)
    end_dt=datetime.datetime.strptime(p_end_dt, '%Y-%m-%d').date() - timedelta(days=365)
    print(str(start_dt)+"    "+str(end_dt))
    
    #Get the most active station
    #qry=select([measurements_tb.station,func.count(measurements_tb.station).label("rec_count")]).\
    #group_by(measurements_tb.station).order_by(desc("rec_count"))
    #most_actv=engine.execute(qry).first()
    
    qry2 = session.query(func.min(measurements_tb.tobs),func.avg(measurements_tb.tobs),func.max(measurements_tb.tobs)).\
            filter(measurements_tb.measure_date>=start_dt,measurements_tb.measure_date<=end_dt).all()
        
    return qry2


#-------FUNCTION CALL-----------
qry_result=calc_temps('2016-01-01','2016-01-25')
print(qry_result)
```

    2015-01-01    2015-01-25
    [(56, Decimal('66.9123'), 79)]
    


```python
avg_list=[x[1] for x in qry_result]
min_list=[x[0] for x in qry_result]
max_list=[x[2] for x in qry_result]
print(str(avg_list[0])+"  "+str(min_list[0])+"   "+str(max_list[0]))
```

    66.9123  56   79
    


```python
plt.clf()
fig, ax = plt.subplots(figsize=(2,6))
ax.set_ylabel("Temp(F)")
ax.set_title("Trip Avg Temp (All Stations)")
plt.bar(0, avg_list, .02,
                 alpha=0.5,
                 color='r',
                 yerr=max_list[0]-min_list[0],               
                 )

plt.show()
```


    <matplotlib.figure.Figure at 0x20803f575f8>



![png](output_17_1.png)



```python

```
