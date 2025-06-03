# Airflows Imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Imports

import pandas as pd
import datetime
from datetime import timedelta

now_time=datetime.datetime.now()
now_time_str=str(now_time.strftime("%Y-%m-%d-%H%M"))


default_args = {
    'owner':'Ariel Subia',
    'depends_on_past': False,
    'email_on_failure':False,
    'email_on_retry':False,
    'retrais':1,
    'retry_delay':timedelta(hours=3)
}

# Functions Area

def _get_data1():
    
    # Data extract from API for first flight
    import requests 
    import os
    import json
    
    api_key=os.getenv('SERPAPI_API_KEY')
    params = {
            "api_key": api_key,
            "engine": "google_flights",
            "departure_id":"AEP",
            "arrival_id":"COR",
            "outbound_date":"2025-08-04",
            "return_date":"2025-08-12"
        }
    search = requests.get("https://serpapi.com/search", params=params)
    search.raise_for_status()
    data=search.json()
   
    # Storage of data in json format
    output_path=f"data/flights_AEP_COR_"+now_time_str
    with open(output_path,'w') as outfile:
        json.dump(data,outfile,indent=4)
        
    print("Extract and storage #1 ok")
    
    
def _get_data2():
    
    # Data extract from API for second flight
    import requests 
    import os
    import json
    
    api_key=os.getenv('SERPAPI_API_KEY')
    params = {
            "api_key": api_key,
            "engine": "google_flights",
            "departure_id":"COR",
            "arrival_id":"AEP",
            "outbound_date":"2025-04-12",
            "return_date":"2025-04-13"
        }
    search = requests.get("https://serpapi.com/search", params=params)
    search.raise_for_status()
    data=search.json()
    
    # Storage of data in json format
    output_path=f"data/flights_COR_AEP_"+now_time_str
    with open(output_path,'w') as outfile:
        json.dump(data,outfile,indent=4)
    
    print("Extract and storage #2 ok")
 
 
def _clean_join_data():
    import pandas as pd
    import os
    # Data Reading and Cleaning
    
    # Cleaning and colect useful data for first flight
    output_path=f"data/flights_AEP_COR_"+now_time_str
    with open(output_path,'r') as file:
        data=json.load(file)
        
    keys=["best_flights","other_flights"]

    time_departure=[]
    time_arrival=[]
    price=[]
    airplane=[]
    airline=[]
    flight_number=[]

    for key in keys:
        #print("Key: "+key)
        for item in data[key]:
            for flight in item["flights"]:
                time_departure.append(flight["departure_airport"]["time"])
                time_arrival.append(flight["arrival_airport"]["time"])
                price.append(str(item["price"]))
                airplane.append(flight["airplane"])
                flight_number.append(flight["flight_number"])
                airline.append(flight["airline"])

    data_flights={'time_departure':time_departure,'time_arrival':time_arrival,'price':price,'airplane':airplane,'airline':airline, 'flight_number':flight_number}

    df_flights_AEP_COR = pd.DataFrame(data_flights)

    # Drop unnecesary columns
    df_flights_AEP_COR=df_flights_AEP_COR.drop(['time_departure','time_arrival','airplane'],axis=1)

    # Add columns
    df_flights_AEP_COR.insert(3,"Departure","AEP")
    df_flights_AEP_COR.insert(4,"Arrival","COR")
    df_flights_AEP_COR.insert(5,"requested_at",now_time_str)

    dic = {'Aerolineas Argentinas':'1','Flybondi':'2','JetSMART':'3','LATAM':'4','Gol':'5','Andes Lineas Aereas':'6'}

    df_flights_AEP_COR=df_flights_AEP_COR.replace({'airline':dic})


    output_path=f"data/flights_COR_AEP_"+now_time_str
    with open(output_path,'r') as file:
        data1=json.load(file)
        
    keys=["best_flights","other_flights"]

    time_departure=[]
    time_arrival=[]
    price=[]
    airplane=[]
    airline=[]
    flight_number=[]

    for key in keys:
        #print("Key: "+key)
        for item in data1[key]:
            for flight in item["flights"]:
                time_departure.append(flight["departure_airport"]["time"])
                time_arrival.append(flight["arrival_airport"]["time"])
                price.append(str(item["price"]))
                airplane.append(flight["airplane"])
                flight_number.append(flight["flight_number"])
                airline.append(flight["airline"])

    data_flights={'time_departure':time_departure,'time_arrival':time_arrival,'price':price,'airplane':airplane,'airline':airline, 'flight_number':flight_number}

    df_flights_COR_AEP = pd.DataFrame(data_flights)

    # Drop unnecesary columns
    df_flights_COR_AEP=df_flights_COR_AEP.drop(['time_departure','time_arrival','airplane'],axis=1)

    # Add columns
    df_flights_COR_AEP.insert(3,"Departure","COR")
    df_flights_COR_AEP.insert(4,"Arrival","AEP")
    df_flights_COR_AEP.insert(5,"requested_at",now_time_str)

    dic = {'Aerolineas Argentinas':'1','Flybondi':'2','JetSMART':'3','LATAM':'4','Gol':'5','Andes Lineas Aereas':'6'}

    df_flights_COR_AEP=df_flights_COR_AEP.replace({'airline':dic})

    #Join Dataframes
    df_final=pd.concat([df_flights_AEP_COR,df_flights_COR_AEP],axis=0)
    df_final=df_final[['price','Departure','Arrival','flight_number','requested_at','airline']]
    
    #Save dataframe in csv format
    name_csv=f"csv/data_"+now_time_str
    df_final.to_csv(name_csv,sep='\t', index=False, header=False)
   
    
def _load_data():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pd_hook = PostgresHook(postgres_conn_id='Connection_test')
    pd_hook.bulk_load(table='cfproject.prices',file='data/data_2025-01-13-11-30-04.csv')


with DAG(
    'DAG_Project_CF',
    default_args=default_args,
    description='Creacion de DAG ETL Project CF ASubia',
    schedule_interval=None,
    tags=['ETL','Proyecto CF', 'PostgreSQL']
) as dag:
    
    get_api_bash = BashOperator(
        task_id='get_api_bash',
        bash_command='export SERPAPI_API_KEY=8eab590ca7b723709ffffcf13c3e857ea6aa181ed132bb6d69a0e2a6b972ab3a'
    )
    
    get_api_python1 = PythonOperator(
        task_id = 'get_api_python1',
        python_callable=_get_data1
    )
    
    get_api_python2 = PythonOperator(
        task_id = 'get_api_python2',
        python_callable=_get_data2
    )
    
    clean_join_data = PythonOperator(
        task_id = 'clean_join_data',
        python_callable=_clean_join_data
    )
    
    check_table = PostgresOperator(
        task_id = 'check_table',
        postgres_conn_id='connection_dbeaver',
        sql='sql/create_table.sql'
    )

    load_data = PythonOperator(
        task_id = 'load_data',
        python_callable=_load_data
    )


    
    get_api_bash >> [get_api_python1,get_api_python2] >> clean_join_data >> check_table >> load_data