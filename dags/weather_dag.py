from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import random
import pandas as pd
import os
from datetime import datetime
import config

# Define your DAG settings
default_args = {
    'owner': 'taoutaou',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def get_weather(api_key, city):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": api_key, "units": "metric"}

    try:
        response = requests.get(base_url, params=params)
        data = response.json()
        print(data)
        if data["cod"] == 200:
            weather_info = {
                "city": data["name"],
                "temperature": data["main"]["temp"],
                "description": data["weather"][0]["description"],
                "humidity": data["main"]["humidity"],
                "wind_speed": data["wind"]["speed"],
            }
            return weather_info
        else:
            print("Error: Unable to fetch weather data.")
            return None

    except requests.exceptions.RequestException as e:
        print("Error: ", e)
        return None
# Function to fetch weather data and save it to MongoDB
def extract_weather_data(ti):
    
    # Get the current directory of the script (dag folder)
    script_directory = os.path.dirname(os.path.abspath(__file__))

    # Construct the full path to the CSV file
    csv_file_path = os.path.join(script_directory, 'data', 'worldcities.csv')

    # Read the CSV file
    world_cities = pd.read_csv(csv_file_path)

    api_key = config.api_key
    all_data = pd.DataFrame(columns=['city', 'Temperature', 'Description', 'Humidity', 'Wind_speed','Timestamp'])
    
    for i in range(20):
        random_index = random.randint(0, len(world_cities) - 1)
        city = world_cities['city'].iloc[random_index]
        weather_data = get_weather(api_key, city)
        if weather_data:
            data = {
                'city': weather_data["city"],
                'Temperature': weather_data["temperature"],
                'Description': weather_data["description"],
                'Humidity': weather_data["humidity"],
                'Wind_speed': weather_data["wind_speed"],
                'Timestamp': datetime.now()
            }
            df = pd.DataFrame([data])
            all_data = pd.concat([all_data,df], ignore_index=True)
    
    ti.xcom_push(key = 'weather_data', value = all_data)

def transform_weather_data(ti):
    weather_details = ti.xcom_pull(task_ids = 'extract_weather_data', key = 'weather_data') 
    df = pd.DataFrame(weather_details)

    #df.drop_duplicates(inplace=True)

    df['Temperature'] = df['Temperature'].round(2)  # Round temperature to 2 decimal places
    df['Humidity'] = df['Humidity'].astype(int)  # Convert humidity to integers
    df['Wind_speed'] = df['Wind_speed'].round(2)  # Round wind speed to 2 decimal places

    weather_cleaned = ti.xcom_push(key = 'weather_cleaned', value = df)


def load_weather_data(ti):
    
    weather_details = ti.xcom_pull(task_ids='transform_weather_data', key='weather_cleaned')
     # Get the current directory of the script (dag folder)
    script_directory = os.path.dirname(os.path.abspath(__file__))

    # Construct the full path to the CSV file
    csv_file_path = os.path.join(script_directory, 'data', 'weather_data.csv')
    #weather_details.to_csv(csv_file_path, index=False)


    # Check if the CSV file already exists
    if os.path.exists(csv_file_path):
        # Load the existing data from the CSV file
        existing_data = pd.read_csv(csv_file_path)
    else:
        # Create an empty DataFrame if the file doesn't exist
        existing_data = pd.DataFrame()

    # Append the new weather data to the existing data
    df = pd.concat([existing_data, weather_details], ignore_index=True)
    # Save the combined data to the CSV file
    df.to_csv(csv_file_path, index=False)


# Define your DAG
with DAG(
    dag_id='weather_data_pipeline',
    default_args=default_args,
    description='A pipeline to fetch and store weather data for random cities csv file',
    start_date=datetime(2023, 9, 24),
    schedule_interval='@daily',  # You can adjust the schedule as needed
) as dag:

    #Install_dependecies = BashOperator(task_id='installing_PyMongo',bash_command='pip install pymongo dnspython')
 

    # Define a PythonOperator to execute the fetch_weather_and_save_mongodb function
    fetch_weather_task = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data    )

    transform_weather_task = PythonOperator(
        task_id = 'transform_weather_data',
        python_callable= transform_weather_data
    )

    load_weather_task = PythonOperator(
        task_id= 'load_weather_data',
        python_callable= load_weather_data
    )

# Task dependency
fetch_weather_task >> transform_weather_task >> load_weather_task

