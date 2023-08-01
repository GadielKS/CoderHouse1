import requests
import pandas as pd
from sqlalchemy import create_engine

# Define cities
cities = ['Buenos Aires', 'New York', 'London', 'Paris', 'Berlin', 'Madrid', 'Tokyo', 'Sydney', 'Moscow', 'Cape Town']

# Define database credentials
engine = create_engine(
  'redshift+psycopg2://scharfgadiel_coderhouse:JTD9823vUq@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database'
)

# Define the query to create the table
create_table_query = """
CREATE TABLE IF NOT EXISTS weather_data (
    observation_time TIMESTAMP,
    temp_C INT,
    temp_F INT,
    weatherCode INT,
    weatherIconUrl VARCHAR(1000),
    weatherDesc VARCHAR(1000),
    precipMM REAL,
    humidity INT,
    visibility INT,
    pressure INT,
    cloudcover INT,
    FeelsLikeC INT,
    FeelsLikeF INT,
    uvIndex INT,
    city VARCHAR(100),
    UNIQUE (observation_time, city)
)
"""

try:
    with engine.connect() as connection:
        connection.execute(create_table_query)
except Exception as e:
    print(f"Error creating table: {str(e)}")

# Start the request for each city
for city in cities:
    # Define the API URL
    url = f'https://wttr.in/{city}?format=j1'

    try:
        # Make the request to the API
        response = requests.get(url)

        # Verify that the request was successful
        if response.status_code == 200:
            # Get the data in JSON format
            data = response.json()

            # Extract the current information
            current_condition = data['current_condition'][0]

            # Create a dictionary with only the data we need
            filtered_data = {
                'observation_time': pd.to_datetime(current_condition['observation_time']),
                'temp_C': int(current_condition['temp_C']),
                'temp_F': int(current_condition['temp_F']),
                'weatherCode': int(current_condition['weatherCode']),
                'weatherIconUrl': current_condition['weatherIconUrl'][0]['value'] if current_condition['weatherIconUrl'] else None,
                'weatherDesc': current_condition['weatherDesc'][0]['value'] if current_condition['weatherDesc'] else None,
                'precipMM': float(current_condition['precipMM']),
                'humidity': int(current_condition['humidity']),
                'visibility': int(current_condition['visibility']),
                'pressure': int(current_condition['pressure']),
                'cloudcover': int(current_condition['cloudcover']),
                'FeelsLikeC': int(current_condition['FeelsLikeC']),
                'FeelsLikeF': int(current_condition['FeelsLikeF']),
                'uvIndex': int(current_condition['uvIndex']),
                'city': city
            }

            # Create a DataFrame with that data
            df = pd.DataFrame([filtered_data])

            # Convert the DataFrame to a list of dictionaries
            data_to_insert = df.to_dict(orient='records')

            # Insert the data into the database table
            insert_statement = """
                INSERT INTO weather_data (observation_time, temp_C, temp_F, weatherCode, weatherIconUrl, weatherDesc, precipMM, 
                humidity, visibility, pressure, cloudcover, FeelsLikeC, FeelsLikeF, uvIndex, city)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            with engine.begin() as connection:
                for data in data_to_insert:
                    try:
                        select_query = f"""SELECT * FROM weather_data WHERE observation_time='{data['observation_time']}' AND city='{data['city']}'"""
                        connection.execute(select_query)
                        result = connection.fetchone()
                        if result is None:  # no duplicate, we can insert      
                        # Insert the data
                          connection.execute(insert_statement, list(data.values()))
                    except Exception as e:
                        print(f"Error inserting data: {data}")
                        print(f"Error details: {str(e)}")

        else:
            print(f'Request error: {response.status_code}')

    except requests.exceptions.RequestException as e:
        print(f"Request error: {str(e)}")

    except Exception as e:
        print(f"Unexpected error: {str(e)}")

