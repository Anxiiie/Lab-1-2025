import json
import os
import io
import pytz
import requests
import clickhouse_connect

from datetime import datetime, timedelta
from typing import Dict, List
from minio import Minio
from prefect import flow, task


CITIES = {"Moscow": (55.7558, 37.6176), "Samara": (53.1959, 50.1002)}
TOMORROW = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')


def get_clickhouse_client():
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        port=int(os.getenv("CLICKHOUSE_PORT")),
        username=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD")
    )
    return client


def get_minio_client():
    client = Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=os.getenv("MINIO_SECURE", "False").lower() == "true"
    )
    bucket_name = os.getenv("MINIO_BUCKET_NAME")

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

    return client, bucket_name


@task
def extract_weather_data(city_name: str, lat: float, lon: float) -> Dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "windspeed_10m_max"],
        "hourly": ["temperature_2m", "precipitation", "windspeed_10m", "winddirection_10m"],
        "timezone": "Europe/Moscow", 
        "start_date": TOMORROW,
        "end_date": TOMORROW,
    }

    response = requests.get("https://api.open-meteo.com/v1/forecast", params=params)
    response.raise_for_status()
    data = response.json()
    return data


@task
def save_raw_to_minio(raw_data: Dict, city_name: str):
    client, bucket_name = get_minio_client()
    object_name = f"raw/{TOMORROW}/{city_name.lower()}_raw.json"
    json_data = json.dumps(raw_data, ensure_ascii=False)

    data = io.BytesIO(json_data.encode('utf-8'))

    client.put_object(
        bucket_name,
        object_name,
        data=data, 
        length=len(json_data.encode('utf-8')),
        content_type='application/json'
    )


@task
def transform_hourly_data(raw_data: Dict, city_name: str) -> List[Dict]:
    hourly_data = raw_data.get('hourly', {})
    times = hourly_data.get('time', [])
    temps = hourly_data.get('temperature_2m', [])
    precip = hourly_data.get('precipitation', [])
    windspeed = hourly_data.get('windspeed_10m', [])
    winddir = hourly_data.get('winddirection_10m', [])

    transformed_rows = []
    moscow_tz = pytz.timezone('Europe/Moscow')
    for i in range(len(times)):
        dt = datetime.fromisoformat(times[i].replace('Z', '+00:00'))
        dt_moscow = dt.astimezone(moscow_tz)
        row = {
            "id": int(dt_moscow.timestamp() * 1000000 + hash(city_name) % 1000000),
            "city": city_name,
            "datetime": dt_moscow, 
            "temperature_2m": temps[i],
            "precipitation": precip[i],
            "windspeed_10m": windspeed[i],
            "winddirection_10m": winddir[i] 
        }
        transformed_rows.append(row)
    return transformed_rows


@task
def transform_daily_data(raw_data: Dict, city_name: str) -> Dict:
    daily_data = raw_data.get('daily', {})
    times = daily_data.get('time', [])
    min_temps = daily_data.get('temperature_2m_min', [])
    max_temps = daily_data.get('temperature_2m_max', [])
    precip_sums = daily_data.get('precipitation_sum', [])
    wind_maxs = daily_data.get('windspeed_10m_max', [])

    date_str = times[0] 

    date_obj = datetime.fromisoformat(date_str).date()

    min_temp = min_temps[0] 
    max_temp = max_temps[0] 
    avg_temp = (min_temp + max_temp) / 2.0
    total_precip = precip_sums[0] 
    max_wind = wind_maxs[0] 

    daily_row = {
        "id": int(datetime.fromisoformat(date_str).timestamp() * 1000000 + hash(city_name) % 1000000),
        "city": city_name,
        "date": date_obj, 
        "min_temp": min_temp,
        "max_temp": max_temp,
        "avg_temp": avg_temp,
        "total_precipitation": total_precip,
        "max_windspeed": max_wind,
    }
    return daily_row


@task
def load_hourly_to_clickhouse(hourly_data: List[Dict]):
    client = get_clickhouse_client()
    table_name = "weather_hourly"
    columns = ['id', 'city', 'datetime', 'temperature_2m', 'precipitation', 'windspeed_10m', 'winddirection_10m']

    values = [
        (row['id'], row['city'], row['datetime'], row['temperature_2m'],
         row['precipitation'], row['windspeed_10m'], row['winddirection_10m'])
        for row in hourly_data
    ]

    client.insert(table_name, values, column_names=columns)


@task
def load_daily_to_clickhouse(daily_data: Dict):
    client = get_clickhouse_client()
    table_name = "weather_daily"
    columns = ['id', 'city', 'date', 'min_temp', 'max_temp', 'avg_temp', 'total_precipitation', 'max_windspeed']

    values = [(daily_data['id'], daily_data['city'], daily_data['date'],
               daily_data['min_temp'], daily_data['max_temp'], daily_data['avg_temp'],
               daily_data['total_precipitation'], daily_data['max_windspeed'])]

    client.insert(table_name, values, column_names=columns)


@task
def send_telegram_notification(daily_data: Dict):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    city = daily_data['city']
    date_str = daily_data['date']
    min_t = daily_data['min_temp']
    max_t = daily_data['max_temp']
    total_p = daily_data['total_precipitation']
    max_w = daily_data['max_windspeed']

    message = f"🌤️ Forecast on {date_str} for {city}:\n"
    message += f" - Min. temp: {min_t:.1f}°C\n"
    message += f" - Max. temp: {max_t:.1f}°C\n"
    message += f" - Prec: {total_p:.1f} mm\n"
    message += f" - Max. wind speed: {max_w:.1f} km/h\n"

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    response = requests.post(url, json=payload)
    response.raise_for_status()


@flow
def weather_etl():
    raw_data_results = []
    for city_name, (lat, lon) in CITIES.items():
        raw_data = extract_weather_data(city_name, lat, lon)
        save_raw_to_minio(raw_data, city_name)
        raw_data_results.append((raw_data, city_name))

    for raw_data, city_name in raw_data_results:
        hourly_transformed = transform_hourly_data(raw_data, city_name)
        daily_transformed = transform_daily_data(raw_data, city_name)

        load_hourly_to_clickhouse(hourly_transformed)
        load_daily_to_clickhouse(daily_transformed)

        send_telegram_notification(daily_transformed)
