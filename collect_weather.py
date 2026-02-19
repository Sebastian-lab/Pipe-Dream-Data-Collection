import time
from datetime import datetime, timedelta, timezone
from pydantic_settings import BaseSettings
from zoneinfo import ZoneInfo
from pymongo import MongoClient, ASCENDING
import certifi
import requests
from typing import Dict, Any, List
import logging

from export_data import export_weekly_by_city

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    MONGO_URI: str
    DB_NAME: str = "weather_db"
    OPEN_METEO_URL: str = "https://api.open-meteo.com/v1/forecast"
    REFRESH_INTERVAL_MINUTES: int = 1

    model_config = {
        "env_file": [".env", ".env.local"],
        "case_sensitive": False
    }


CITIES = [
    {"name": "Tokyo", "lat": 35.6895, "lng": 139.6917, "timezone": "Asia/Tokyo"},
    {"name": "San Diego", "lat": 32.7628, "lng": -117.1633, "timezone": "America/Los_Angeles"},
    {"name": "Las Vegas", "lat": 36.1699, "lng": -115.1398, "timezone": "America/Los_Angeles"},
    {"name": "London", "lat": 51.5074, "lng": -0.1278, "timezone": "Europe/London"},
    {"name": "Sydney", "lat": -33.8688, "lng": 151.2093, "timezone": "Australia/Sydney"},
    {"name": "New York", "lat": 40.7128, "lng": -74.0060, "timezone": "America/New_York"}
]


def get_db_collection(collection_name: str, settings: Settings):
    client = MongoClient(settings.MONGO_URI, tlsCAFile=certifi.where())
    db = client[settings.DB_NAME]
    return db[collection_name]


def ensure_time_series_collection(settings: Settings):
    client = MongoClient(settings.MONGO_URI, tlsCAFile=certifi.where())
    db = client[settings.DB_NAME]
    
    collection_name = "readings"
    if collection_name not in db.list_collection_names():
        db.create_collection(
            collection_name,
            timeseries={
                "timeField": "timestamp",
                "metaField": "city"
            },
            expireAfterSeconds=604800
        )
        db[collection_name].create_index([("city", ASCENDING), ("timestamp", ASCENDING)])
        logger.info(f"Created Time Series collection '{collection_name}' with 7-day auto-expiry")
    return db[collection_name]


def fetch_weather(lat: float, lng: float, open_meteo_url: str) -> Dict[str, Any]:
    try:
        url = f"{open_meteo_url}?latitude={lat}&longitude={lng}&current_weather=true"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json().get("current_weather", {}) or {}
    except Exception as e:
        logger.error(f"Failed to fetch weather: {e}")
        return {}


def collect_weather_data():
    settings = Settings()
    collection = ensure_time_series_collection(settings)
    
    for city in CITIES:
        city_tz = ZoneInfo(city["timezone"])
        city_time = datetime.now(city_tz).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"[{city_time}] Fetching weather for {city['name']}")
        
        weather_data = fetch_weather(city["lat"], city["lng"], settings.OPEN_METEO_URL)
        if not weather_data:
            continue
        
        temp_c = weather_data.get("temperature")
        temp_f = round((temp_c * 9/5) + 32, 2) if temp_c is not None else None
        
        timestamp = datetime.now(city_tz)
        
        new_reading = {
            "city": city["name"],
            "timezone": city["timezone"],
            "timestamp": timestamp,
            "features": [timestamp.isoformat(), temp_c, temp_f]
        }
        
        collection.insert_one(new_reading)
        
        logger.info(f"[{city_time}] Stored reading for {city['name']}: {temp_c}°C / {temp_f}°F")
    
    logger.info("Weather data collection complete")


if __name__ == "__main__":
    EXPORT_HOUR = 8
    EXPORT_CHECK_HOUR = 0
    
    while True:
        now = datetime.now()
        
        if now.weekday() == 0 and now.hour == EXPORT_HOUR and now.minute == 0:
            logger.info("Monday 8:00 AM detected - running weekly export")
            try:
                export_weekly_by_city()
            except Exception as e:
                logger.error(f"Weekly export failed: {e}")
            time.sleep(60)
        
        collect_weather_data()
        next_run = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        sleep_seconds = (next_run - now).total_seconds()
        if sleep_seconds <= 0:
            sleep_seconds = 60
        time.sleep(sleep_seconds)
