import time
from datetime import datetime, timedelta, timezone
from pydantic_settings import BaseSettings
from zoneinfo import ZoneInfo
from pymongo import MongoClient
import certifi
import requests
from datetime import datetime
from typing import Dict, Any, List
import logging

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
    collection = get_db_collection("city_readings", settings)
    
    for city in CITIES:
        city_tz = ZoneInfo(city["timezone"])
        city_time = datetime.now(city_tz).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"[{city_time}] Fetching weather for {city['name']}")
        
        weather_data = fetch_weather(city["lat"], city["lng"], settings.OPEN_METEO_URL)
        if not weather_data:
            continue
        
        temp_c = weather_data.get("temperature")
        temp_f = round((temp_c * 9/5) + 32, 2) if temp_c is not None else None
        
        new_reading = {
            "tempC": temp_c,
            "tempF": temp_f,
            "timezone": city["timezone"],
            "localTime": datetime.now(city_tz).isoformat()
        }
        
        collection.update_one(
            {"city": city["name"]},
            {
                "$set": {"updated_at": datetime.utcnow()},
                "$push": {"readings": {"$each": [new_reading], "$slice": -10080}}
            },
            upsert=True
        )
        
        logger.info(f"[{city_time}] Stored reading for {city['name']}: {temp_c}°C / {temp_f}°F")
    
    logger.info("Weather data collection complete")


if __name__ == "__main__":
    while True:
        collect_weather_data()
        now = datetime.now()
        next_run = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        sleep_seconds = (next_run - now).total_seconds()
        time.sleep(sleep_seconds)
