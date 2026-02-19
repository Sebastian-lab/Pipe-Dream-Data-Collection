import csv
import os
from datetime import datetime, timedelta
from pydantic_settings import BaseSettings
from pymongo import MongoClient
import certifi
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    MONGO_URI: str
    DB_NAME: str = "weather_db"

    model_config = {
        "env_file": [".env", ".env.local"],
        "case_sensitive": False,
        "extra": "ignore"
    }


CITIES = [
    {"name": "Tokyo", "timezone": "Asia/Tokyo"},
    {"name": "San Diego", "timezone": "America/Los_Angeles"},
    {"name": "Las Vegas", "timezone": "America/Los_Angeles"},
    {"name": "London", "timezone": "Europe/London"},
    {"name": "Sydney", "timezone": "Australia/Sydney"},
    {"name": "New York", "timezone": "America/New_York"}
]


def get_previous_week_range():
    today = datetime.now().date()
    days_since_monday = today.weekday()
    last_monday = today - timedelta(days=days_since_monday + 7)
    last_sunday = last_monday + timedelta(days=6)
    return last_monday, last_sunday


def get_current_week_range():
    today = datetime.now().date()
    days_since_monday = today.weekday()
    current_monday = today - timedelta(days=days_since_monday)
    current_sunday = current_monday + timedelta(days=6)
    return current_monday, current_sunday


def export_weekly_by_city(test_mode: bool = False):
    settings = Settings()
    
    if test_mode:
        start_date, end_date = get_current_week_range()
    else:
        start_date, end_date = get_previous_week_range()
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date, datetime.max.time())
    
    mode_label = "test (current week)" if test_mode else "weekly"
    logger.info(f"Exporting {mode_label} readings from {start_date} to {end_date}")
    
    client = MongoClient(settings.MONGO_URI, tlsCAFile=certifi.where())
    db = client[settings.DB_NAME]
    collection = db["readings"]
    exports_collection = db["historical_exports"]
    
    cursor = collection.find({
        "timestamp": {"$gte": start_datetime, "$lte": end_datetime}
    }).sort([("city", 1), ("timestamp", 1)])
    
    city_data = {}
    for doc in cursor:
        city = doc.get("city")
        if city not in city_data:
            city_data[city] = []
        city_data[city].append(doc)
    
    total_exported = 0
    for city, readings in city_data.items():
        csv_lines = ["tempC,tempF,timestamp"]
        
        for doc in readings:
            features = doc.get("features", [])
            csv_lines.append(f"{features[1] if len(features) > 1 else ''},{features[2] if len(features) > 2 else ''},{features[0] if len(features) > 0 else ''}")
            total_exported += 1
        
        csv_content = "\n".join(csv_lines)
        
        city_info = next((c for c in CITIES if c["name"] == city), {})
        
        exports_collection.insert_one({
            "city": city,
            "timezone": city_info.get("timezone"),
            "week_start": start_date.isoformat(),
            "week_end": end_date.isoformat(),
            "filename": f"{city.lower().replace(' ', '_')}_{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}.csv",
            "csv_content": csv_content,
            "created_at": datetime.utcnow()
        })
        
        logger.info(f"Exported {len(readings)} readings for {city} to MongoDB")
    
    logger.info(f"Exported {total_exported} total readings to MongoDB")
    return total_exported


def export_readings_to_csv(days: int = 7):
    settings = Settings()
    client = MongoClient(settings.MONGO_URI, tlsCAFile=certifi.where())
    db = client[settings.DB_NAME]
    collection = db["readings"]

    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    logger.info(f"Exporting readings from {start_date.isoformat()} to {end_date.isoformat()}")

    cursor = collection.find({
        "timestamp": {"$gte": start_date, "$lte": end_date}
    }).sort("timestamp", 1)

    count = 0
    filename = f"readings_{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}.csv"

    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["city", "tempC", "tempF", "timestamp", "localTime"])

        for doc in cursor:
            writer.writerow([
                doc.get("city"),
                doc.get("tempC"),
                doc.get("tempF"),
                doc.get("timestamp").isoformat() if doc.get("timestamp") else "",
                doc.get("localTime", "")
            ])
            count += 1

    logger.info(f"Exported {count} readings to {filename}")
    return filename, count


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Export weather data to CSV")
    parser.add_argument("--test", action="store_true", help="Export last week's data immediately (for testing)")
    args = parser.parse_args()
    
    if args.test:
        print("Test mode: Exporting last week's data")
    export_weekly_by_city(test_mode=args.test)
