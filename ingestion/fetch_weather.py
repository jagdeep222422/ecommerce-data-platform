"""
Module: fetch_weather.py
Description: Ingests weather forecast data from OpenWeatherMap API.
Used as an external feature for demand forecasting (ML layer).
Covers: API key auth, nested JSON parsing, feature enrichment.

Setup: Get a free API key at https://openweathermap.org/api (free tier = 1000 calls/day)
Set env var: export OPENWEATHER_API_KEY="your_key_here"
"""

import requests
import json
import os
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://api.openweathermap.org/data/2.5"
OUTPUT_DIR = "./data/bronze/weather"
API_KEY = os.getenv("OPENWEATHER_API_KEY", "demo_key")  # Set your key as env variable

CITIES = [
    {"name": "New York", "country": "US"},
    {"name": "London", "country": "GB"},
    {"name": "Mumbai", "country": "IN"},
    {"name": "Tokyo", "country": "JP"},
]


def fetch_current_weather(city: str, country: str) -> dict:
    """
    Fetch current weather for a city.
    Returns cleaned weather dict for use as ML feature.
    """
    url = f"{BASE_URL}/weather"
    params = {"q": f"{city},{country}", "appid": API_KEY, "units": "metric"}

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    # Flatten nested JSON to flat feature row
    return {
        "city": city,
        "country": country,
        "temperature_c": data["main"]["temp"],
        "feels_like_c": data["main"]["feels_like"],
        "humidity_pct": data["main"]["humidity"],
        "weather_condition": data["weather"][0]["main"],
        "weather_description": data["weather"][0]["description"],
        "wind_speed_mps": data["wind"]["speed"],
        "visibility_m": data.get("visibility", None),
        "recorded_at": datetime.utcfromtimestamp(data["dt"]).isoformat(),
        "_ingestion_timestamp": datetime.now().isoformat(),
        "_source": "openweathermap.org",
    }


def ingest_weather_all_cities() -> dict:
    """
    Ingest weather for all configured cities.
    Demonstrates multi-endpoint ingestion with rate limit handling.
    """
    run_date = datetime.now().strftime("%Y-%m-%d")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results = []
    errors = []

    for city_config in CITIES:
        try:
            weather = fetch_current_weather(city_config["name"], city_config["country"])
            results.append(weather)
            logger.info(f"✅ {city_config['name']}: {weather['temperature_c']}°C, {weather['weather_condition']}")
            time.sleep(0.5)  # Rate limit: max 60 calls/min on free tier
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.error("Invalid API key. Using mock data for demo purposes.")
                results.append(_mock_weather(city_config["name"], city_config["country"]))
            else:
                errors.append({"city": city_config["name"], "error": str(e)})
        except Exception as e:
            logger.error(f"Failed for {city_config['name']}: {e}")
            results.append(_mock_weather(city_config["name"], city_config["country"]))

    # Save to Bronze
    os.makedirs(f"{OUTPUT_DIR}/{run_date}", exist_ok=True)
    output_path = f"{OUTPUT_DIR}/{run_date}/weather_{timestamp}.json"

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"✅ Saved weather data for {len(results)} cities to: {output_path}")
    return {
        "status": "success",
        "cities_ingested": len(results),
        "errors": errors,
        "output_path": output_path,
    }


def _mock_weather(city: str, country: str) -> dict:
    """
    Returns mock weather data when API key is not set.
    Used for local development and testing.
    """
    import random
    return {
        "city": city,
        "country": country,
        "temperature_c": round(random.uniform(10, 35), 1),
        "feels_like_c": round(random.uniform(8, 33), 1),
        "humidity_pct": random.randint(40, 90),
        "weather_condition": random.choice(["Clear", "Clouds", "Rain"]),
        "weather_description": "mock data for demo",
        "wind_speed_mps": round(random.uniform(1, 15), 1),
        "visibility_m": 10000,
        "recorded_at": datetime.now().isoformat(),
        "_ingestion_timestamp": datetime.now().isoformat(),
        "_source": "mock_data",
    }


if __name__ == "__main__":
    result = ingest_weather_all_cities()
    print(json.dumps(result, indent=2))
