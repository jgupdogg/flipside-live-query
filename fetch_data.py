import requests
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def fetch_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data: {e}")
        return None
