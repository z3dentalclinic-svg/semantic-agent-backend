"""
Semantic Agent Backend
FastAPI server with Google Ads API integration
Credentials from environment variables
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import os
import yaml

app = FastAPI(title="Semantic Agent API", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create google-ads.yaml from environment variables
def create_google_ads_config():
    """Create google-ads.yaml from environment variables"""
    config = {
        'developer_token': os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN'),
        'client_id': os.getenv('GOOGLE_ADS_CLIENT_ID'),
        'client_secret': os.getenv('GOOGLE_ADS_CLIENT_SECRET'),
        'refresh_token': os.getenv('GOOGLE_ADS_REFRESH_TOKEN', ''),
        'login_customer_id': os.getenv('GOOGLE_ADS_CUSTOMER_ID'),
        'use_proto_plus': True
    }
    
    # Write to file
    with open('google-ads.yaml', 'w') as f:
        yaml.dump(config, f)
    
    return config

# Models
class LocationRequest(BaseModel):
    country_code: str

class LocationResponse(BaseModel):
    id: str
    name: str
    type: str

@app.get("/")
async def root():
    credentials_loaded = all([
        os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN'),
        os.getenv('GOOGLE_ADS_CLIENT_ID'),
        os.getenv('GOOGLE_ADS_CLIENT_SECRET'),
        os.getenv('GOOGLE_ADS_CUSTOMER_ID')
    ])
    
    return {
        "service": "Semantic Agent API",
        "version": "1.0.0",
        "status": "running",
        "credentials_loaded": credentials_loaded,
        "endpoints": {
            "health": "/health",
            "locations": "/api/locations/{country_code}",
            "countries": "/api/countries"
        }
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "credentials": "loaded" if os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN") else "missing"
    }

@app.get("/api/countries")
async def get_countries():
    countries = [
        {"code": "IE", "name": "Ireland", "flag": "🇮🇪"},
        {"code": "UA", "name": "Україна", "flag": "🇺🇦"},
        {"code": "US", "name": "United States", "flag": "🇺🇸"},
        {"code": "GB", "name": "United Kingdom", "flag": "🇬🇧"},
        {"code": "DE", "name": "Deutschland", "flag": "🇩🇪"},
        {"code": "FR", "name": "France", "flag": "🇫🇷"},
        {"code": "ES", "name": "España", "flag": "🇪🇸"},
        {"code": "IT", "name": "Italia", "flag": "🇮🇹"},
        {"code": "PL", "name": "Polska", "flag": "🇵🇱"},
        {"code": "RU", "name": "Россия", "flag": "🇷🇺"},
    ]
    return {"countries": countries}

@app.get("/api/locations/{country_code}")
async def get_locations(country_code: str):
    """Get locations from Google Ads API"""
    try:
        # Create config from env vars
        create_google_ads_config()
        
        # Import Google Ads service
        from google_ads_service import get_locations_for_country
        
        locations = get_locations_for_country(country_code)
        return {
            "country_code": country_code,
            "locations": locations,
            "source": "google_ads_api"
        }
    except Exception as e:
        # Fallback to mock data
        print(f"Error: {e}")
        
        mock_data = {
            "IE": {
                "regions": [
                    {"id": "1007321", "name": "Carlow", "type": "County"},
                    {"id": "1007322", "name": "Cavan", "type": "County"},
                    {"id": "1007323", "name": "Clare", "type": "County"},
                    {"id": "1007324", "name": "Cork", "type": "County"},
                    {"id": "1007325", "name": "Donegal", "type": "County"},
                    {"id": "1007326", "name": "Dublin", "type": "County"},
                ],
                "cities": [
                    {"id": "1007340", "name": "Dublin", "type": "City"},
                    {"id": "1007341", "name": "Cork", "type": "City"},
                    {"id": "1007342", "name": "Galway", "type": "City"},
                ]
            },
            "UA": {
                "regions": [
                    {"id": "21135", "name": "Дніпропетровська", "type": "Oblast"},
                    {"id": "21136", "name": "Київська", "type": "Oblast"},
                    {"id": "21137", "name": "Львівська", "type": "Oblast"},
                ],
                "cities": [
                    {"id": "1012864", "name": "Дніпро", "type": "City"},
                    {"id": "1011969", "name": "Київ", "type": "City"},
                    {"id": "1009902", "name": "Львів", "type": "City"},
                ]
            }
        }
        
        if country_code.upper() in mock_data:
            return {
                "country_code": country_code.upper(),
                "locations": mock_data[country_code.upper()],
                "source": "mock_fallback",
                "error": str(e)
            }
        else:
            return {
                "country_code": country_code.upper(),
                "locations": {"regions": [], "cities": []},
                "source": "mock_fallback",
                "error": str(e)
            }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
