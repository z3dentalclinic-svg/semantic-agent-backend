"""
Google Ads API Service
Handles all interactions with Google Ads API
"""

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import os

def get_google_ads_client():
    """
    Initialize and return Google Ads API client
    """
    try:
        # Load configuration from google-ads.yaml
        client = GoogleAdsClient.load_from_storage("google-ads.yaml")
        return client
    except Exception as e:
        print(f"Error loading Google Ads client: {e}")
        return None

def get_locations_for_country(country_code: str):
    """
    Fetch all locations (regions and cities) for a given country
    from Google Geo Target Constants
    
    Args:
        country_code: ISO 2-letter country code (e.g., "IE", "UA")
    
    Returns:
        dict with 'regions' and 'cities' lists
    """
    client = get_google_ads_client()
    
    if not client:
        raise Exception("Failed to initialize Google Ads client")
    
    try:
        geo_service = client.get_service("GeoTargetConstantService")
        
        # Build query to get locations for country
        query = f"""
            SELECT
                geo_target_constant.id,
                geo_target_constant.name,
                geo_target_constant.country_code,
                geo_target_constant.target_type,
                geo_target_constant.canonical_name
            FROM geo_target_constant
            WHERE geo_target_constant.country_code = '{country_code}'
            AND geo_target_constant.status = 'ENABLED'
            ORDER BY geo_target_constant.name
        """
        
        # Execute query
        search_request = client.get_type("SearchGeoTargetConstantRequest")
        search_request.query = query
        
        response = geo_service.search_geo_target_constants(request=search_request)
        
        regions = []
        cities = []
        
        for row in response:
            location = {
                "id": str(row.geo_target_constant.id),
                "name": row.geo_target_constant.name,
                "type": row.geo_target_constant.target_type.name
            }
            
            # Categorize by type
            target_type = row.geo_target_constant.target_type.name
            
            if target_type in ["PROVINCE", "REGION", "STATE", "COUNTY", "DEPARTMENT"]:
                regions.append(location)
            elif target_type in ["CITY", "MUNICIPALITY"]:
                cities.append(location)
        
        return {
            "regions": regions,
            "cities": cities
        }
        
    except GoogleAdsException as ex:
        print(f"Google Ads API error: {ex}")
        raise Exception(f"Failed to fetch locations: {str(ex)}")
    except Exception as e:
        print(f"Error: {e}")
        raise Exception(f"Failed to fetch locations: {str(e)}")

def get_keyword_ideas(keyword: str, country_code: str, language_code: str):
    """
    Get keyword ideas from Google Keyword Planner
    
    Args:
        keyword: Seed keyword
        country_code: Country code (e.g., "IE")
        language_code: Language code (e.g., "en")
    
    Returns:
        List of keyword ideas with metrics
    """
    client = get_google_ads_client()
    
    if not client:
        raise Exception("Failed to initialize Google Ads client")
    
    try:
        keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")
        
        request = client.get_type("GenerateKeywordIdeasRequest")
        request.customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID", "")
        
        # Set keyword seed
        request.keyword_seed.keywords.append(keyword)
        
        # Set location (country)
        request.geo_target_constants.append(
            f"geoTargetConstants/{get_country_location_id(country_code)}"
        )
        
        # Set language
        request.language = f"languageConstants/{get_language_id(language_code)}"
        
        # Execute request
        keyword_ideas = keyword_plan_idea_service.generate_keyword_ideas(
            request=request
        )
        
        results = []
        for idea in keyword_ideas:
            results.append({
                "keyword": idea.text,
                "avg_monthly_searches": idea.keyword_idea_metrics.avg_monthly_searches,
                "competition": idea.keyword_idea_metrics.competition.name,
                "low_top_of_page_bid_micros": idea.keyword_idea_metrics.low_top_of_page_bid_micros,
                "high_top_of_page_bid_micros": idea.keyword_idea_metrics.high_top_of_page_bid_micros,
            })
        
        return results
        
    except GoogleAdsException as ex:
        print(f"Google Ads API error: {ex}")
        raise Exception(f"Failed to get keyword ideas: {str(ex)}")

def get_country_location_id(country_code: str) -> str:
    """
    Map country codes to Google Ads location IDs
    """
    mapping = {
        "IE": "2372",      # Ireland
        "UA": "2804",      # Ukraine
        "US": "2840",      # United States
        "GB": "2826",      # United Kingdom
        "DE": "2276",      # Germany
        "FR": "2250",      # France
        "ES": "2724",      # Spain
        "IT": "2380",      # Italy
        "PL": "2616",      # Poland
        "RU": "2643",      # Russia
    }
    return mapping.get(country_code, "2840")  # Default to US

def get_language_id(language_code: str) -> str:
    """
    Map language codes to Google Ads language IDs
    """
    mapping = {
        "en": "1000",    # English
        "ru": "1003",    # Russian
        "uk": "1036",    # Ukrainian
        "es": "1003",    # Spanish
        "de": "1001",    # German
        "fr": "1002",    # French
        "it": "1004",    # Italian
        "pl": "1025",    # Polish
    }
    return mapping.get(language_code, "1000")  # Default to English
