import asyncio
import hashlib
import logging
import os
import secrets
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests as http_requests
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
# Flight schedule data fetched via public aviation data feeds with timestamp support

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Airport registry (with lat/lon for ADS-B point queries)
# ---------------------------------------------------------------------------
AIRPORTS = {
    "VIDP": {"iata": "DEL", "name": "Indira Gandhi International",       "city": "Delhi",       "country": "India", "lat": 28.5665, "lon": 77.1031, "tz": "Asia/Kolkata"},
    "VABB": {"iata": "BOM", "name": "Chhatrapati Shivaji Maharaj Intl",  "city": "Mumbai",      "country": "India", "lat": 19.0896, "lon": 72.8656, "tz": "Asia/Kolkata"},
    "VOBL": {"iata": "BLR", "name": "Kempegowda International",          "city": "Bengaluru",   "country": "India", "lat": 13.1986, "lon": 77.7066, "tz": "Asia/Kolkata"},
    "VOMM": {"iata": "MAA", "name": "Chennai International",             "city": "Chennai",     "country": "India", "lat": 12.9941, "lon": 80.1709, "tz": "Asia/Kolkata"},
    "VOHY": {"iata": "HYD", "name": "Rajiv Gandhi International",        "city": "Hyderabad",   "country": "India", "lat": 17.2403, "lon": 78.4294, "tz": "Asia/Kolkata"},
    "VECC": {"iata": "CCU", "name": "Netaji Subhas Chandra Bose Intl",   "city": "Kolkata",     "country": "India", "lat": 22.6520, "lon": 88.4467, "tz": "Asia/Kolkata"},
    "VOCX": {"iata": "COK", "name": "Cochin International",              "city": "Kochi",       "country": "India", "lat": 10.1520, "lon": 76.4019, "tz": "Asia/Kolkata"},
    "VAAH": {"iata": "AMD", "name": "Sardar Vallabhbhai Patel Intl",     "city": "Ahmedabad",   "country": "India", "lat": 23.0774, "lon": 72.6347, "tz": "Asia/Kolkata"},
    "VANP": {"iata": "NAG", "name": "Dr Babasaheb Ambedkar Intl",        "city": "Nagpur",      "country": "India", "lat": 21.0922, "lon": 79.0472, "tz": "Asia/Kolkata"},
    "VOGO": {"iata": "GOI", "name": "Goa International (Dabolim)",       "city": "Goa",         "country": "India", "lat": 15.3808, "lon": 73.8314, "tz": "Asia/Kolkata"},
    "VICC": {"iata": "IXC", "name": "Chandigarh International",          "city": "Chandigarh",  "country": "India", "lat": 30.6735, "lon": 76.7885, "tz": "Asia/Kolkata"},
    "VEGY": {"iata": "GAU", "name": "Lokpriya Gopinath Bordoloi Intl",   "city": "Guwahati",    "country": "India", "lat": 26.1061, "lon": 91.5859, "tz": "Asia/Kolkata"},
    "VEPT": {"iata": "PAT", "name": "Lok Nayak Jayaprakash Airport",     "city": "Patna",       "country": "India", "lat": 25.5913, "lon": 85.0880, "tz": "Asia/Kolkata"},
    "VOPN": {"iata": "PNQ", "name": "Pune International",                "city": "Pune",        "country": "India", "lat": 18.5822, "lon": 73.9197, "tz": "Asia/Kolkata"},
    "VOPB": {"iata": "IXZ", "name": "Veer Savarkar International",       "city": "Port Blair",  "country": "India", "lat": 11.6412, "lon": 92.7296, "tz": "Asia/Kolkata"},
    "VILK": {"iata": "LKO", "name": "Chaudhary Charan Singh Intl",      "city": "Lucknow",     "country": "India", "lat": 26.7606, "lon": 80.8893, "tz": "Asia/Kolkata"},
    "VIJP": {"iata": "JAI", "name": "Jaipur International",              "city": "Jaipur",      "country": "India", "lat": 26.8242, "lon": 75.8122, "tz": "Asia/Kolkata"},
    "VEBS": {"iata": "BBI", "name": "Biju Patnaik International",        "city": "Bhubaneswar", "country": "India", "lat": 20.2444, "lon": 85.8178, "tz": "Asia/Kolkata"},
    "VIBN": {"iata": "VNS", "name": "Lal Bahadur Shastri International", "city": "Varanasi",    "country": "India", "lat": 25.4524, "lon": 82.8593, "tz": "Asia/Kolkata"},
    "VOTV": {"iata": "TRV", "name": "Trivandrum International",          "city": "Thiruvananthapuram", "country": "India", "lat": 8.4821, "lon": 76.9201, "tz": "Asia/Kolkata"},
    "VOML": {"iata": "IXE", "name": "Mangalore International",           "city": "Mangalore",   "country": "India", "lat": 12.9613, "lon": 74.8901, "tz": "Asia/Kolkata"},
    "VOCB": {"iata": "CJB", "name": "Coimbatore International",          "city": "Coimbatore",  "country": "India", "lat": 11.0300, "lon": 77.0434, "tz": "Asia/Kolkata"},
    "VISR": {"iata": "SXR", "name": "Sheikh ul-Alam International",      "city": "Srinagar",    "country": "India", "lat": 33.9871, "lon": 74.7742, "tz": "Asia/Kolkata"},
    "VIAR": {"iata": "ATQ", "name": "Sri Guru Ram Dass Jee Intl",        "city": "Amritsar",    "country": "India", "lat": 31.7096, "lon": 74.7973, "tz": "Asia/Kolkata"},
    "VERC": {"iata": "IXR", "name": "Birsa Munda Airport",               "city": "Ranchi",      "country": "India", "lat": 23.3143, "lon": 85.3217, "tz": "Asia/Kolkata"},
    "VAID": {"iata": "IDR", "name": "Devi Ahilyabai Holkar Airport",     "city": "Indore",      "country": "India", "lat": 22.7218, "lon": 75.8011, "tz": "Asia/Kolkata"},
    "VOVZ": {"iata": "VTZ", "name": "Visakhapatnam International",       "city": "Visakhapatnam", "country": "India", "lat": 17.7212, "lon": 83.2245, "tz": "Asia/Kolkata"},
    "VERP": {"iata": "RPR", "name": "Swami Vivekananda Airport",         "city": "Raipur",      "country": "India", "lat": 21.1804, "lon": 81.7387, "tz": "Asia/Kolkata"},
    "VEIM": {"iata": "IMF", "name": "Bir Tikendrajit International",     "city": "Imphal",      "country": "India", "lat": 24.7600, "lon": 93.8967, "tz": "Asia/Kolkata"},
    "VEBD": {"iata": "IXB", "name": "Bagdogra Airport",                  "city": "Bagdogra",    "country": "India", "lat": 26.6812, "lon": 88.3286, "tz": "Asia/Kolkata"},
    "VIDN": {"iata": "DED", "name": "Jolly Grant Airport",               "city": "Dehradun",    "country": "India", "lat": 30.1897, "lon": 78.1803, "tz": "Asia/Kolkata"},
    "VOCL": {"iata": "CCJ", "name": "Calicut International",             "city": "Kozhikode",   "country": "India", "lat": 11.1368, "lon": 75.9553, "tz": "Asia/Kolkata"},
    "VOTR": {"iata": "TRZ", "name": "Tiruchirappalli International",     "city": "Tiruchirappalli", "country": "India", "lat": 10.7654, "lon": 78.7097, "tz": "Asia/Kolkata"},
    "VIUD": {"iata": "UDR", "name": "Maharana Pratap Airport",            "city": "Udaipur",     "country": "India", "lat": 24.6177, "lon": 73.8961, "tz": "Asia/Kolkata"},
    "VOMD": {"iata": "IXM", "name": "Madurai Airport",                   "city": "Madurai",     "country": "India", "lat": 9.8345,  "lon": 78.0934, "tz": "Asia/Kolkata"},
    "VILH": {"iata": "IXL", "name": "Kushok Bakula Rimpochee Airport",   "city": "Leh",         "country": "India", "lat": 34.1359, "lon": 77.5465, "tz": "Asia/Kolkata"},
    "VOGA": {"iata": "GOX", "name": "Manohar International (Mopa)",      "city": "North Goa",   "country": "India", "lat": 15.7383, "lon": 73.8314, "tz": "Asia/Kolkata"},
    # Additional major airports
    "VABP": {"iata": "BHO", "name": "Raja Bhoj Airport",                 "city": "Bhopal",      "country": "India", "lat": 23.2875, "lon": 77.3374, "tz": "Asia/Kolkata"},
    "VIJU": {"iata": "IXJ", "name": "Jammu Airport",                     "city": "Jammu",       "country": "India", "lat": 32.6891, "lon": 74.8374, "tz": "Asia/Kolkata"},
    "VOVR": {"iata": "VGA", "name": "Vijayawada International Airport",  "city": "Vijayawada",  "country": "India", "lat": 16.5304, "lon": 80.7968, "tz": "Asia/Kolkata"},
    "VOTP": {"iata": "TIR", "name": "Tirupati Airport",                  "city": "Tirupati",    "country": "India", "lat": 13.6325, "lon": 79.5433, "tz": "Asia/Kolkata"},
    "VOKN": {"iata": "CNN", "name": "Kannur International Airport",      "city": "Kannur",      "country": "India", "lat": 11.9186, "lon": 75.5477, "tz": "Asia/Kolkata"},
    "VOBM": {"iata": "IXG", "name": "Belagavi Airport",                  "city": "Belagavi",    "country": "India", "lat": 15.8593, "lon": 74.6183, "tz": "Asia/Kolkata"},
    "VABO": {"iata": "BDQ", "name": "Vadodara Airport",                  "city": "Vadodara",    "country": "India", "lat": 22.3362, "lon": 73.2263, "tz": "Asia/Kolkata"},
    "VIJO": {"iata": "JDH", "name": "Jodhpur Airport",                   "city": "Jodhpur",     "country": "India", "lat": 26.2511, "lon": 73.0489, "tz": "Asia/Kolkata"},
    "VASU": {"iata": "STV", "name": "Surat Airport",                     "city": "Surat",       "country": "India", "lat": 21.1141, "lon": 72.7418, "tz": "Asia/Kolkata"},
    "VIGR": {"iata": "GWL", "name": "Gwalior Airport",                   "city": "Gwalior",     "country": "India", "lat": 26.2933, "lon": 78.2278, "tz": "Asia/Kolkata"},
    "VEMN": {"iata": "DIB", "name": "Dibrugarh Airport",                 "city": "Dibrugarh",   "country": "India", "lat": 27.4839, "lon": 95.0169, "tz": "Asia/Kolkata"},
    "VEJT": {"iata": "JRH", "name": "Jorhat Airport",                    "city": "Jorhat",      "country": "India", "lat": 26.7315, "lon": 94.1755, "tz": "Asia/Kolkata"},
    "VEKU": {"iata": "IXS", "name": "Silchar Airport",                   "city": "Silchar",     "country": "India", "lat": 24.9129, "lon": 92.9787, "tz": "Asia/Kolkata"},
    "VOHB": {"iata": "HBX", "name": "Hubli Airport",                     "city": "Hubli",       "country": "India", "lat": 15.3617, "lon": 75.0849, "tz": "Asia/Kolkata"},
    "VEGK": {"iata": "GOP", "name": "Gorakhpur Airport",                 "city": "Gorakhpur",   "country": "India", "lat": 26.7397, "lon": 83.4497, "tz": "Asia/Kolkata"},
    "VOMY": {"iata": "MYQ", "name": "Mysore Airport",                    "city": "Mysuru",      "country": "India", "lat": 12.2306, "lon": 76.6496, "tz": "Asia/Kolkata"},
    "VORY": {"iata": "RJA", "name": "Rajahmundry Airport",               "city": "Rajahmundry", "country": "India", "lat": 17.1104, "lon": 81.8182, "tz": "Asia/Kolkata"},
    "VIGG": {"iata": "DHM", "name": "Kangra Airport",                    "city": "Dharamsala",  "country": "India", "lat": 32.1651, "lon": 76.2634, "tz": "Asia/Kolkata"},
    "VAJB": {"iata": "JLR", "name": "Jabalpur Airport",                  "city": "Jabalpur",    "country": "India", "lat": 23.1778, "lon": 80.0520, "tz": "Asia/Kolkata"},
    "VEMR": {"iata": "DMU", "name": "Dimapur Airport",                   "city": "Dimapur",     "country": "India", "lat": 25.8839, "lon": 93.7710, "tz": "Asia/Kolkata"},
    "VEAK": {"iata": "AJL", "name": "Lengpui Airport",                   "city": "Aizawl",      "country": "India", "lat": 23.8406, "lon": 92.6197, "tz": "Asia/Kolkata"},
    "VAAU": {"iata": "IXU", "name": "Aurangabad Airport",                "city": "Aurangabad",  "country": "India", "lat": 19.8627, "lon": 75.3981, "tz": "Asia/Kolkata"},
    "VASD": {"iata": "SAG", "name": "Shirdi Airport",                    "city": "Shirdi",      "country": "India", "lat": 19.6882, "lon": 74.3789, "tz": "Asia/Kolkata"},
    # Northeast India
    "VEAT": {"iata": "IXA", "name": "Maharaja Bir Bikram Airport",       "city": "Agartala",    "country": "India", "lat": 23.8873, "lon": 91.2387, "tz": "Asia/Kolkata"},
    "VETZ": {"iata": "TEZ", "name": "Tezpur Airport",                    "city": "Tezpur",      "country": "India", "lat": 26.7083, "lon": 92.7847, "tz": "Asia/Kolkata"},
    "VELR": {"iata": "IXI", "name": "Lilabari Airport",                  "city": "Lilabari",    "country": "India", "lat": 27.2950, "lon": 94.0978, "tz": "Asia/Kolkata"},
    "VEDG": {"iata": "RDP", "name": "Kazi Nazrul Islam Airport",         "city": "Durgapur",    "country": "India", "lat": 23.6225, "lon": 87.2430, "tz": "Asia/Kolkata"},
    "VEBI": {"iata": "SHL", "name": "Umroi Airport",                     "city": "Shillong",    "country": "India", "lat": 25.7036, "lon": 91.9787, "tz": "Asia/Kolkata"},
    "VEJH": {"iata": "JRG", "name": "Veer Surendra Sai Airport",         "city": "Jharsuguda",  "country": "India", "lat": 21.9134, "lon": 84.0504, "tz": "Asia/Kolkata"},
    "VEDO": {"iata": "DGH", "name": "Deoghar Airport",                   "city": "Deoghar",     "country": "India", "lat": 24.4192, "lon": 86.7087, "tz": "Asia/Kolkata"},
    # North India (UP, Uttarakhand, HP, Rajasthan)
    "VEAB": {"iata": "IXD", "name": "Prayagraj Airport",                 "city": "Prayagraj",   "country": "India", "lat": 25.4401, "lon": 81.7339, "tz": "Asia/Kolkata"},
    "VIAG": {"iata": "AGR", "name": "Agra Airport",                      "city": "Agra",        "country": "India", "lat": 27.1558, "lon": 77.9610, "tz": "Asia/Kolkata"},
    "VIKA": {"iata": "KNU", "name": "Kanpur Airport",                    "city": "Kanpur",      "country": "India", "lat": 26.4414, "lon": 80.3649, "tz": "Asia/Kolkata"},
    "VIBY": {"iata": "BEK", "name": "Bareilly Airport",                  "city": "Bareilly",    "country": "India", "lat": 28.4209, "lon": 79.4500, "tz": "Asia/Kolkata"},
    "VEKI": {"iata": "KBK", "name": "Kushinagar International Airport",  "city": "Kushinagar",  "country": "India", "lat": 26.7408, "lon": 83.8188, "tz": "Asia/Kolkata"},
    "VIBR": {"iata": "KUU", "name": "Kullu-Manali Airport",              "city": "Kullu",       "country": "India", "lat": 31.8767, "lon": 77.1544, "tz": "Asia/Kolkata"},
    "VIPT": {"iata": "PGH", "name": "Pantnagar Airport",                 "city": "Pantnagar",   "country": "India", "lat": 29.0334, "lon": 79.4737, "tz": "Asia/Kolkata"},
    "VIKG": {"iata": "KQH", "name": "Kishangarh Airport",               "city": "Ajmer",       "country": "India", "lat": 26.6821, "lon": 74.8131, "tz": "Asia/Kolkata"},
    "VIJR": {"iata": "JSA", "name": "Jaisalmer Airport",                 "city": "Jaisalmer",   "country": "India", "lat": 26.8889, "lon": 70.8650, "tz": "Asia/Kolkata"},
    "VIBK": {"iata": "BKB", "name": "Bikaner Airport",                   "city": "Bikaner",     "country": "India", "lat": 28.0706, "lon": 73.2072, "tz": "Asia/Kolkata"},
    # West India (Gujarat, Maharashtra)
    "VABV": {"iata": "BHU", "name": "Bhavnagar Airport",                 "city": "Bhavnagar",   "country": "India", "lat": 21.7522, "lon": 72.1852, "tz": "Asia/Kolkata"},
    "VAJM": {"iata": "JGA", "name": "Jamnagar Airport",                  "city": "Jamnagar",    "country": "India", "lat": 22.4655, "lon": 70.0126, "tz": "Asia/Kolkata"},
    "VAHS": {"iata": "RAJ", "name": "Rajkot International Airport",      "city": "Rajkot",      "country": "India", "lat": 22.3600, "lon": 70.7795, "tz": "Asia/Kolkata"},
    "VAPR": {"iata": "PBD", "name": "Porbandar Airport",                 "city": "Porbandar",   "country": "India", "lat": 21.6487, "lon": 69.6572, "tz": "Asia/Kolkata"},
    "VABJ": {"iata": "BHJ", "name": "Bhuj Airport",                      "city": "Bhuj",        "country": "India", "lat": 23.2878, "lon": 69.6701, "tz": "Asia/Kolkata"},
    "VAOZ": {"iata": "ISK", "name": "Nashik Airport",                    "city": "Nashik",      "country": "India", "lat": 20.1196, "lon": 73.9134, "tz": "Asia/Kolkata"},
    # South India
    "VOAT": {"iata": "AGX", "name": "Agatti Airport",                    "city": "Agatti",      "country": "India", "lat": 10.8237, "lon": 72.1760, "tz": "Asia/Kolkata"},
    "VOCP": {"iata": "CDP", "name": "Kadapa Airport",                    "city": "Kadapa",      "country": "India", "lat": 14.5130, "lon": 78.7728, "tz": "Asia/Kolkata"},
    "VOTK": {"iata": "TCR", "name": "Thoothukudi Airport",               "city": "Thoothukudi", "country": "India", "lat":  8.7242, "lon": 77.9876, "tz": "Asia/Kolkata"},
    "VOSM": {"iata": "SXV", "name": "Salem Airport",                     "city": "Salem",       "country": "India", "lat": 11.7833, "lon": 78.0660, "tz": "Asia/Kolkata"},
    "VOPC": {"iata": "PNY", "name": "Pondicherry Airport",               "city": "Pondicherry", "country": "India", "lat": 11.9680, "lon": 79.8120, "tz": "Asia/Kolkata"},
}

IATA_TO_ICAO = {v["iata"]: k for k, v in AIRPORTS.items()}

# Common IATA-to-ICAO airline prefix mapping for ADS-B callsign matching
AIRLINE_IATA_TO_ICAO = {
    # Indian carriers
    "AI": "AIC", "6E": "IGO", "UK": "UKA", "SG": "SEJ", "IX": "AXB",
    "I5": "IAD", "QP": "ABT", "G8": "GOW", "S5": "SSW",
    # International carriers (common on Indian routes)
    "EK": "UAE", "QR": "QTR", "SV": "SVA", "EY": "ETD", "TK": "THY",
    "BA": "BAW", "LH": "DLH", "AF": "AFR", "KL": "KLM", "SQ": "SIA",
    "CX": "CPA", "QF": "QFA", "AA": "AAL", "DL": "DAL", "UA": "UAL",
    "JL": "JAL", "NH": "ANA", "PK": "PIA", "MU": "CES", "CA": "CCA",
    "UL": "ALK", "AC": "ACA", "LX": "SWR", "OS": "AUA", "AZ": "ITY",
    "IB": "IBE", "VS": "VIR", "MS": "MSR", "WY": "OMA", "GF": "GFA",
    "FZ": "FDB", "G9": "ABY", "KU": "KAC", "RJ": "RJA",
}

CACHE_TTL = 180
BACKGROUND_INTERVAL = 12
ADSB_CACHE_TTL = 60  # ADS-B cache refresh more frequently
MAX_PAGES = 6  # Fetch up to 6 pages (600 flights) per airport for background refresh
MAX_PAGES_DATE = 12  # Fetch up to 12 pages (1200 flights) for specific date requests
OPENSKY_CACHE_TTL = 1200   # 20 minutes — respects anonymous rate limits
ENRICHED_STATUS_CACHE_TTL = 900  # 15 minutes between external enrichment retries
LIVE_FLIGHTS_CACHE_TTL = 60  # Refresh live aircraft positions every 60 seconds

# ---------------------------------------------------------------------------
# Caches
# ---------------------------------------------------------------------------
_cache: dict = {}                  # Primary schedule data
_adsb_cache: dict = {}             # Live ADS-B data per airport
_history_cache: dict = {}          # Historical flights {icao: {direction: {date_str: [flights]}}}
_opensky_cache: dict = {}          # OpenSky track data {icao: {direction: {callsign: {firstSeen, lastSeen}}}}
_enriched_status_cache: dict = {}  # Externally-resolved statuses {"FN:DATE" -> {category, text, source, fetched_at}}
_live_flights_cache: dict = {"data": [], "fetched_at": 0}  # All airborne aircraft in India
_executor = ThreadPoolExecutor(max_workers=8)

# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------
_rate_limits: dict = defaultdict(list)  # IP -> [timestamps]
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX = 60  # max requests per window

# Dashboard access token (set on first page load)
_valid_tokens: dict = {}  # token -> expiry timestamp
TOKEN_TTL = 3600  # 1 hour


def _check_rate_limit(ip: str) -> bool:
    now = time.time()
    timestamps = _rate_limits[ip]
    # Clean old entries
    _rate_limits[ip] = [t for t in timestamps if now - t < RATE_LIMIT_WINDOW]
    if len(_rate_limits[ip]) >= RATE_LIMIT_MAX:
        return False
    _rate_limits[ip].append(now)
    return True


def _generate_token() -> str:
    token = secrets.token_urlsafe(32)
    _valid_tokens[token] = time.time() + TOKEN_TTL
    # Clean expired tokens
    now = time.time()
    expired = [t for t, exp in _valid_tokens.items() if exp < now]
    for t in expired:
        del _valid_tokens[t]
    return token


def _validate_token(token: str | None) -> bool:
    if not token:
        return False
    expiry = _valid_tokens.get(token)
    if not expiry:
        return False
    return time.time() < expiry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _safe_get(d, *keys, default=None):
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k)
        else:
            return default
        if d is None:
            return default
    return d


STATUS_CATEGORY_MAP = {
    "scheduled": "scheduled",
    "delayed": "delayed",
    "departed": "departed",
    "en route": "en_route",
    "landed": "landed",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "diverted": "diverted",
    "estimated": "on_time",
    "boarding": "boarding",
    "gate closed": "boarding",
    "taxiing": "departed",
    "on time": "on_time",
}

STATUS_COLOR_MAP = {
    "scheduled": "gray",
    "on_time": "green",
    "departed": "green",
    "en_route": "green",
    "landed": "green",
    "boarding": "yellow",
    "delayed": "yellow",
    "cancelled": "red",
    "diverted": "red",
    "unknown": "gray",
}


def _flight_number_to_callsign(flight_number: str, airline_icao: str) -> str:
    """Convert flight number like EK203 to ICAO callsign UAE203."""
    if not flight_number:
        return ""
    # Extract airline prefix and numeric part
    prefix = ""
    num = ""
    for i, c in enumerate(flight_number):
        if c.isdigit():
            prefix = flight_number[:i].strip()
            num = flight_number[i:].strip()
            break
    if not prefix or not num:
        return flight_number
    icao_prefix = AIRLINE_IATA_TO_ICAO.get(prefix, airline_icao or prefix)
    return f"{icao_prefix}{num}"


def _normalize_flight(raw: dict, direction: str, queried_icao: str = "", queried_info: dict | None = None) -> dict | None:
    try:
        flight = raw.get("flight", raw)
        qi = queried_info or {}

        flight_number = _safe_get(flight, "identification", "number", "default", default="")
        callsign = _safe_get(flight, "identification", "callsign", default="")

        airline_name = _safe_get(flight, "airline", "name", default="") or _safe_get(flight, "airline", "short", default="Unknown")
        airline_iata = _safe_get(flight, "airline", "code", "iata", default="")
        airline_icao = _safe_get(flight, "airline", "code", "icao", default="")

        status_text = _safe_get(flight, "status", "text", default="Unknown")
        status_key = status_text.lower().strip() if status_text else "unknown"

        if status_key.startswith("estimated"):
            status_category = "on_time"
            status_text = "On Time"
        elif status_key.startswith("landed"):
            status_category = "landed"
            status_text = "Landed"
        elif status_key.startswith("departed"):
            status_category = "departed"
            status_text = "Departed"
        elif status_key.startswith("delayed"):
            status_category = "delayed"
            status_text = "Delayed"
        elif status_key in ("canceled", "cancelled"):
            status_category = "cancelled"
            status_text = "Cancelled"
        else:
            status_category = STATUS_CATEGORY_MAP.get(status_key, "unknown")

        status_color = STATUS_COLOR_MAP.get(status_category, "gray")

        origin_iata = _safe_get(flight, "airport", "origin", "code", "iata", default="")
        origin_name = _safe_get(flight, "airport", "origin", "name", default="")
        origin_city = _safe_get(flight, "airport", "origin", "position", "region", "city", default="")
        origin_country = _safe_get(flight, "airport", "origin", "position", "country", "name", default="")

        if direction == "departures" and not origin_iata:
            origin_iata = qi.get("iata", "")
            origin_name = qi.get("name", "")
            origin_city = qi.get("city", "")
            origin_country = qi.get("country", "")

        dest_iata = _safe_get(flight, "airport", "destination", "code", "iata", default="")
        dest_name = _safe_get(flight, "airport", "destination", "name", default="")
        dest_city = _safe_get(flight, "airport", "destination", "position", "region", "city", default="")
        dest_country = _safe_get(flight, "airport", "destination", "position", "country", "name", default="")

        if direction == "arrivals" and not dest_iata:
            dest_iata = qi.get("iata", "")
            dest_name = qi.get("name", "")
            dest_city = qi.get("city", "")
            dest_country = qi.get("country", "")

        sched_dep = _safe_get(flight, "time", "scheduled", "departure", default=None)
        est_dep = _safe_get(flight, "time", "estimated", "departure", default=None)
        real_dep = _safe_get(flight, "time", "real", "departure", default=None)
        sched_arr = _safe_get(flight, "time", "scheduled", "arrival", default=None)
        est_arr = _safe_get(flight, "time", "estimated", "arrival", default=None)
        real_arr = _safe_get(flight, "time", "real", "arrival", default=None)

        if direction == "departures":
            gate = _safe_get(flight, "airport", "origin", "info", "gate", default=None)
            terminal = _safe_get(flight, "airport", "origin", "info", "terminal", default=None)
        else:
            gate = _safe_get(flight, "airport", "destination", "info", "gate", default=None)
            terminal = _safe_get(flight, "airport", "destination", "info", "terminal", default=None)

        aircraft_model = _safe_get(flight, "aircraft", "model", "text", default=None)
        aircraft_reg = _safe_get(flight, "aircraft", "registration", default=None)

        delay_minutes = None
        if direction == "departures" and sched_dep and est_dep:
            if isinstance(sched_dep, (int, float)) and isinstance(est_dep, (int, float)) and sched_dep > 0 and est_dep > 0:
                delay_minutes = round((est_dep - sched_dep) / 60)
        elif direction == "arrivals" and sched_arr and est_arr:
            if isinstance(sched_arr, (int, float)) and isinstance(est_arr, (int, float)) and sched_arr > 0 and est_arr > 0:
                delay_minutes = round((est_arr - sched_arr) / 60)

        if delay_minutes and delay_minutes > 15 and status_category in ("scheduled", "on_time", "unknown"):
            status_category = "delayed"
            status_color = "yellow"
            if not status_text or status_text.lower() in ("scheduled", "estimated", "unknown", "on time"):
                status_text = f"Delayed ({delay_minutes}m)"

        # Generate ICAO callsign for ADS-B matching
        expected_callsign = _flight_number_to_callsign(flight_number, airline_icao)

        return {
            "flight_number": flight_number or "",
            "callsign": callsign or "",
            "expected_callsign": expected_callsign,
            "airline": {
                "name": airline_name or "Unknown",
                "iata": airline_iata or "",
                "icao": airline_icao or "",
            },
            "origin": {
                "iata": origin_iata or "",
                "name": origin_name or "",
                "city": origin_city or "",
                "country": origin_country or "",
            },
            "destination": {
                "iata": dest_iata or "",
                "name": dest_name or "",
                "city": dest_city or "",
                "country": dest_country or "",
            },
            "times": {
                "scheduled_departure": sched_dep if isinstance(sched_dep, (int, float)) and sched_dep > 0 else None,
                "estimated_departure": est_dep if isinstance(est_dep, (int, float)) and est_dep > 0 else None,
                "actual_departure": real_dep if isinstance(real_dep, (int, float)) and real_dep > 0 else None,
                "scheduled_arrival": sched_arr if isinstance(sched_arr, (int, float)) and sched_arr > 0 else None,
                "estimated_arrival": est_arr if isinstance(est_arr, (int, float)) and est_arr > 0 else None,
                "actual_arrival": real_arr if isinstance(real_arr, (int, float)) and real_arr > 0 else None,
            },
            "delay_minutes": delay_minutes,
            "status": {
                "text": status_text or "Unknown",
                "category": status_category,
                "color": status_color,
            },
            "gate": str(gate) if gate else None,
            "terminal": str(terminal) if terminal else None,
            "aircraft": {
                "model": aircraft_model,
                "registration": aircraft_reg,
            },
            "verification": None,  # Will be filled by ADS-B cross-check
        }
    except Exception as e:
        logger.warning("Failed to normalize flight: %s", e)
        return None


# ---------------------------------------------------------------------------
# Schedule data fetching (public aviation data feed, timestamp-based)
# ---------------------------------------------------------------------------
_SCHED_API_URL = "https://api.flightradar24.com/common/v1/airport.json"
_SCHED_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}


def _fetch_schedule_page(icao: str, direction: str, timestamp: int, page: int = 1) -> list:
    """Fetch a single page of schedule data from the aviation data feed."""
    params = {
        "code": icao,
        "format": "json",
        "limit": 100,
        "page": page,
        "plugin[]": "schedule",
        "plugin-setting[schedule][mode]": direction,
        "plugin-setting[schedule][timestamp]": timestamp,
    }
    resp = http_requests.get(_SCHED_API_URL, params=params, headers=_SCHED_HEADERS, timeout=15)
    if resp.status_code != 200:
        return []
    data = resp.json()
    result = _safe_get(data, "result", "response")
    if not result:
        return []
    schedule = _safe_get(result, "airport", "pluginData", "schedule")
    if not schedule:
        return []
    return _safe_get(schedule, direction, "data", default=[])


def _fetch_airport_data(icao: str) -> dict | None:
    """Fetch full-day flight data by starting from midnight in airport local time."""
    try:
        airport_info = AIRPORTS.get(icao, {})
        airport_tz = ZoneInfo(airport_info.get("tz", "UTC"))

        # Get midnight today in airport local time
        now_local = datetime.now(tz=airport_tz)
        midnight_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        midnight_ts = int(midnight_local.timestamp())

        all_departures = []
        all_arrivals = []

        for direction in ("departures", "arrivals"):
            all_flights = []
            for page in range(1, MAX_PAGES + 1):
                try:
                    raw = _fetch_schedule_page(icao, direction, midnight_ts, page)
                    if not raw:
                        break
                    flights = [f for f in (
                        _normalize_flight(r, direction, icao, airport_info) for r in raw
                    ) if f is not None]
                    all_flights.extend(flights)
                    if len(raw) < 100:
                        break
                except Exception as e:
                    logger.warning("Error fetching %s page %d for %s: %s", direction, page, icao, e)
                    break
                time.sleep(1)

            if direction == "departures":
                all_departures = all_flights
            else:
                all_arrivals = all_flights

        if not all_departures and not all_arrivals:
            return None

        # Deduplicate by flight number + scheduled time
        seen_dep = set()
        unique_dep = []
        for f in all_departures:
            key = (f["flight_number"], f["times"].get("scheduled_departure"))
            if key not in seen_dep:
                seen_dep.add(key)
                unique_dep.append(f)

        seen_arr = set()
        unique_arr = []
        for f in all_arrivals:
            key = (f["flight_number"], f["times"].get("scheduled_arrival"))
            if key not in seen_arr:
                seen_arr.add(key)
                unique_arr.append(f)

        return {"departures": unique_dep, "arrivals": unique_arr}

    except Exception as e:
        logger.error("Error fetching %s: %s", icao, e)
        return None


def _fetch_airport_data_for_date(icao: str, date_str: str) -> dict | None:
    """Fetch flight data for a specific date by setting timestamp to midnight of that date."""
    try:
        airport_info = AIRPORTS.get(icao, {})
        airport_tz = ZoneInfo(airport_info.get("tz", "UTC"))

        target_date = datetime.strptime(date_str, "%Y-%m-%d")
        target_midnight = target_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=airport_tz)
        target_ts = int(target_midnight.timestamp())

        all_departures = []
        all_arrivals = []

        for direction in ("departures", "arrivals"):
            all_flights = []
            for page in range(1, MAX_PAGES_DATE + 1):
                try:
                    raw = _fetch_schedule_page(icao, direction, target_ts, page)
                    if not raw:
                        break
                    flights = [f for f in (
                        _normalize_flight(r, direction, icao, airport_info) for r in raw
                    ) if f is not None]
                    all_flights.extend(flights)
                    if len(raw) < 100:
                        break
                except Exception as e:
                    logger.warning("Error fetching %s page %d for %s (date %s): %s", direction, page, icao, date_str, e)
                    break
                time.sleep(1)

            if direction == "departures":
                all_departures = all_flights
            else:
                all_arrivals = all_flights

        if not all_departures and not all_arrivals:
            return None

        # Deduplicate
        seen_dep = set()
        unique_dep = []
        for f in all_departures:
            key = (f["flight_number"], f["times"].get("scheduled_departure"))
            if key not in seen_dep:
                seen_dep.add(key)
                unique_dep.append(f)

        seen_arr = set()
        unique_arr = []
        for f in all_arrivals:
            key = (f["flight_number"], f["times"].get("scheduled_arrival"))
            if key not in seen_arr:
                seen_arr.add(key)
                unique_arr.append(f)

        return {"departures": unique_dep, "arrivals": unique_arr}

    except Exception as e:
        logger.error("Error fetching %s for date %s: %s", icao, date_str, e)
        return None


# ---------------------------------------------------------------------------
# ADS-B verification (Airplanes.live)
# ---------------------------------------------------------------------------
ADSB_API_BASE = "https://api.airplanes.live/v2"


def _fetch_adsb_for_airport(icao: str) -> dict | None:
    """Fetch all aircraft within 30nm of an airport from Airplanes.live."""
    airport = AIRPORTS.get(icao)
    if not airport:
        return None
    try:
        url = f"{ADSB_API_BASE}/point/{airport['lat']}/{airport['lon']}/50"
        resp = http_requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        aircraft = data.get("ac", [])

        # Build a set of active callsigns (trimmed)
        callsigns = set()
        aircraft_details = {}
        for ac in aircraft:
            cs = (ac.get("flight") or "").strip()
            if cs:
                callsigns.add(cs.upper())
                aircraft_details[cs.upper()] = {
                    "alt": ac.get("alt_baro"),
                    "gs": ac.get("gs"),
                    "lat": ac.get("lat"),
                    "lon": ac.get("lon"),
                    "on_ground": ac.get("alt_baro") == "ground",
                }

        return {"callsigns": callsigns, "details": aircraft_details, "total": len(aircraft)}
    except Exception as e:
        logger.warning("ADS-B fetch error for %s: %s", icao, e)
        return None


# ---------------------------------------------------------------------------
# OpenSky Network — flight track history (free, no key, 1h delay for anon)
# ---------------------------------------------------------------------------
OPENSKY_API_BASE = "https://opensky-network.org/api"


def _fetch_opensky_tracks(icao: str, direction: str) -> dict | None:
    """
    Fetch actual departure/arrival tracks from OpenSky for the past 4 hours.
    Used under the OpenSky Network non-commercial Terms of Use (ODbL license).
    See: https://opensky-network.org/about/terms-of-use

    OpenSky data has a ~1h delay for anonymous access, so this resolves
    'unknown'/'scheduled' flights that are 1+ hours past their scheduled time.
    Rate: 30 req per ~25-min cycle ≈ 86 req/day, well within the free-tier limit.
    """
    try:
        endpoint = "departure" if direction == "departures" else "arrival"
        now_ts = int(time.time())
        begin_ts = now_ts - 14400  # 4 hours ago
        url = f"{OPENSKY_API_BASE}/flights/{endpoint}"
        params = {"airport": icao, "begin": begin_ts, "end": now_ts}
        resp = http_requests.get(url, params=params, timeout=20)
        if resp.status_code == 429:
            logger.warning("OpenSky rate limit reached, backing off")
            return None
        if resp.status_code in (401, 403):
            logger.warning("OpenSky auth required — anonymous access may be restricted. See terms-of-use.")
            return None
        if resp.status_code == 404:
            return {"callsign_map": {}, "total": 0, "fetched_at": time.time()}
        if resp.status_code != 200:
            return None
        tracks = resp.json() or []
        callsign_map: dict = {}
        for t in tracks:
            cs = (t.get("callsign") or "").strip().upper()
            if not cs:
                continue
            first_seen = t.get("firstSeen")
            last_seen  = t.get("lastSeen")
            callsign_map[cs] = {
                "firstSeen": first_seen,
                "lastSeen":  last_seen,
                "icao24":    t.get("icao24", ""),
                "estDep":    t.get("estDepartureAirport", ""),
                "estArr":    t.get("estArrivalAirport", ""),
            }
        logger.info("OpenSky %s %s: %d tracks", icao, direction, len(callsign_map))
        return {"callsign_map": callsign_map, "total": len(tracks), "fetched_at": time.time()}
    except Exception as e:
        logger.warning("OpenSky fetch error for %s/%s: %s", icao, direction, e)
        return None


def _get_opensky_status(flight: dict, icao: str, direction: str) -> dict | None:
    """
    Look up a flight in the OpenSky cache and return inferred status if found.
    Returns a status dict or None if not found / cache stale.
    """
    entry = _opensky_cache.get(icao, {}).get(direction)
    if not entry:
        return None
    age = time.time() - entry.get("fetched_at", 0)
    if age > OPENSKY_CACHE_TTL:
        return None

    callsign_map = entry.get("callsign_map", {})

    # Try multiple callsign variants
    candidates = []
    if flight.get("expected_callsign"):
        candidates.append(flight["expected_callsign"].upper())
    if flight.get("callsign"):
        candidates.append(flight["callsign"].upper())
    fn = (flight.get("flight_number") or "").replace(" ", "").upper()
    if fn:
        candidates.append(fn)

    for cs in candidates:
        if cs in callsign_map:
            track = callsign_map[cs]
            first = track.get("firstSeen")
            if first and isinstance(first, (int, float)) and first > 0:
                return {
                    "callsign":  cs,
                    "firstSeen": first,
                    "lastSeen":  track.get("lastSeen"),
                    "icao24":    track.get("icao24"),
                }
    return None


# ---------------------------------------------------------------------------
# Extra status enrichment — ixigo / airline website / airport fallback layer
# ---------------------------------------------------------------------------

def _map_external_status(s: str) -> tuple[str, str] | None:
    """Map an arbitrary status string from an external source to (category, display_text)."""
    s = (s or "").lower().strip()
    if not s or s in ("unknown", "-", "n/a", ""):
        return None
    if any(x in s for x in ("cancel",)):
        return "cancelled", "Cancelled"
    if any(x in s for x in ("land", "arrived", "arrival complete", "arrived at")):
        return "landed", "Landed"
    if any(x in s for x in ("depart", "airborne", "in flight", "en route", "enroute", "inflight")):
        return "departed", "Departed"
    if "delay" in s:
        return "delayed", "Delayed"
    if "board" in s:
        return "boarding", "Boarding"
    if any(x in s for x in ("on time", "ontime")):
        return "on_time", "On Time"
    return None


def _fetch_ixigo_status(flight_number: str, date_str: str) -> tuple[str, str, str] | None:
    """
    Try ixigo's aggregated flight-status API.
    ixigo aggregates status from airlines and airports, making it useful as a fallback.
    Returns (category, display_text, "ixigo") or None.
    """
    fn = flight_number.replace(" ", "").upper()
    date_compact = date_str.replace("-", "")  # YYYYMMDD
    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.ixigo.com/",
        "X-Requested-With": "XMLHttpRequest",
    }
    # Try several endpoint + parameter format variations
    attempts = [
        ("https://www.ixigo.com/api/v1/flights/flight-status",
         {"flightNumber": fn, "flightDate": date_compact}),
        ("https://www.ixigo.com/api/v1/flights/flight-status",
         {"flightNumber": fn, "date": date_str}),
        ("https://www.ixigo.com/api/v2/flights/flight-status",
         {"flightNumber": fn, "flightDate": date_compact}),
    ]
    for url, params in attempts:
        try:
            resp = http_requests.get(url, params=params, headers=headers, timeout=8)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except ValueError:
                    continue
                # ixigo may return a list or a dict
                if isinstance(data, list) and data:
                    data = data[0]
                if not isinstance(data, dict):
                    continue
                # Extract status from various possible field names
                status_str = (
                    data.get("flightStatus")
                    or data.get("status")
                    or data.get("currentStatus")
                    or data.get("flightStatusDescription")
                    or _safe_get(data, "flightInfo", "status")
                    or _safe_get(data, "data", "flightStatus")
                    or ""
                )
                mapped = _map_external_status(status_str)
                if mapped:
                    logger.info("ixigo resolved %s on %s → %s", fn, date_str, mapped[0])
                    return mapped[0], mapped[1], "ixigo"
        except Exception as e:
            logger.debug("ixigo endpoint %s error for %s: %s", url, fn, e)
    return None


def _fetch_airline_status(
    flight_number: str, airline_iata: str, date_str: str
) -> tuple[str, str, str] | None:
    """
    Try airline-specific public status endpoints.
    Covers Indian carriers, Middle East carriers, and common international carriers on India routes.
    Best-effort — many endpoints require JS/auth so failures are expected.
    Returns (category, display_text, source_name) or None.
    """
    fn = flight_number.replace(" ", "").upper()
    num_part = "".join(c for c in fn if c.isdigit())
    date_compact = date_str.replace("-", "")  # YYYYMMDD
    base_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/html, */*",
    }

    def _try(url, params, headers, source, *field_paths):
        """GET url, return mapped status tuple or None. field_paths are tried in order."""
        try:
            r = http_requests.get(url, params=params, headers=headers, timeout=8)
            if r.status_code != 200:
                return None
            data = r.json()
            if isinstance(data, list) and data:
                data = data[0]
            if not isinstance(data, dict):
                return None
            for path in field_paths:
                val = _safe_get(data, *path) if isinstance(path, tuple) else data.get(path, "")
                if val:
                    mapped = _map_external_status(str(val))
                    if mapped:
                        logger.info("%s resolved %s on %s → %s", source, fn, date_str, mapped[0])
                        return mapped[0], mapped[1], source
        except Exception as e:
            logger.debug("%s error for %s: %s", source, fn, e)
        return None

    try:
        # --- Indian carriers ---
        if airline_iata == "AI":
            return (
                _try("https://www.airindia.com/api/flight-status",
                     {"flightNumber": f"AI{num_part}", "departureDate": date_str},
                     {**base_headers, "Referer": "https://www.airindia.com/"},
                     "Air India", "status", "flightStatus", ("data", "status"), ("flight", "status"))
                or _try("https://www.airindia.com/content/dam/air-india/APIs/flightstatus",
                        {"fn": f"AI{num_part}", "date": date_str},
                        base_headers, "Air India", "status", "flightStatus")
            )

        elif airline_iata in ("UK", "IX"):
            # Vistara merged into Air India; Air India Express same portal
            airline_label = "Air India Express" if airline_iata == "IX" else "Air India"
            base_url = "https://www.airindiaexpress.com/api/flight-status" if airline_iata == "IX" else "https://www.airindia.com/api/flight-status"
            return _try(base_url,
                        {"flightNumber": f"{airline_iata}{num_part}", "departureDate": date_str},
                        {**base_headers, "Referer": f"https://{'www.airindiaexpress.com' if airline_iata == 'IX' else 'www.airindia.com'}/"},
                        airline_label, "status", "flightStatus", ("data", "status"))

        elif airline_iata == "6E":
            return (
                _try("https://book.goindigo.in/api/v1/flightstatus",
                     {"flightNumber": f"6E{num_part}", "date": date_str},
                     {**base_headers, "Referer": "https://www.goindigo.in/"},
                     "IndiGo", "status", "flightStatus", ("flightInfo", "status"), ("data", "flightStatus"))
                or _try("https://www.goindigo.in/api/flight-status",
                        {"flightNumber": f"6E{num_part}", "journeyDate": date_str},
                        {**base_headers, "Referer": "https://www.goindigo.in/"},
                        "IndiGo", "status", "flightStatus")
            )

        elif airline_iata == "SG":
            return _try("https://www.spicejet.com/api/flight-status",
                        {"flightNumber": f"SG{num_part}", "departureDate": date_str},
                        {**base_headers, "Referer": "https://www.spicejet.com/"},
                        "SpiceJet", "status", "flightStatus", ("data", "status"))

        elif airline_iata == "QP":
            return _try("https://www.akasaair.com/api/flightstatus",
                        {"flightNumber": f"QP{num_part}", "date": date_str},
                        {**base_headers, "Referer": "https://www.akasaair.com/"},
                        "Akasa Air", "status", "flightStatus", ("data", "status"))

        elif airline_iata == "I5":
            return _try("https://www.airasia.com/api/v1/flight-status",
                        {"flightNumber": f"I5{num_part}", "departureDate": date_str},
                        {**base_headers, "Referer": "https://www.airasia.com/"},
                        "AIX Connect", "status", "flightStatus", ("data", "status"))

        # --- Middle East carriers ---
        elif airline_iata == "EK":
            return _try("https://www.emirates.com/api/flight-status/search",
                        {"flightNumber": f"EK{num_part}", "departureDate": date_str},
                        {**base_headers, "Referer": "https://www.emirates.com/"},
                        "Emirates", "status", "flightStatus", ("flights", 0, "status"))

        elif airline_iata == "QR":
            return _try("https://www.qatarairways.com/api/v1/flight-status",
                        {"flightNumber": f"QR{num_part}", "departureDate": date_str},
                        {**base_headers, "Referer": "https://www.qatarairways.com/"},
                        "Qatar Airways", "status", ("flightStatus", "text"), ("data", "status"))

        elif airline_iata == "EY":
            return _try("https://www.etihad.com/api/flight-status",
                        {"flightNumber": f"EY{num_part}", "departureDate": date_str},
                        {**base_headers, "Referer": "https://www.etihad.com/"},
                        "Etihad", "status", ("flight", "status"))

        elif airline_iata == "TK":
            return _try("https://www.turkishairlines.com/api/flight-status",
                        {"flightNumber": f"TK{num_part}", "flightDate": date_compact},
                        {**base_headers, "Referer": "https://www.turkishairlines.com/"},
                        "Turkish Airlines", "status", ("flights", 0, "status"))

        elif airline_iata == "SV":
            return _try("https://www.saudia.com/api/flight-status",
                        {"flightNumber": f"SV{num_part}", "flightDate": date_str},
                        {**base_headers, "Referer": "https://www.saudia.com/"},
                        "Saudia", "status", "flightStatus")

        elif airline_iata == "FZ":
            return _try("https://www.flydubai.com/api/flight-status",
                        {"flightNumber": f"FZ{num_part}", "departureDate": date_str},
                        {**base_headers, "Referer": "https://www.flydubai.com/"},
                        "flydubai", "status", "flightStatus", ("data", "status"))

        elif airline_iata == "G9":
            return _try("https://www.airarabia.com/api/flight-status",
                        {"flightNumber": f"G9{num_part}", "departureDate": date_str},
                        {**base_headers, "Referer": "https://www.airarabia.com/"},
                        "Air Arabia", "status", "flightStatus")

        elif airline_iata == "WY":
            return _try("https://www.omanair.com/api/flight-status",
                        {"flightNumber": f"WY{num_part}", "departureDate": date_str},
                        {**base_headers, "Referer": "https://www.omanair.com/"},
                        "Oman Air", "status", "flightStatus", ("data", "status"))

        elif airline_iata == "GF":
            return _try("https://www.gulfair.com/api/flight-status",
                        {"flightNumber": f"GF{num_part}", "departureDate": date_str},
                        {**base_headers, "Referer": "https://www.gulfair.com/"},
                        "Gulf Air", "status", "flightStatus")

        # --- Other international carriers common on India routes ---
        elif airline_iata == "SQ":
            return _try("https://www.singaporeair.com/api/v1/flight-status",
                        {"flightNumber": f"SQ{num_part}", "departureDate": date_str},
                        {**base_headers, "Referer": "https://www.singaporeair.com/"},
                        "Singapore Airlines", "status", ("flightStatus", "description"), ("data", "status"))

        elif airline_iata == "BA":
            return _try("https://www.britishairways.com/travel/flightstatus/public/en_in/api",
                        {"flightNumber": f"BA{num_part}", "scheduledDepartureDate": date_str},
                        {**base_headers, "Referer": "https://www.britishairways.com/"},
                        "British Airways", "status", "flightStatus", ("legs", 0, "status"))

        elif airline_iata == "LH":
            return _try("https://api.lufthansa.com/v1/operations/flightstatus",
                        {"flightNumber": f"LH{num_part}", "date": date_str},
                        {**base_headers, "Referer": "https://www.lufthansa.com/"},
                        "Lufthansa", ("FlightStatusResource", "Flights", "Flight", 0, "FlightStatus", "Definition"))

        elif airline_iata == "AF":
            return _try("https://www.airfrance.com/api/v1/flight-status",
                        {"flightNumber": f"AF{num_part}", "departureDate": date_str},
                        {**base_headers, "Referer": "https://www.airfrance.com/"},
                        "Air France", "status", "flightStatus", ("data", "status"))

        elif airline_iata == "KL":
            return _try("https://www.klm.com/api/v1/flight-status",
                        {"flightNumber": f"KL{num_part}", "departureDate": date_str},
                        {**base_headers, "Referer": "https://www.klm.com/"},
                        "KLM", "status", "flightStatus", ("data", "status"))

        elif airline_iata == "MS":
            return _try("https://www.egyptair.com/api/flight-status",
                        {"flightNumber": f"MS{num_part}", "departureDate": date_str},
                        {**base_headers, "Referer": "https://www.egyptair.com/"},
                        "EgyptAir", "status", "flightStatus")

        elif airline_iata == "PK":
            return _try("https://www.piac.com.pk/api/flight-status",
                        {"flightNumber": f"PK{num_part}", "departureDate": date_str},
                        {**base_headers, "Referer": "https://www.piac.com.pk/"},
                        "PIA", "status", "flightStatus", ("data", "status"))

    except Exception as e:
        logger.debug("Airline status fetch error %s/%s: %s", flight_number, airline_iata, e)

    return None


def _try_enrich_flight_status(
    flight_number: str, airline_iata: str, date_str: str
) -> tuple[str, str, str] | None:
    """
    Try all external enrichment sources in order.
    Priority: ixigo (aggregated Indian OTA) → airline official endpoint.
    Covers Indian carriers (AI, 6E, SG, IX, QP, I5, UK), Middle East (EK, QR, EY, TK, SV, FZ, G9, WY, GF),
    and other internationals common on India routes (SQ, BA, LH, AF, KL, MS, PK).
    Returns (category, display_text, source_name) or None.
    """
    result = _fetch_ixigo_status(flight_number, date_str)
    if result:
        return result
    return _fetch_airline_status(flight_number, airline_iata, date_str)


def _match_callsign(flight_num: str, expected_cs: str, raw_cs: str, active_callsigns: set) -> tuple[bool, str | None]:
    """Try multiple callsign formats and return (is_tracked, matched_callsign)."""
    candidates = []
    if expected_cs:
        candidates.append(expected_cs.upper())
    if raw_cs:
        candidates.append(raw_cs.upper())
    if flight_num:
        fn = flight_num.replace(" ", "").upper()
        candidates.append(fn)
        # Also try with airline ICAO prefix derived from IATA (e.g. EK203 → try UAE203 already in expected_cs)
        # Try just removing spaces variations
        if len(fn) > 2:
            candidates.append(fn[:2] + fn[2:].lstrip("0") or fn[:2] + "0")

    for cs in candidates:
        if cs and cs in active_callsigns:
            return True, cs
    return False, None


def _infer_status_from_time_and_adsb(
    status_cat: str,
    sched_time: float | None,
    actual_time: float | None,
    is_tracked: bool,
    adsb_info: dict,
    direction: str,
    now: float,
) -> tuple[str, str, str] | None:
    """
    For 'unknown' or 'scheduled' flights, infer likely status from timing + ADS-B.
    Returns (new_category, new_text, confidence) or None if no inference possible.
    """
    if status_cat not in ("unknown", "scheduled", "on_time"):
        return None

    if not sched_time or not isinstance(sched_time, (int, float)) or sched_time <= 0:
        return None

    overdue_sec = now - sched_time  # positive = past scheduled time

    # Flight not yet due
    if overdue_sec < -1800:  # >30 min in the future
        return None

    # ---- ADS-B says aircraft is AIRBORNE near this airport ----
    if is_tracked and not adsb_info.get("on_ground", False):
        if direction == "departures":
            return ("departed", "Departed", "medium")
        else:
            return ("en_route", "En Route", "medium")

    # ---- ADS-B says aircraft is ON GROUND near this airport ----
    if is_tracked and adsb_info.get("on_ground", False):
        if overdue_sec > 600:  # 10+ min past scheduled, still on ground → delayed
            return ("delayed", "Delayed", "medium")
        else:
            return ("boarding", "Boarding", "medium")

    # ---- No ADS-B signal — use time heuristics ----
    # NOTE: Lack of ADS-B signal does NOT imply cancellation — many flights operate
    # without ADS-B coverage.  Only mark as cancelled when we have a positive signal
    # (e.g. from the schedule API or external enrichment).
    if not is_tracked:
        if overdue_sec > 14400:  # 4h+ overdue with no signal — status unknown
            return ("delayed", "No Update Available", "low")
        elif overdue_sec > 7200:  # 2-4h overdue
            return ("delayed", "No Update Available", "low")
        elif overdue_sec > 900:  # 15+ min overdue → delayed
            return ("delayed", f"Delayed ({int(overdue_sec / 60)}m+)", "low")
        elif overdue_sec > 0:  # Just past scheduled but no signal yet (could be boarding)
            return ("boarding", "Due Now", "low")

    return None


def _verify_flights_with_adsb(flights: list, icao: str, direction: str) -> list:
    """Cross-verify flight statuses with ADS-B data, and infer status for unknown/scheduled flights."""
    adsb_entry = _adsb_cache.get(icao)
    adsb_available = adsb_entry and (time.time() - adsb_entry.get("fetched_at", 0) <= ADSB_CACHE_TTL * 3)

    if not adsb_available:
        for f in flights:
            f["verification"] = {"status": "no_data", "source": "ADS-B"}
        return flights

    active_callsigns = adsb_entry.get("callsigns", set())
    adsb_details = adsb_entry.get("details", {})
    now = time.time()

    for f in flights:
        expected_cs = f.get("expected_callsign", "")
        raw_cs = f.get("callsign", "")
        flight_num = f.get("flight_number", "")

        is_tracked, matched_cs = _match_callsign(flight_num, expected_cs, raw_cs, active_callsigns)
        adsb_info = adsb_details.get(matched_cs, {}) if matched_cs else {}
        status_cat = f["status"]["category"]

        if direction == "departures":
            sched_time = f["times"].get("scheduled_departure")
            actual_time = f["times"].get("actual_departure") or f["times"].get("estimated_departure")
        else:
            sched_time = f["times"].get("scheduled_arrival")
            actual_time = f["times"].get("actual_arrival") or f["times"].get("estimated_arrival")

        # ----------------------------------------------------------------
        # For unknown/scheduled flights: try to infer real status first
        # ----------------------------------------------------------------
        if status_cat in ("unknown", "scheduled", "on_time") and sched_time:
            # 1. Check OpenSky track history — confirms actual departure/arrival
            opensky_track = _get_opensky_status(f, icao, direction)
            if opensky_track:
                first_seen = opensky_track["firstSeen"]
                last_seen  = opensky_track.get("lastSeen")
                if direction == "departures":
                    new_cat  = "departed"
                    new_text = f"Departed {datetime.fromtimestamp(first_seen, tz=timezone.utc).strftime('%H:%M')} UTC"
                else:
                    new_cat  = "landed" if last_seen and last_seen < now - 600 else "en_route"
                    new_text = "Landed" if new_cat == "landed" else "En Route"
                f["status"]["category"] = new_cat
                f["status"]["color"]    = STATUS_COLOR_MAP.get(new_cat, "gray")
                f["status"]["text"]     = new_text
                status_cat = new_cat
                f["verification"] = {
                    "status":  "confirmed",
                    "detail":  f"Actual track confirmed by OpenSky (icao24: {opensky_track.get('icao24', '?')})",
                    "source":  "OpenSky",
                    "confidence": "high",
                }
                continue

            # 2. ADS-B + timing inference
            inferred = _infer_status_from_time_and_adsb(
                status_cat, sched_time, actual_time, is_tracked, adsb_info, direction, now
            )
            if inferred:
                new_cat, new_text, conf = inferred
                if status_cat == "unknown" or new_cat in ("departed", "en_route", "boarding"):
                    f["status"]["category"] = new_cat
                    f["status"]["color"] = STATUS_COLOR_MAP.get(new_cat, "gray")
                    if f["status"]["text"].lower() in ("unknown", "scheduled", "on time", "estimated"):
                        f["status"]["text"] = new_text
                    status_cat = new_cat
                    f["verification"] = {
                        "status": "inferred",
                        "detail": f"Inferred from ADS-B + timing ({conf} confidence)",
                        "source": "ADS-B+Time",
                        "confidence": conf,
                    }
                    continue

            # 3. Externally-enriched status (pre-populated by background enrichment task)
            #    Applies to past-scheduled flights (>15 min overdue) with no ADS-B/OpenSky resolution.
            if status_cat in ("unknown", "scheduled") and now - sched_time > 900:
                fn_key = (
                    f"{flight_num}:"
                    f"{datetime.fromtimestamp(sched_time, tz=timezone.utc).strftime('%Y-%m-%d')}"
                )
                enriched = _enriched_status_cache.get(fn_key)
                if (
                    enriched
                    and enriched.get("resolved")
                    and now - enriched.get("fetched_at", 0) < ENRICHED_STATUS_CACHE_TTL
                ):
                    new_cat = enriched["category"]
                    new_text = enriched["text"]
                    source   = enriched["source"]
                    f["status"]["category"] = new_cat
                    f["status"]["color"]    = STATUS_COLOR_MAP.get(new_cat, "gray")
                    f["status"]["text"]     = new_text
                    status_cat = new_cat
                    f["verification"] = {
                        "status":     "confirmed",
                        "detail":     f"Status resolved via {source}",
                        "source":     source,
                        "confidence": "medium",
                    }
                    continue

        # ----------------------------------------------------------------
        # Standard ADS-B verification for all other statuses
        # ----------------------------------------------------------------
        if status_cat == "cancelled":
            if is_tracked:
                f["verification"] = {
                    "status": "discrepancy",
                    "detail": "ADS-B shows aircraft active despite cancellation",
                    "source": "ADS-B",
                    "confidence": "check",
                }
            else:
                f["verification"] = {
                    "status": "confirmed",
                    "detail": "No aircraft signal — consistent with cancellation",
                    "source": "ADS-B",
                    "confidence": "high",
                }

        elif status_cat in ("departed", "en_route"):
            if is_tracked and not adsb_info.get("on_ground", False):
                f["verification"] = {
                    "status": "confirmed",
                    "detail": "Aircraft airborne on ADS-B",
                    "source": "ADS-B",
                    "confidence": "high",
                }
            elif is_tracked and adsb_info.get("on_ground", False):
                f["verification"] = {
                    "status": "confirmed",
                    "detail": "Aircraft on ground (taxiing/just departed)",
                    "source": "ADS-B",
                    "confidence": "medium",
                }
            else:
                f["verification"] = {
                    "status": "unverified",
                    "detail": "Aircraft likely out of ADS-B range",
                    "source": "ADS-B",
                    "confidence": "low",
                }

        elif status_cat in ("on_time", "scheduled", "boarding"):
            if sched_time and sched_time > now:
                f["verification"] = {
                    "status": "pending",
                    "detail": "Scheduled — awaiting departure window",
                    "source": "ADS-B",
                    "confidence": "pending",
                }
            elif is_tracked:
                f["verification"] = {
                    "status": "confirmed",
                    "detail": "Aircraft detected near airport",
                    "source": "ADS-B",
                    "confidence": "medium",
                }
            else:
                f["verification"] = {
                    "status": "unverified",
                    "detail": "No ADS-B signal yet",
                    "source": "ADS-B",
                    "confidence": "low",
                }

        elif status_cat == "landed":
            if is_tracked:
                f["verification"] = {
                    "status": "confirmed",
                    "detail": "Aircraft detected on ground",
                    "source": "ADS-B",
                    "confidence": "high",
                }
            else:
                f["verification"] = {
                    "status": "confirmed",
                    "detail": "Transponder off — normal post-landing",
                    "source": "ADS-B",
                    "confidence": "medium",
                }

        elif status_cat == "delayed":
            if is_tracked and adsb_info.get("on_ground", False):
                f["verification"] = {
                    "status": "confirmed",
                    "detail": "Aircraft on ground, confirming delay",
                    "source": "ADS-B",
                    "confidence": "high",
                }
            else:
                f["verification"] = {
                    "status": "confirmed",
                    "detail": "Delay reported by airline",
                    "source": "Airline",
                    "confidence": "medium",
                }

        else:  # unknown — no inference was possible
            if is_tracked:
                f["verification"] = {
                    "status": "inferred",
                    "detail": "Aircraft detected near airport",
                    "source": "ADS-B",
                    "confidence": "low",
                }
            else:
                f["verification"] = {
                    "status": "unverified",
                    "detail": "No data available",
                    "source": "ADS-B",
                    "confidence": "low",
                }

    return flights


# ---------------------------------------------------------------------------
# Cache management
# ---------------------------------------------------------------------------
def _update_cache(icao: str, data: dict):
    now = time.time()
    _cache[icao] = {
        "departures": {"data": data["departures"], "fetched_at": now},
        "arrivals": {"data": data["arrivals"], "fetched_at": now},
    }
    # Also store in history cache by date
    _store_history(icao, data)


def _store_history(icao: str, data: dict):
    """Store flights by date for historical access, using airport local time."""
    if icao not in _history_cache:
        _history_cache[icao] = {"departures": {}, "arrivals": {}}

    airport_tz_name = AIRPORTS.get(icao, {}).get("tz", "UTC")
    airport_tz = ZoneInfo(airport_tz_name)

    for direction in ("departures", "arrivals"):
        for flight in data[direction]:
            ts = None
            if direction == "departures":
                ts = flight["times"].get("scheduled_departure")
            else:
                ts = flight["times"].get("scheduled_arrival")
            if not ts:
                continue
            # Use airport local time for date grouping
            date_str = datetime.fromtimestamp(ts, tz=airport_tz).strftime("%Y-%m-%d")
            if date_str not in _history_cache[icao][direction]:
                _history_cache[icao][direction][date_str] = {}
            key = (flight["flight_number"], ts)
            _history_cache[icao][direction][date_str][key] = flight

    # Clean old dates (keep 4 days back)
    now_local = datetime.now(tz=airport_tz)
    cutoff = (now_local - timedelta(days=4)).strftime("%Y-%m-%d")
    for direction in ("departures", "arrivals"):
        old_dates = [d for d in _history_cache[icao][direction] if d < cutoff]
        for d in old_dates:
            del _history_cache[icao][direction][d]


def _get_cached(icao: str, direction: str) -> tuple[list | None, bool]:
    entry = _safe_get(_cache, icao, direction)
    if entry is None:
        return None, False
    age = time.time() - entry["fetched_at"]
    return entry["data"], age < CACHE_TTL


def _get_flights_for_date(icao: str, direction: str, date_str: str) -> list | None:
    """Get flights for a specific date from history cache."""
    history = _safe_get(_history_cache, icao, direction, date_str)
    if history:
        return list(history.values())
    return None


def _compute_stats(flights: list) -> dict:
    total = len(flights)
    on_time = sum(1 for f in flights if f["status"]["category"] in ("on_time", "departed", "en_route", "landed", "boarding"))
    delayed = sum(1 for f in flights if f["status"]["category"] == "delayed")
    cancelled = sum(1 for f in flights if f["status"]["category"] == "cancelled")
    return {
        "total": total,
        "on_time": on_time,
        "delayed": delayed,
        "cancelled": cancelled,
        "on_time_percentage": round(on_time / total * 100, 1) if total > 0 else 0,
    }


# ---------------------------------------------------------------------------
# Background refresh tasks
# ---------------------------------------------------------------------------
_backoff_interval = BACKGROUND_INTERVAL


async def _background_refresh_schedule():
    global _backoff_interval
    logger.info("Schedule background refresh started")
    loop = asyncio.get_event_loop()

    while True:
        for icao in AIRPORTS:
            try:
                data = await loop.run_in_executor(_executor, _fetch_airport_data, icao)
                if data:
                    _update_cache(icao, data)
                    _backoff_interval = BACKGROUND_INTERVAL
                    logger.info("Schedule refreshed %s (%s): %d dep, %d arr",
                                icao, AIRPORTS[icao]["iata"],
                                len(data["departures"]), len(data["arrivals"]))
                else:
                    logger.warning("No schedule data for %s", icao)
            except Exception as e:
                logger.error("Schedule refresh error for %s: %s", icao, e)
                _backoff_interval = min(_backoff_interval * 2, 120)

            await asyncio.sleep(_backoff_interval)


async def _background_refresh_adsb():
    logger.info("ADS-B background refresh started")
    loop = asyncio.get_event_loop()

    while True:
        for icao in AIRPORTS:
            try:
                data = await loop.run_in_executor(_executor, _fetch_adsb_for_airport, icao)
                if data:
                    _adsb_cache[icao] = {**data, "fetched_at": time.time()}
                    logger.info("ADS-B refreshed %s: %d aircraft in range",
                                AIRPORTS[icao]["iata"], data["total"])
            except Exception as e:
                logger.warning("ADS-B refresh error for %s: %s", icao, e)

            # Respect rate limit for Airplanes.live
            await asyncio.sleep(2)

        # Cycle through all airports takes ~30 seconds, then wait before next cycle
        await asyncio.sleep(30)


async def _background_refresh_opensky():
    """
    Periodically fetch OpenSky flight track history to resolve unknown/scheduled statuses.
    Staggered to stay well within OpenSky anonymous rate limits (~400 req/day).
    15 airports × 2 directions = 30 requests per cycle; cycle every 25 min = ~86 req/day.
    """
    logger.info("OpenSky background refresh started")
    loop = asyncio.get_event_loop()
    # Initial delay — let the primary data load first
    await asyncio.sleep(120)

    while True:
        for icao in AIRPORTS:
            for direction in ("departures", "arrivals"):
                try:
                    result = await loop.run_in_executor(
                        _executor, _fetch_opensky_tracks, icao, direction
                    )
                    if result is not None:
                        if icao not in _opensky_cache:
                            _opensky_cache[icao] = {}
                        _opensky_cache[icao][direction] = result
                        logger.info("OpenSky %s %s: %d tracks", icao, direction, result["total"])
                except Exception as e:
                    logger.warning("OpenSky refresh error %s/%s: %s", icao, direction, e)

                # Stagger requests — OpenSky asks for polite access
                await asyncio.sleep(30)

        # Wait before next full cycle (~25 min total cycle time already from stagger)
        await asyncio.sleep(300)


async def _background_enrich_past_scheduled():
    """
    Periodically resolves flights still showing 'scheduled' / 'unknown' after their
    departure/arrival time has passed.  Queries ixigo and airline-specific status
    endpoints as a 4th-layer fallback after OpenSky + ADS-B.

    Rate discipline: 2 s between individual flight requests, full cycle every 10 min.
    Only flights >15 min overdue are attempted; results cached for 15 min to avoid
    hammering external services.
    """
    logger.info("Past-scheduled enrichment task started")
    loop = asyncio.get_event_loop()
    # Wait for primary schedule and ADS-B data to load first
    await asyncio.sleep(240)

    while True:
        now = time.time()
        # Collect candidates: past-scheduled flights not yet enriched (or enrichment expired)
        candidates: list[tuple[str, str, str, str]] = []  # (fn, airline_iata, date_str, cache_key)

        for icao in AIRPORTS:
            for direction in ("departures", "arrivals"):
                entry = _safe_get(_cache, icao, direction)
                if not entry:
                    continue
                for f in entry.get("data", []):
                    cat = f["status"]["category"]
                    if cat not in ("scheduled", "unknown"):
                        continue
                    ts = (
                        f["times"].get("scheduled_departure")
                        if direction == "departures"
                        else f["times"].get("scheduled_arrival")
                    )
                    if not ts or ts > now - 900:  # skip if <15 min overdue
                        continue
                    fn = f.get("flight_number", "")
                    if not fn:
                        continue
                    airline_iata = f.get("airline", {}).get("iata", "")
                    date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                    cache_key = f"{fn}:{date_str}"
                    # Skip if we have a fresh enrichment attempt (resolved or not)
                    existing = _enriched_status_cache.get(cache_key)
                    if existing and now - existing.get("fetched_at", 0) < ENRICHED_STATUS_CACHE_TTL:
                        continue
                    candidates.append((fn, airline_iata, date_str, cache_key))

        if candidates:
            logger.info(
                "Past-scheduled enrichment: %d flights to probe (ixigo/airline sources)",
                len(candidates),
            )

        resolved = 0
        for fn, airline_iata, date_str, cache_key in candidates:
            result = await loop.run_in_executor(
                _executor, _try_enrich_flight_status, fn, airline_iata, date_str
            )
            # Always cache the attempt (even None) so we don't retry too soon
            _enriched_status_cache[cache_key] = {
                **(
                    {"category": result[0], "text": result[1], "source": result[2]}
                    if result
                    else {}
                ),
                "fetched_at": time.time(),
                "resolved": result is not None,
            }
            if result:
                resolved += 1
                logger.info(
                    "Enriched past-scheduled %s → %s (via %s)", fn, result[0], result[2]
                )
            # Polite rate-limiting between external requests
            await asyncio.sleep(2)

        if resolved:
            logger.info(
                "Past-scheduled enrichment cycle: resolved %d/%d flights",
                resolved, len(candidates),
            )

        await asyncio.sleep(600)  # full cycle every 10 minutes


# ---------------------------------------------------------------------------
# Live flights across India's airspace
# ---------------------------------------------------------------------------
# Invert AIRLINE_IATA_TO_ICAO so we can map ICAO callsign prefix → IATA
_ICAO_PREFIX_TO_IATA = {v: k for k, v in AIRLINE_IATA_TO_ICAO.items()}


def _fetch_live_flights_india() -> list:
    """Fetch all airborne aircraft in India's airspace (lat 5.5–37.5, lon 66–98.5).

    Tries OpenSky Network states/all (bounding box) first, falls back to
    Airplanes.live with a 1000 nm radius from India's geographic centre.
    Both sources are already acknowledged in the app footer.
    """
    headers = {"User-Agent": "India-Flight-Status-Dashboard/1.0 (non-commercial)"}

    # --- Primary: OpenSky bounding box ---
    try:
        resp = http_requests.get(
            "https://opensky-network.org/api/states/all",
            params={"lamin": 5.5, "lomin": 66.0, "lamax": 37.5, "lomax": 98.5},
            headers=headers,
            timeout=15,
        )
        if resp.status_code == 200:
            states = resp.json().get("states") or []
            result = []
            for s in states:
                if len(s) < 9 or s[8]:  # skip on-ground
                    continue
                lat, lon = s[6], s[5]
                if lat is None or lon is None:
                    continue
                callsign = (s[1] or "").strip().upper()
                if not callsign:
                    continue
                alt_m = s[7]
                result.append({
                    "icao24": s[0] or "",
                    "callsign": callsign,
                    "lat": round(lat, 4),
                    "lon": round(lon, 4),
                    "heading": round(s[10]) if s[10] is not None else 0,
                    "alt_ft": round(alt_m * 3.28084) if alt_m else None,
                    "source": "opensky",
                })
            logger.info("Live flights (OpenSky): %d aircraft", len(result))
            return result
    except Exception as e:
        logger.warning("OpenSky live flights error: %s", e)

    # --- Fallback: Airplanes.live (centre of India, 1000 nm radius) ---
    try:
        resp = http_requests.get(f"{ADSB_API_BASE}/point/22/82/1000", timeout=15)
        if resp.status_code == 200:
            result = []
            for ac in resp.json().get("ac", []):
                if ac.get("alt_baro") == "ground":
                    continue
                lat = ac.get("lat")
                lon = ac.get("lon")
                if lat is None or lon is None:
                    continue
                if not (5.5 <= lat <= 37.5 and 66 <= lon <= 98.5):
                    continue
                callsign = (ac.get("flight") or "").strip().upper()
                if not callsign:
                    continue
                alt_baro = ac.get("alt_baro")
                result.append({
                    "icao24": ac.get("hex", ""),
                    "callsign": callsign,
                    "lat": round(lat, 4),
                    "lon": round(lon, 4),
                    "heading": round(ac.get("track") or 0),
                    "alt_ft": alt_baro if isinstance(alt_baro, int) else None,
                    "source": "airplanes.live",
                })
            logger.info("Live flights (Airplanes.live): %d aircraft", len(result))
            return result
    except Exception as e:
        logger.warning("Airplanes.live live flights error: %s", e)

    return []


def _build_flight_delay_index() -> dict:
    """Build a callsign → delay info index from all currently cached scheduled flights.

    Maps ICAO callsign (e.g. 'IGO123') to delay_minutes and route info so the
    live-flights overlay can colour aircraft by how late they are.
    """
    index: dict = {}
    for icao_ap, directions in _cache.items():
        for _dir, entry in directions.items():
            if not isinstance(entry, dict):
                continue
            for f in (entry.get("data") or []):
                airline_iata = (f.get("airline") or {}).get("iata", "")
                icao_prefix = AIRLINE_IATA_TO_ICAO.get(airline_iata, "")
                if not icao_prefix:
                    continue
                fn = (f.get("flight_number") or "").replace(" ", "").upper()
                num_part = "".join(c for c in fn if c.isdigit())
                if not num_part:
                    continue
                callsign = f"{icao_prefix}{num_part}"
                delay = f.get("delay_minutes") or 0
                if callsign not in index or delay > index[callsign].get("delay_minutes", 0):
                    index[callsign] = {
                        "delay_minutes": delay,
                        "flight_number": f.get("flight_number", ""),
                        "airline": (f.get("airline") or {}).get("name", ""),
                        "origin": (f.get("origin") or {}).get("iata", ""),
                        "destination": (f.get("destination") or {}).get("iata", ""),
                    }
    return index


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    schedule_task  = asyncio.create_task(_background_refresh_schedule())
    adsb_task      = asyncio.create_task(_background_refresh_adsb())
    opensky_task   = asyncio.create_task(_background_refresh_opensky())
    enrichment_task = asyncio.create_task(_background_enrich_past_scheduled())
    yield
    for task in (schedule_task, adsb_task, opensky_task, enrichment_task):
        task.cancel()
    for task in (schedule_task, adsb_task, opensky_task, enrichment_task):
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="Middle East Flight Status", lifespan=lifespan)

app.mount("/static", StaticFiles(directory="static"), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["X-Dashboard-Token"],
)


# Scraper protection middleware
@app.middleware("http")
async def scraper_protection(request: Request, call_next):
    path = request.url.path

    # Always allow dashboard page, static assets, and health endpoint
    if path in ("/", "/health") or path.startswith("/static"):
        response = await call_next(request)
        return response

    # API endpoints require rate limiting
    if path.startswith("/api/"):
        client_ip = request.client.host if request.client else "unknown"

        # Rate limit check
        if not _check_rate_limit(client_ip):
            return JSONResponse(
                {"error": "Rate limit exceeded. Please slow down."},
                status_code=429,
            )

        # Token validation for data endpoints
        if path in ("/api/flights", "/api/overview", "/api/live-flights"):
            token = request.headers.get("X-Dashboard-Token") or request.query_params.get("_token")
            if not _validate_token(token):
                return JSONResponse(
                    {"error": "Invalid or expired session. Please refresh the page."},
                    status_code=403,
                )

    response = await call_next(request)
    return response


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    airports_list = [{"icao": k, **v} for k, v in AIRPORTS.items()]
    airports_list.sort(key=lambda a: a["city"])
    token = _generate_token()
    return templates.TemplateResponse("index.html", {
        "request": request,
        "airports": airports_list,
        "api_token": token,
    })


@app.get("/api/flights")
async def get_flights(
    airport: str = Query(..., description="IATA or ICAO code"),
    direction: str = Query("departures", regex="^(departures|arrivals)$"),
    date: str = Query(None, description="Date in YYYY-MM-DD format"),
):
    icao = airport.upper()
    if icao in IATA_TO_ICAO:
        icao = IATA_TO_ICAO[icao]
    if icao not in AIRPORTS:
        return JSONResponse({"error": f"Unknown airport: {airport}"}, status_code=400)

    # Determine target date using airport local time
    airport_tz_name = AIRPORTS[icao].get("tz", "UTC")
    airport_tz = ZoneInfo(airport_tz_name)
    today = datetime.now(tz=airport_tz).strftime("%Y-%m-%d")
    target_date = date or today

    # Try to get flights for the requested date
    flights = None
    stale = False

    # First try history cache for specific date
    date_flights = _get_flights_for_date(icao, direction, target_date)
    if date_flights:
        flights = date_flights
        stale = False

    # For today, also merge live cache
    if target_date == today:
        cached_data, is_fresh = _get_cached(icao, direction)
        if cached_data is not None and is_fresh:
            if flights is None:
                flights = cached_data
            else:
                existing_keys = {(f["flight_number"], f["times"].get("scheduled_departure") or f["times"].get("scheduled_arrival")) for f in flights}
                for f in cached_data:
                    key = (f["flight_number"], f["times"].get("scheduled_departure") or f["times"].get("scheduled_arrival"))
                    if key not in existing_keys:
                        flights.append(f)
                        existing_keys.add(key)
            stale = False

    if flights is None:
        # No cached data — fetch live schedule data for the requested date
        loop = asyncio.get_event_loop()
        if target_date == today:
            data = await loop.run_in_executor(_executor, _fetch_airport_data, icao)
        else:
            data = await loop.run_in_executor(_executor, _fetch_airport_data_for_date, icao, target_date)
        if data:
            if target_date == today:
                _update_cache(icao, data)
            else:
                _store_history(icao, data)  # Don't overwrite live cache with historical data
            flights = data[direction]
            stale = False
        else:
            # Last resort: stale cache (only for today)
            if target_date == today:
                cached_data, _ = _get_cached(icao, direction)
                if cached_data is not None:
                    flights = cached_data
                    stale = True
            if flights is None:
                return JSONResponse(
                    {"error": "No data available for this date. Data may not be available yet."},
                    status_code=503,
                )

    # Filter by date using airport local time
    if target_date and flights:
        filtered = []
        for f in flights:
            ts = None
            if direction == "departures":
                ts = f["times"].get("scheduled_departure")
            else:
                ts = f["times"].get("scheduled_arrival")
            if ts:
                flight_date = datetime.fromtimestamp(ts, tz=airport_tz).strftime("%Y-%m-%d")
                if flight_date == target_date:
                    filtered.append(f)
        if filtered:
            flights = filtered

    # Cross-verify with ADS-B data
    flights = _verify_flights_with_adsb(flights, icao, direction)

    airport_info = {"icao": icao, **{k: v for k, v in AIRPORTS[icao].items() if k not in ("lat", "lon")}}
    entry = _safe_get(_cache, icao, direction)
    fetched_at = entry["fetched_at"] if entry else time.time()
    # For non-today dates, never mark as stale (it's intentionally historical)
    if target_date != today:
        stale = False
        fetched_at = time.time()

    # ADS-B metadata
    adsb_entry = _adsb_cache.get(icao)
    adsb_age = time.time() - adsb_entry["fetched_at"] if adsb_entry else None

    return {
        "airport": airport_info,
        "direction": direction,
        "date": target_date,
        "flights": flights,
        "statistics": _compute_stats(flights),
        "meta": {
            "fetched_at": fetched_at,
            "stale": stale,
            "cache_ttl_seconds": CACHE_TTL,
            "adsb_age_seconds": round(adsb_age) if adsb_age else None,
            "adsb_aircraft_count": adsb_entry.get("total") if adsb_entry else None,
        },
    }


@app.get("/api/overview")
async def get_overview():
    """Return OTP stats for all airports (today's departures) for the map view."""
    results = []
    for icao, info in AIRPORTS.items():
        tz = ZoneInfo(info.get("tz", "UTC"))
        today_str = datetime.now(tz=tz).strftime("%Y-%m-%d")
        stats = {"total": 0, "on_time": 0, "on_time_percentage": 0, "cancelled": 0, "delayed": 0}

        entry = _safe_get(_cache, icao, "departures")
        if entry and entry.get("data"):
            today_flights = []
            for f in entry["data"]:
                ts = f["times"].get("scheduled_departure")
                if ts:
                    fd = datetime.fromtimestamp(ts, tz=tz).strftime("%Y-%m-%d")
                    if fd == today_str:
                        today_flights.append(f)
            if today_flights:
                stats = _compute_stats(today_flights)

        results.append({
            "icao": icao,
            "iata": info["iata"],
            "name": info["name"],
            "city": info["city"],
            "lat": info["lat"],
            "lon": info["lon"],
            "stats": stats,
        })
    return {"airports": results}


@app.get("/api/airports")
async def get_airports():
    airports_list = [
        {"icao": k, **{kk: vv for kk, vv in v.items() if kk not in ("lat", "lon")}}
        for k, v in AIRPORTS.items()
    ]
    airports_list.sort(key=lambda a: a["city"])
    return {"airports": airports_list}


@app.get("/api/live-flights")
async def get_live_flights():
    """Return all airborne aircraft over India with delay info where available."""
    global _live_flights_cache
    now = time.time()
    if now - _live_flights_cache["fetched_at"] > LIVE_FLIGHTS_CACHE_TTL:
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(_executor, _fetch_live_flights_india)
        _live_flights_cache = {"data": data, "fetched_at": now}

    aircraft_list = _live_flights_cache.get("data") or []
    delay_index = _build_flight_delay_index()

    result = []
    for ac in aircraft_list:
        callsign = ac["callsign"]
        info = delay_index.get(callsign, {})
        result.append({
            "icao24": ac["icao24"],
            "callsign": callsign,
            "lat": ac["lat"],
            "lon": ac["lon"],
            "heading": ac.get("heading", 0),
            "alt_ft": ac.get("alt_ft"),
            "delay_minutes": info.get("delay_minutes"),
            "flight_number": info.get("flight_number") or callsign,
            "airline": info.get("airline", ""),
            "origin": info.get("origin", ""),
            "destination": info.get("destination", ""),
            "matched": bool(info),
        })

    return {
        "aircraft": result,
        "fetched_at": _live_flights_cache["fetched_at"],
        "total": len(result),
    }


@app.get("/health")
async def health():
    cached_airports = {
        AIRPORTS[icao]["iata"]: {
            "schedule_age": round(time.time() - _cache[icao]["departures"]["fetched_at"]) if icao in _cache else None,
            "adsb_age": round(time.time() - _adsb_cache[icao]["fetched_at"]) if icao in _adsb_cache else None,
            "adsb_aircraft": _adsb_cache[icao].get("total") if icao in _adsb_cache else None,
        }
        for icao in AIRPORTS
    }
    return {"status": "ok", "airports": cached_airports}


templates = Jinja2Templates(directory="templates")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
