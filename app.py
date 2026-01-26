# -*- coding: utf-8 -*-
"""
Created on Wed Jan  7 14:55:19 2026

@author: brcum
"""

# -*- coding: utf-8 -*-
"""
Beyond Price and Time
Copyright © 2026 Truth Communications LLC. All Rights Reserved.

Top 1000 stocks with historical backfill for immediate trading signals
True R:R based on price position within stasis bands
Explicit Take Profit and Stop Loss levels

Requirements:
    pip install dash dash-bootstrap-components pandas numpy websocket-client requests

Setup:
    1. Create an 'assets' folder in the same directory as this script
    2. Save your logo as 'logo.png' in the assets folder

Usage:
    python beyond_price_and_time.py
"""

import time
import threading
import numpy as np
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque
import webbrowser
from enum import Enum
import copy
import json

import dash
from dash import dcc, html, Input, Output, State, callback_context, dash_table
import dash_bootstrap_components as dbc
import pandas as pd

import websocket
import ssl
import requests

# ============================================================================
# API KEY
# ============================================================================

POLYGON_API_KEY = "PnzhJOXEJO7tSpHr0ct2zjFKi6XO0yGi"

# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class Config:
    symbols: List[str] = field(default_factory=lambda: [
        # Mega Cap Tech (1-50)
        'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'NVDA', 'META', 'TSLA', 'AVGO', 'ORCL',
        'ADBE', 'CRM', 'AMD', 'INTC', 'CSCO', 'QCOM', 'IBM', 'NOW', 'INTU', 'AMAT',
        'MU', 'LRCX', 'ADI', 'KLAC', 'SNPS', 'CDNS', 'MRVL', 'FTNT', 'PANW', 'CRWD',
        'ZS', 'DDOG', 'SNOW', 'PLTR', 'NET', 'MDB', 'TEAM', 'WDAY', 'OKTA', 'HUBS',
        'ZM', 'DOCU', 'SQ', 'PYPL', 'SHOP', 'MELI', 'SE', 'UBER', 'LYFT', 'DASH',
        # Tech continued (51-100)
        'ABNB', 'COIN', 'HOOD', 'RBLX', 'U', 'TTWO', 'EA', 'ATVI', 'NFLX', 'ROKU',
        'SPOT', 'TTD', 'PINS', 'SNAP', 'TWTR', 'MTCH', 'BMBL', 'ZG', 'RDFN', 'OPEN',
        'CVNA', 'CPNG', 'GRAB', 'BEKE', 'JD', 'BABA', 'PDD', 'BIDU', 'NIO', 'XPEV',
        'LI', 'RIVN', 'LCID', 'FSR', 'GOEV', 'ARVL', 'REE', 'HYLN', 'NKLA', 'RIDE',
        'WKHS', 'SOLO', 'KNDI', 'BLNK', 'CHPT', 'EVGO', 'DCFC', 'VLTA', 'PTRA', 'LEV',
        # Finance (101-200)
        'BRK.B', 'JPM', 'V', 'MA', 'BAC', 'WFC', 'GS', 'MS', 'C', 'AXP',
        'BLK', 'SCHW', 'SPGI', 'CME', 'ICE', 'CB', 'PGR', 'MMC', 'AON', 'TRV',
        'MET', 'PRU', 'AIG', 'ALL', 'AFL', 'HIG', 'MTB', 'FITB', 'RF', 'CFG',
        'KEY', 'HBAN', 'ZION', 'CMA', 'USB', 'PNC', 'TFC', 'ALLY', 'COF', 'DFS',
        'SYF', 'NTRS', 'STT', 'BK', 'TROW', 'IVZ', 'BEN', 'AMG', 'SEIC', 'MKTX',
        'CBOE', 'NDAQ', 'MSCI', 'FDS', 'MCO', 'INFO', 'BR', 'WEX', 'GPN', 'FIS',
        'FISV', 'ADP', 'PAYX', 'PAYC', 'PCTY', 'HQY', 'WU', 'MGI', 'EEFT', 'IMXI',
        'PAYO', 'RPAY', 'FOUR', 'TOST', 'BILL', 'AFRM', 'UPST', 'LC', 'SOFI', 'NU',
        'PSFE', 'PAGS', 'STNE', 'XP', 'VTEX', 'DOCN', 'TWLO', 'BAND', 'LMND', 'ROOT',
        'OSCR', 'CLOV', 'HIMS', 'TDOC', 'AMWL', 'TALK', 'CERT', 'GDRX', 'NTRA', 'GH',
        # Healthcare (201-350)
        'UNH', 'JNJ', 'LLY', 'PFE', 'ABBV', 'MRK', 'TMO', 'ABT', 'DHR', 'BMY',
        'AMGN', 'GILD', 'VRTX', 'REGN', 'ISRG', 'MDT', 'SYK', 'BSX', 'ELV', 'CI',
        'HUM', 'CNC', 'MCK', 'CAH', 'CVS', 'WBA', 'ZTS', 'IDXX', 'DXCM', 'IQV',
        'MTD', 'A', 'WAT', 'BIO', 'HOLX', 'ALGN', 'PODD', 'MRNA', 'BIIB', 'ILMN',
        'ALNY', 'BMRN', 'INCY', 'MEDP', 'CRL', 'PKI', 'DGX', 'LH', 'HCA', 'THC',
        'UHS', 'CYH', 'SEM', 'ACHC', 'ADUS', 'AMED', 'ENSG', 'NHC', 'PNTG', 'CCRN',
        'AMN', 'HCSG', 'EHC', 'SGRY', 'USPH', 'OPCH', 'ALHC', 'BKD', 'NHI', 'OHI',
        'SBRA', 'HR', 'DHC', 'PEAK', 'VTR', 'WELL', 'LTC', 'CTRE', 'GMRE', 'MPW',
        'AGNC', 'NLY', 'STWD', 'BXMT', 'LADR', 'KREF', 'TRTX', 'GPMT', 'RC', 'TWO',
        'ARR', 'ORC', 'IVR', 'NYMT', 'MFA', 'PMT', 'MITT', 'WMC', 'ANH', 'CMO',
        'CHMI', 'EARN', 'DX', 'NRZ', 'RITM', 'ACRE', 'ARI', 'CIGI', 'JLL', 'CBRE',
        'CWK', 'NMRK', 'MMI', 'DOUG', 'RMAX', 'EXPI', 'COMP', 'RDFN', 'OPEN', 'OPAD',
        'FTHM', 'REAL', 'HOUS', 'TRUP', 'PETS', 'WOOF', 'CHWY', 'BARK', 'FRPT', 'PET',
        'ELAN', 'ZTS', 'IDXX', 'NVST', 'PDCO', 'HSIC', 'XRAY', 'ALGN', 'NVST', 'APEN',
        'SDC', 'ALGM', 'GMED', 'NUVA', 'OFIX', 'KIDS', 'OMI', 'LMAT', 'ATRC', 'IRTC',
        # Consumer Discretionary (351-500)
        'HD', 'LOW', 'TJX', 'NKE', 'SBUX', 'MCD', 'CMG', 'YUM', 'DPZ', 'QSR',
        'BKNG', 'MAR', 'HLT', 'H', 'WH', 'RCL', 'CCL', 'NCLH', 'LVS', 'WYNN',
        'MGM', 'CZR', 'PENN', 'DKNG', 'RSI', 'GENI', 'SKLZ', 'DMYT', 'SGHC', 'BALY',
        'RRR', 'MCRI', 'PLYA', 'TNL', 'VAC', 'IHG', 'CHH', 'STAY', 'APTS', 'SVC',
        'PK', 'RLJ', 'SHO', 'DRH', 'XHR', 'INN', 'APLE', 'CLDT', 'HT', 'AHT',
        'DRI', 'EAT', 'WING', 'TXRH', 'BLMN', 'CAKE', 'CHUY', 'TACO', 'JACK', 'DENN',
        'ARCO', 'LOCO', 'BROS', 'SBUX', 'DNKN', 'JAB', 'QSR', 'WEN', 'ARMK', 'CMPGY',
        'TGT', 'COST', 'WMT', 'DG', 'DLTR', 'BIG', 'OLLI', 'FIVE', 'PSMT', 'IMKTA',
        'ROST', 'BURL', 'GPS', 'ANF', 'AEO', 'URBN', 'EXPR', 'TLYS', 'ZUMZ', 'BOOT',
        'GES', 'GIII', 'PVH', 'RL', 'TPR', 'VFC', 'LEVI', 'HBI', 'HAFC', 'GIII',
        'LULU', 'NKE', 'UAA', 'UA', 'SKX', 'SHOO', 'CAL', 'SCVL', 'HIBB', 'BGFV',
        'DKS', 'ASO', 'SPWH', 'VSTO', 'SWBI', 'RGR', 'AMMO', 'POWW', 'CLSK', 'AOUT',
        'GRMN', 'GPRO', 'SONO', 'KOSS', 'VOXX', 'HEAR', 'LOGI', 'CRSR', 'HEAR', 'JBL',
        'POOL', 'TSCO', 'ORLY', 'AZO', 'AAP', 'GPC', 'BBY', 'GME', 'BBBY', 'W',
        'ETSY', 'EBAY', 'CPRT', 'KMX', 'CVNA', 'VRM', 'LOTZ', 'SFT', 'CARG', 'CARS',
        # Consumer Staples (501-600)
        'PG', 'KO', 'PEP', 'PM', 'MO', 'STZ', 'TAP', 'BUD', 'DEO', 'BF.B',
        'MNST', 'KDP', 'CELH', 'FIZZ', 'COKE', 'NBEV', 'REED', 'PRMW', 'WTER', 'TBEV',
        'KHC', 'GIS', 'K', 'CAG', 'CPB', 'SJM', 'HRL', 'TSN', 'HSY', 'MDLZ',
        'CL', 'EL', 'CHD', 'CLX', 'KMB', 'SPB', 'HNST', 'BRBR', 'PRPL', 'CSPR',
        'KR', 'SYY', 'USFD', 'PFGC', 'CHEF', 'UNFI', 'SPTN', 'ANDE', 'BGS', 'SMPL',
        'HAIN', 'BYND', 'TTCF', 'APPH', 'VITL', 'FRPT', 'NOMD', 'STKL', 'OTLY', 'COCO',
        'LANC', 'JJSF', 'SENEA', 'SENEB', 'LNDC', 'FARM', 'CALM', 'JBSS', 'BRFS', 'PPC',
        'SAFM', 'INGR', 'DAR', 'THS', 'FLO', 'IPAR', 'CENT', 'CENTA', 'ACCO', 'SWM',
        'ATR', 'SON', 'SEE', 'PKG', 'IP', 'WRK', 'GPK', 'GEF', 'BERY', 'AMCR',
        'AVY', 'SLGN', 'BLL', 'CCK', 'BALL', 'OI', 'ARD', 'TROX', 'CC', 'HUN',
        # Energy (601-700)
        'XOM', 'CVX', 'COP', 'SLB', 'EOG', 'PSX', 'VLO', 'MPC', 'OXY', 'HAL',
        'PXD', 'DVN', 'FANG', 'HES', 'APA', 'MRO', 'CTRA', 'EQT', 'AR', 'SWN',
        'RRC', 'CNX', 'COG', 'MTDR', 'PDCE', 'SM', 'WLL', 'OAS', 'CPE', 'CRK',
        'TELL', 'RIG', 'VAL', 'NE', 'DO', 'HP', 'PTEN', 'NBR', 'DRQ', 'LBRT',
        'PUMP', 'OIS', 'WHD', 'AROC', 'USAC', 'TRGP', 'WES', 'AM', 'HESM', 'SMLP',
        'CEQP', 'GLP', 'HEP', 'CAPL', 'DKL', 'MPLX', 'PAA', 'PAGP', 'NS', 'NGL',
        'GEL', 'SUN', 'USDP', 'SPH', 'SHLX', 'BPMP', 'ETRN', 'DCP', 'ENLC', 'NBLX',
        'KMI', 'WMB', 'OKE', 'ET', 'EPD', 'ENB', 'TRP', 'BKR', 'FTI', 'NOV',
        'CHX', 'HLX', 'OII', 'SDRL', 'TDW', 'GEOS', 'CLB', 'LIQT', 'NR', 'EFXT',
        'HCC', 'ARCH', 'AMR', 'ARLP', 'CEIX', 'BTU', 'METC', 'NC', 'SXC', 'HCC',
        # Industrials (701-850)
        'CAT', 'DE', 'UNP', 'HON', 'GE', 'RTX', 'BA', 'LMT', 'NOC', 'GD',
        'TDG', 'HWM', 'TXT', 'LHX', 'LDOS', 'SAIC', 'BAH', 'CACI', 'KTOS', 'MRCY',
        'MMM', 'ITW', 'EMR', 'ROK', 'ETN', 'PH', 'DOV', 'AME', 'NDSN', 'KEYS',
        'ZBRA', 'TER', 'TRMB', 'JKHY', 'ANSS', 'PTC', 'MANH', 'GWRE', 'SSNC', 'VRSK',
        'FDX', 'UPS', 'CHRW', 'EXPD', 'XPO', 'JBHT', 'ODFL', 'SAIA', 'WERN', 'KNX',
        'SNDR', 'HTLD', 'ARCB', 'MRTN', 'YELL', 'HUBG', 'ECHO', 'RLGT', 'GXO', 'FWRD',
        'DAL', 'UAL', 'AAL', 'LUV', 'ALK', 'JBLU', 'SAVE', 'MESA', 'SKYW', 'ALGT',
        'HA', 'RYAAY', 'CPA', 'VLRS', 'GOL', 'AZUL', 'ERJ', 'TGI', 'SNCY', 'ATSG',
        'NSC', 'CSX', 'UNP', 'CP', 'CNI', 'WAB', 'GWW', 'FAST', 'WSO', 'AIT',
        'MSM', 'SITE', 'POOL', 'WCC', 'CNM', 'FERG', 'WSO', 'GMS', 'BLDR', 'BLD',
        'IBP', 'MHK', 'TREX', 'DOOR', 'TILE', 'SSD', 'AWI', 'FRTA', 'APOG', 'NX',
        'VMC', 'MLM', 'SUM', 'EXP', 'ITE', 'USLM', 'IIIN', 'ROCK', 'CRON', 'SMID',
        'TEX', 'AGCO', 'CNHI', 'ALG', 'PCAR', 'CMI', 'OSK', 'NAV', 'SHYF', 'WNC',
        'ALSN', 'BC', 'MOD', 'HAYW', 'REVG', 'FSS', 'TTC', 'HY', 'ASTE', 'GTX',
        'DY', 'MTRN', 'ATKR', 'NVT', 'POWL', 'ENS', 'AEIS', 'VICR', 'BELFB', 'BELFA',
        # Materials (851-900)
        'LIN', 'APD', 'ECL', 'SHW', 'PPG', 'DD', 'DOW', 'LYB', 'EMN', 'CE',
        'ALB', 'FMC', 'CF', 'MOS', 'NTR', 'IFF', 'CTVA', 'RPM', 'AXTA', 'AZEK',
        'NUE', 'STLD', 'CLF', 'X', 'RS', 'CMC', 'ZEUS', 'SCHN', 'CRS', 'HAYN',
        'ATI', 'KALU', 'CENX', 'AA', 'ARNC', 'CSTM', 'HXL', 'KRA', 'CMP', 'TKR',
        'FCX', 'SCCO', 'TECK', 'VALE', 'RIO', 'BHP', 'NEM', 'GOLD', 'AEM', 'FNV',
        # Real Estate (901-950)
        'AMT', 'PLD', 'CCI', 'EQIX', 'PSA', 'SPG', 'O', 'WELL', 'DLR', 'AVB',
        'EQR', 'VTR', 'ARE', 'MAA', 'UDR', 'ESS', 'INVH', 'SUI', 'ELS', 'HST',
        'PK', 'RLJ', 'SHO', 'DRH', 'XHR', 'INN', 'APLE', 'CLDT', 'HT', 'AHT',
        'IRM', 'CUBE', 'EXR', 'REXR', 'STAG', 'COLD', 'IIPR', 'VICI', 'GLPI', 'STOR',
        'NNN', 'EPRT', 'ADC', 'FCPT', 'GTY', 'PINE', 'SAFE', 'LAND', 'ILPT', 'OPI',
        # Utilities (951-1000)
        'NEE', 'DUK', 'SO', 'D', 'AEP', 'EXC', 'SRE', 'XEL', 'ED', 'PEG',
        'WEC', 'ES', 'AWK', 'DTE', 'ETR', 'FE', 'PPL', 'CMS', 'AES', 'AEE',
        'CNP', 'NI', 'PNW', 'NRG', 'VST', 'PCG', 'EIX', 'OGE', 'ALE', 'POR',
        'IDA', 'BKH', 'NWE', 'AVA', 'SJI', 'NJR', 'OGS', 'SR', 'UTL', 'MGEE',
        'T', 'VZ', 'TMUS', 'CMCSA', 'DIS', 'CHTR', 'PARA', 'WBD', 'FOX', 'FOXA',
    ])
    
    thresholds: List[float] = field(default_factory=lambda: [
        0.005, 0.0075, 0.01, 0.0125, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05
    ])
    
    display_bits: int = 20
    update_interval_ms: int = 1000
    cache_refresh_interval: float = 0.5
    history_days: int = 5
    
    polygon_api_key: str = POLYGON_API_KEY
    polygon_ws_url: str = "wss://delayed.polygon.io/stocks"
    polygon_rest_url: str = "https://api.polygon.io"
    
    volumes: Dict[str, float] = field(default_factory=dict)
    week52_data: Dict[str, Dict] = field(default_factory=dict)
    
    min_tradable_stasis: int = 3

config = Config()
config.symbols = list(dict.fromkeys(config.symbols))  # Remove duplicates

# ============================================================================
# ENUMS
# ============================================================================

class Direction(Enum):
    LONG = "LONG"
    SHORT = "SHORT"

class SignalStrength(Enum):
    WEAK = "WEAK"
    MODERATE = "MODERATE"
    STRONG = "STRONG"
    VERY_STRONG = "VERY_STRONG"

# ============================================================================
# DATA FETCHERS
# ============================================================================

def fetch_52_week_data() -> Dict[str, Dict]:
    print("📊 Fetching 52-week high/low data...")
    week52_data = {}
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    success_count = 0
    fail_count = 0
    
    for i, symbol in enumerate(config.symbols):
        try:
            url = (
                f"{config.polygon_rest_url}/v2/aggs/ticker/{symbol}/range/1/day/"
                f"{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
                f"?adjusted=true&sort=asc&limit=365&apiKey={config.polygon_api_key}"
            )
            
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('results') and len(data['results']) > 0:
                    highs = [bar['h'] for bar in data['results']]
                    lows = [bar['l'] for bar in data['results']]
                    high_val = max(highs)
                    low_val = min(lows)
                    week52_data[symbol] = {
                        'high': high_val,
                        'low': low_val,
                        'range': high_val - low_val,
                    }
                    success_count += 1
                else:
                    week52_data[symbol] = {'high': None, 'low': None, 'range': None}
                    fail_count += 1
            else:
                week52_data[symbol] = {'high': None, 'low': None, 'range': None}
                fail_count += 1
            
            if (i + 1) % 100 == 0:
                print(f"   📈 Processed {i + 1}/{len(config.symbols)} (✓{success_count} ✗{fail_count})...")
            
            time.sleep(0.12)
        except:
            week52_data[symbol] = {'high': None, 'low': None, 'range': None}
            fail_count += 1
    
    print(f"✅ 52-week data: {success_count} success, {fail_count} failed\n")
    return week52_data

def fetch_volume_data() -> Dict[str, float]:
    print("📊 Fetching volume data...")
    volumes = {}
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=45)
    
    for i, symbol in enumerate(config.symbols):
        try:
            url = (
                f"{config.polygon_rest_url}/v2/aggs/ticker/{symbol}/range/1/day/"
                f"{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
                f"?adjusted=true&sort=desc&limit=30&apiKey={config.polygon_api_key}"
            )
            
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('results') and len(data['results']) > 0:
                    total_volume = sum(bar['v'] for bar in data['results'])
                    volumes[symbol] = (total_volume / len(data['results'])) / 1_000_000
                else:
                    volumes[symbol] = 10.0
            else:
                volumes[symbol] = 10.0
            
            if (i + 1) % 100 == 0:
                print(f"   📈 Processed {i + 1}/{len(config.symbols)}...")
            
            time.sleep(0.12)
        except:
            volumes[symbol] = 10.0
    
    print(f"✅ Volume data: {len(volumes)} symbols\n")
    return volumes

def fetch_historical_bars(symbol: str, days: int = 5) -> List[Dict]:
    bars = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    try:
        url = (
            f"{config.polygon_rest_url}/v2/aggs/ticker/{symbol}/range/1/minute/"
            f"{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
            f"?adjusted=true&sort=asc&limit=50000&apiKey={config.polygon_api_key}"
        )
        
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('results'):
                for bar in data['results']:
                    bars.append({
                        'timestamp': datetime.fromtimestamp(bar['t'] / 1000),
                        'close': bar['c'],
                    })
    except:
        pass
    
    return bars

def calculate_52week_percentile(price: float, symbol: str) -> Optional[float]:
    if symbol not in config.week52_data:
        return None
    data = config.week52_data[symbol]
    if data is None:
        return None
    high = data.get('high')
    low = data.get('low')
    range_val = data.get('range')
    if high is None or low is None or range_val is None or range_val <= 0:
        return None
    percentile = ((price - low) / range_val) * 100
    return max(0.0, min(100.0, percentile))

# ============================================================================
# STASIS INFO
# ============================================================================

@dataclass
class StasisInfo:
    start_time: datetime
    start_price: float
    peak_stasis: int = 1
    
    def get_duration(self) -> timedelta:
        return datetime.now() - self.start_time
    
    def get_duration_str(self) -> str:
        duration = self.get_duration()
        total_seconds = int(duration.total_seconds())
        if total_seconds < 60:
            return f"{total_seconds}s"
        elif total_seconds < 3600:
            return f"{total_seconds // 60}m {total_seconds % 60}s"
        else:
            return f"{total_seconds // 3600}h {(total_seconds % 3600) // 60}m"
    
    def get_start_date_str(self) -> str:
        return self.start_time.strftime("%m/%d %H:%M")
    
    def get_price_change_pct(self, current_price: float) -> float:
        if self.start_price == 0:
            return 0.0
        return (current_price - self.start_price) / self.start_price * 100

# ============================================================================
# BITSTREAM
# ============================================================================

@dataclass
class BitEntry:
    bit: int
    price: float
    timestamp: datetime

class Bitstream:
    def __init__(self, symbol: str, threshold: float, initial_price: float, volume: float):
        self.symbol = symbol
        self.threshold = threshold
        self.initial_price = initial_price
        self.volume = volume
        
        self.reference_price = initial_price
        self.current_live_price = initial_price
        self.last_price_update = datetime.now()
        
        self._update_bands()
        
        self.bits: deque = deque(maxlen=500)
        
        self.current_stasis = 0
        self.last_bit = None
        self.direction = None
        self.signal_strength = None
        
        self.stasis_info: Optional[StasisInfo] = None
        
        self.total_bits = 0
        self._lock = threading.Lock()
    
    def _update_bands(self):
        self.band_width = self.threshold * self.reference_price
        self.upper_band = self.reference_price + self.band_width
        self.lower_band = self.reference_price - self.band_width
    
    def process_price(self, price: float, timestamp: datetime) -> List[int]:
        with self._lock:
            self.current_live_price = price
            self.last_price_update = timestamp
            
            generated_bits = []
            
            if self.lower_band < price < self.upper_band:
                return generated_bits
            
            if self.band_width <= 0:
                return generated_bits
            
            x = int((price - self.reference_price) / self.band_width)
            
            if x > 0:
                for _ in range(x):
                    self.bits.append(BitEntry(1, price, timestamp))
                    generated_bits.append(1)
                    self.total_bits += 1
                self.reference_price = price
                self._update_bands()
            elif x < 0:
                for _ in range(abs(x)):
                    self.bits.append(BitEntry(0, price, timestamp))
                    generated_bits.append(0)
                    self.total_bits += 1
                self.reference_price = price
                self._update_bands()
            
            if generated_bits:
                self._update_stasis(timestamp)
            
            return generated_bits
    
    def _update_stasis(self, timestamp: datetime):
        if len(self.bits) < 2:
            self.current_stasis = len(self.bits)
            self.last_bit = self.bits[-1].bit if self.bits else None
            self.direction = None
            self.signal_strength = None
            return
        
        bits_list = list(self.bits)
        
        stasis_count = 1
        stasis_start_idx = len(bits_list) - 1
        
        for i in range(len(bits_list) - 1, 0, -1):
            if bits_list[i].bit != bits_list[i-1].bit:
                stasis_count += 1
                stasis_start_idx = i - 1
            else:
                break
        
        prev_stasis = self.current_stasis
        self.current_stasis = stasis_count
        self.last_bit = bits_list[-1].bit
        
        if prev_stasis < 2 and stasis_count >= 2:
            if 0 <= stasis_start_idx < len(bits_list):
                first_bit = bits_list[stasis_start_idx]
                self.stasis_info = StasisInfo(
                    start_time=first_bit.timestamp,
                    start_price=first_bit.price,
                    peak_stasis=stasis_count,
                )
        elif stasis_count >= 2 and self.stasis_info is not None:
            if stasis_count > self.stasis_info.peak_stasis:
                self.stasis_info.peak_stasis = stasis_count
        elif prev_stasis >= 2 and stasis_count < 2:
            self.stasis_info = None
        
        if self.current_stasis >= 2:
            self.direction = Direction.LONG if self.last_bit == 0 else Direction.SHORT
            if self.current_stasis >= 10:
                self.signal_strength = SignalStrength.VERY_STRONG
            elif self.current_stasis >= 7:
                self.signal_strength = SignalStrength.STRONG
            elif self.current_stasis >= 5:
                self.signal_strength = SignalStrength.MODERATE
            elif self.current_stasis >= 3:
                self.signal_strength = SignalStrength.WEAK
            else:
                self.signal_strength = None
        else:
            self.direction = None
            self.signal_strength = None
    
    def is_tradable(self) -> bool:
        with self._lock:
            return (
                self.current_stasis >= config.min_tradable_stasis and
                self.direction is not None and
                self.volume > 1.0
            )
    
    def get_snapshot(self, live_price: Optional[float] = None) -> Dict:
        with self._lock:
            current_price = live_price if live_price is not None else self.current_live_price
            
            anchor_price = None
            stasis_start_str = "—"
            stasis_duration_str = "—"
            duration_seconds = 0
            stasis_price_change_pct = None
            
            if self.stasis_info is not None:
                anchor_price = self.stasis_info.start_price
                stasis_start_str = self.stasis_info.get_start_date_str()
                stasis_duration_str = self.stasis_info.get_duration_str()
                duration_seconds = self.stasis_info.get_duration().total_seconds()
                stasis_price_change_pct = self.stasis_info.get_price_change_pct(current_price)
            
            take_profit = None
            stop_loss = None
            risk_reward = None
            distance_to_tp_pct = None
            distance_to_sl_pct = None
            
            if self.direction is not None and self.current_stasis >= 2:
                if self.direction == Direction.LONG:
                    take_profit = self.upper_band
                    stop_loss = self.lower_band
                    reward = take_profit - current_price
                    risk = current_price - stop_loss
                else:
                    take_profit = self.lower_band
                    stop_loss = self.upper_band
                    reward = current_price - take_profit
                    risk = stop_loss - current_price
                
                if risk > 0 and reward > 0:
                    risk_reward = reward / risk
                elif risk > 0 and reward <= 0:
                    risk_reward = 0.0
                else:
                    risk_reward = None
                
                if current_price > 0:
                    distance_to_tp_pct = (abs(take_profit - current_price) / current_price) * 100
                    distance_to_sl_pct = (abs(stop_loss - current_price) / current_price) * 100
            
            week52_percentile = calculate_52week_percentile(current_price, self.symbol)
            recent_bits = [b.bit for b in list(self.bits)[-15:]]
            
            return {
                'symbol': self.symbol,
                'threshold': self.threshold,
                'threshold_pct': self.threshold * 100,
                'stasis': self.current_stasis,
                'total_bits': self.total_bits,
                'recent_bits': recent_bits,
                'current_price': current_price,
                'anchor_price': anchor_price,
                'direction': self.direction.value if self.direction else None,
                'signal_strength': self.signal_strength.value if self.signal_strength else None,
                'is_tradable': (
                    self.current_stasis >= config.min_tradable_stasis and
                    self.direction is not None and
                    self.volume > 1.0
                ),
                'stasis_start_str': stasis_start_str,
                'stasis_duration_str': stasis_duration_str,
                'duration_seconds': duration_seconds,
                'stasis_price_change_pct': stasis_price_change_pct,
                'take_profit': take_profit,
                'stop_loss': stop_loss,
                'risk_reward': risk_reward,
                'distance_to_tp_pct': distance_to_tp_pct,
                'distance_to_sl_pct': distance_to_sl_pct,
                'week52_percentile': week52_percentile,
                'volume': self.volume,
            }

# ============================================================================
# PRICE FEED
# ============================================================================

class PolygonPriceFeed:
    def __init__(self):
        self.lock = threading.Lock()
        self.current_prices: Dict[str, float] = {}
        self.is_running = False
        self.ws = None
        self.ws_thread = None
        self.message_count = 0
        
        for symbol in config.symbols:
            self.current_prices[symbol] = None
    
    def start(self):
        self.is_running = True
        self.ws_thread = threading.Thread(target=self._ws_loop, daemon=True)
        self.ws_thread.start()
        print("✅ WebSocket starting...")
        return True
    
    def stop(self):
        self.is_running = False
        if self.ws:
            self.ws.close()
    
    def _ws_loop(self):
        while self.is_running:
            try:
                self._connect()
            except Exception as e:
                print(f"❌ WebSocket error: {e}")
                if self.is_running:
                    time.sleep(5)
    
    def _connect(self):
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if isinstance(data, list):
                    for msg in data:
                        self._process(msg)
                else:
                    self._process(data)
            except:
                pass
        
        def on_open(ws):
            print("✅ WebSocket connected!")
            ws.send(json.dumps({"action": "auth", "params": config.polygon_api_key}))
        
        def on_close(ws, code, msg):
            print(f"WebSocket closed: {code}")
        
        def on_error(ws, error):
            print(f"WebSocket error: {error}")
        
        self.ws = websocket.WebSocketApp(
            config.polygon_ws_url,
            on_open=on_open,
            on_message=on_message,
            on_close=on_close,
            on_error=on_error
        )
        self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
    
    def _process(self, msg: Dict):
        if msg.get('ev') == 'status':
            status = msg.get('status')
            print(f"📊 Status: {status} - {msg.get('message', '')}")
            if status == 'auth_success':
                self._subscribe()
        elif msg.get('ev') == 'A':
            symbol = msg.get('sym', '')
            price = msg.get('c') or msg.get('vw')
            if price and symbol in config.symbols:
                with self.lock:
                    self.current_prices[symbol] = float(price)
                    self.message_count += 1
    
    def _subscribe(self):
        if self.ws:
            for i in range(0, len(config.symbols), 50):
                batch = config.symbols[i:i+50]
                self.ws.send(json.dumps({
                    "action": "subscribe",
                    "params": ",".join([f"A.{s}" for s in batch])
                }))
                time.sleep(0.1)
            print(f"📡 Subscribed to {len(config.symbols)} stocks")
    
    def get_all_prices(self) -> Dict[str, float]:
        with self.lock:
            return {k: v for k, v in self.current_prices.items() if v is not None}
    
    def get_status(self) -> Dict:
        with self.lock:
            connected = sum(1 for v in self.current_prices.values() if v is not None)
            return {
                'total': len(config.symbols),
                'connected': connected,
                'message_count': self.message_count
            }

price_feed = PolygonPriceFeed()

# ============================================================================
# BITSTREAM MANAGER
# ============================================================================

class BitstreamManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.streams: Dict[Tuple[str, float], Bitstream] = {}
        self.is_running = False
        
        self.cached_data: List[Dict] = []
        self.cache_lock = threading.Lock()
        
        self.initialized = False
        self.backfill_complete = False
        self.backfill_progress = 0
    
    def backfill(self):
        print("\n" + "=" * 60)
        print("📜 BACKFILLING HISTORICAL DATA")
        print("=" * 60)
        
        historical_data = {}
        
        for i, symbol in enumerate(config.symbols):
            bars = fetch_historical_bars(symbol, config.history_days)
            if bars:
                historical_data[symbol] = bars
            
            self.backfill_progress = int((i + 1) / len(config.symbols) * 100)
            
            if (i + 1) % 50 == 0:
                print(f"   📊 {i + 1}/{len(config.symbols)} ({self.backfill_progress}%)")
            
            time.sleep(0.12)
        
        print(f"\n✅ Historical data: {len(historical_data)} symbols")
        
        with self.lock:
            for symbol, bars in historical_data.items():
                if not bars:
                    continue
                
                initial_price = bars[0]['close']
                volume = config.volumes.get(symbol, 10.0)
                
                for threshold in config.thresholds:
                    key = (symbol, threshold)
                    self.streams[key] = Bitstream(symbol, threshold, initial_price, volume)
                    
                    for bar in bars:
                        self.streams[key].process_price(bar['close'], bar['timestamp'])
        
        self.initialized = True
        self.backfill_complete = True
        
        tradable = sum(1 for s in self.streams.values() if s.is_tradable())
        print(f"✅ Bitstreams: {len(self.streams)} | Tradable: {tradable}")
        print("=" * 60 + "\n")
    
    def start(self):
        self.is_running = True
        threading.Thread(target=self._process_loop, daemon=True).start()
        threading.Thread(target=self._cache_loop, daemon=True).start()
    
    def _process_loop(self):
        while self.is_running:
            time.sleep(0.1)
            if not self.backfill_complete:
                continue
            
            prices = price_feed.get_all_prices()
            timestamp = datetime.now()
            
            with self.lock:
                for symbol, price in prices.items():
                    for threshold in config.thresholds:
                        key = (symbol, threshold)
                        if key in self.streams:
                            self.streams[key].process_price(price, timestamp)
    
    def _cache_loop(self):
        while self.is_running:
            time.sleep(config.cache_refresh_interval)
            if not self.initialized:
                continue
            
            live_prices = price_feed.get_all_prices()
            snapshots = []
            
            with self.lock:
                for stream in self.streams.values():
                    live_price = live_prices.get(stream.symbol)
                    snapshots.append(stream.get_snapshot(live_price))
            
            with self.cache_lock:
                self.cached_data = snapshots
    
    def get_data(self) -> List[Dict]:
        with self.cache_lock:
            return copy.deepcopy(self.cached_data)

manager = BitstreamManager()

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def format_bits(bits: List[int]) -> str:
    return "".join(str(b) for b in bits) if bits else "—"

def format_rr(rr: Optional[float]) -> str:
    if rr is None:
        return "—"
    if rr <= 0:
        return "0:1"
    return f"{rr:.2f}:1" if rr < 10 else f"{rr:.0f}:1"

def get_table_data() -> pd.DataFrame:
    data = manager.get_data()
    if not data:
        return pd.DataFrame()
    
    rows = []
    for d in data:
        chg_str = "—"
        if d['stasis_price_change_pct'] is not None:
            sign = "+" if d['stasis_price_change_pct'] >= 0 else ""
            chg_str = f"{sign}{d['stasis_price_change_pct']:.2f}%"
        
        w52_str = "—"
        if d['week52_percentile'] is not None:
            w52_str = f"{d['week52_percentile']:.0f}%"
        
        rows.append({
            'Symbol': d['symbol'],
            'Band': f"{d['threshold_pct']:.2f}%",
            'Band_Val': d['threshold'],
            'Stasis': d['stasis'],
            'Dir': d['direction'] or '—',
            'Str': d['signal_strength'] or '—',
            'Current': f"${d['current_price']:.2f}" if d['current_price'] else "—",
            'Current_Val': d['current_price'] or 0,
            'Anchor': f"${d['anchor_price']:.2f}" if d['anchor_price'] else "—",
            'Anchor_Val': d['anchor_price'] or 0,
            'TP': f"${d['take_profit']:.2f}" if d['take_profit'] else "—",
            'TP_Val': d['take_profit'] or 0,
            'SL': f"${d['stop_loss']:.2f}" if d['stop_loss'] else "—",
            'SL_Val': d['stop_loss'] or 0,
            'R:R': format_rr(d['risk_reward']),
            'RR_Val': d['risk_reward'] if d['risk_reward'] is not None else -1,
            '→TP': f"{d['distance_to_tp_pct']:.2f}%" if d['distance_to_tp_pct'] else "—",
            '→TP_Val': d['distance_to_tp_pct'] or 0,
            '→SL': f"{d['distance_to_sl_pct']:.2f}%" if d['distance_to_sl_pct'] else "—",
            '→SL_Val': d['distance_to_sl_pct'] or 0,
            'Started': d['stasis_start_str'],
            'Duration': d['stasis_duration_str'],
            'Dur_Val': d['duration_seconds'],
            'Chg': chg_str,
            'Chg_Val': d['stasis_price_change_pct'] if d['stasis_price_change_pct'] else 0,
            '52W': w52_str,
            '52W_Val': d['week52_percentile'] if d['week52_percentile'] is not None else -1,
            'Bits': d['total_bits'],
            'Recent': format_bits(d['recent_bits']),
            'Tradable': '✅' if d['is_tradable'] else '',
            'Is_Tradable': d['is_tradable'],
        })
    
    return pd.DataFrame(rows)

# ============================================================================
# DASH APP
# ============================================================================

CUSTOM_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;500;600;700;800;900&display=swap');
@import url('https://fonts.googleapis.com/css2?family=Roboto+Mono:wght@400;500;600;700&display=swap');

body { 
    background-color: #0a0a0a !important; 
}

.title-font {
    font-family: 'Orbitron', sans-serif !important;
}

.data-font {
    font-family: 'Roboto Mono', monospace !important;
}

/* Headers and labels use Orbitron */
h1, h2, h3, h4, h5, h6, .btn, label, .nav-link, .card-header {
    font-family: 'Orbitron', sans-serif !important;
}

/* Data and numbers use Roboto Mono */
td, input, .form-control, pre, code {
    font-family: 'Roboto Mono', monospace !important;
}

/* Dropdown styling */
.Select-control, .Select-menu-outer, .Select-option, .Select-value-label {
    font-family: 'Roboto Mono', monospace !important;
    font-size: 11px !important;
}
"""

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
app.title = "Beyond Price and Time"

app.index_string = f'''
<!DOCTYPE html>
<html>
    <head>
        {{%metas%}}
        <title>{{%title%}}</title>
        {{%favicon%}}
        {{%css%}}
        <style>{CUSTOM_CSS}</style>
    </head>
    <body>
        {{%app_entry%}}
        <footer>
            {{%config%}}
            {{%scripts%}}
            {{%renderer%}}
        </footer>
    </body>
</html>
'''

app.layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.Div([
                html.Img(src='/assets/logo.png', style={'height': '50px', 'marginRight': '15px'}),
                html.Div([
                    html.H2("BEYOND PRICE AND TIME", className="text-success mb-0 title-font",
                           style={'fontSize': '24px', 'fontWeight': '700', 'letterSpacing': '3px'}),
                    html.P("STASIS DETECTION SYSTEM", className="text-muted title-font",
                          style={'fontSize': '10px', 'fontStyle': 'italic', 'letterSpacing': '2px', 'marginBottom': '0'}),
                ], style={'display': 'inline-block', 'verticalAlign': 'middle'}),
            ], style={'display': 'flex', 'alignItems': 'center'})
        ], width=8),
        dbc.Col([
            html.Div(id='connection-status', className="text-end title-font")
        ], width=4)
    ], className="mb-2 mt-2"),
    
    # Stats
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div(id='stats-display', className="text-center")
                ], style={'padding': '8px'})
            ], style={'backgroundColor': '#1a2a3a', 'border': '1px solid #00ff88'})
        ])
    ], className="mb-2"),
    
    # Filters
    dbc.Row([
        dbc.Col([
            dbc.ButtonGroup([
                dbc.Button("ALL", id="btn-all", color="secondary", outline=True, size="sm", 
                          className="title-font", style={'letterSpacing': '1px'}),
                dbc.Button("TRADABLE", id="btn-tradable", color="success", outline=True, size="sm", 
                          active=True, className="title-font", style={'letterSpacing': '1px'}),
            ])
        ], width=1),
        dbc.Col([
            dcc.Dropdown(id='filter-symbol', 
                        options=[{'label': 'ALL SYMBOLS', 'value': 'ALL'}] + 
                        [{'label': s, 'value': s} for s in config.symbols],
                        value='ALL', clearable=False, style={'fontSize': '10px'})
        ], width=1),
        dbc.Col([
            dcc.Dropdown(id='filter-threshold', 
                        options=[{'label': 'ALL BANDS', 'value': 'ALL'}] + 
                        [{'label': f'{t*100:.2f}%', 'value': t} for t in config.thresholds],
                        value='ALL', clearable=False, style={'fontSize': '10px'})
        ], width=1),
        dbc.Col([
            dcc.Input(id='filter-stasis', type='number', value=3, min=0,
                     placeholder="Min Stasis",
                     style={'width': '100%', 'fontSize': '11px', 'fontFamily': 'Roboto Mono'})
        ], width=1),
        dbc.Col([
            dcc.Dropdown(id='filter-direction',
                        options=[{'label': 'ALL DIRS', 'value': 'ALL'}, 
                                {'label': 'LONG', 'value': 'LONG'},
                                {'label': 'SHORT', 'value': 'SHORT'}],
                        value='ALL', clearable=False, style={'fontSize': '10px'})
        ], width=1),
        dbc.Col([
            dcc.Dropdown(id='filter-rr',
                        options=[{'label': 'ANY R:R', 'value': -1},
                                {'label': '≥0.5', 'value': 0.5},
                                {'label': '≥1', 'value': 1}, 
                                {'label': '≥2', 'value': 2},
                                {'label': '≥3', 'value': 3}],
                        value=-1, clearable=False, style={'fontSize': '10px'})
        ], width=1),
        dbc.Col([
            dcc.Dropdown(id='filter-52w',
                        options=[{'label': 'ANY 52W', 'value': 'ALL'},
                                {'label': '0-20%', 'value': '0-20'}, 
                                {'label': '20-40%', 'value': '20-40'},
                                {'label': '40-60%', 'value': '40-60'}, 
                                {'label': '60-80%', 'value': '60-80'},
                                {'label': '80-100%', 'value': '80-100'}],
                        value='ALL', clearable=False, style={'fontSize': '10px'})
        ], width=1),
        dbc.Col([
            dcc.Dropdown(id='filter-duration',
                        options=[{'label': 'ANY DUR', 'value': 0}, 
                                {'label': '5m+', 'value': 300},
                                {'label': '15m+', 'value': 900}, 
                                {'label': '1h+', 'value': 3600}],
                        value=0, clearable=False, style={'fontSize': '10px'})
        ], width=1),
        dbc.Col([
            dcc.Dropdown(id='filter-rows',
                        options=[{'label': '50', 'value': 50}, 
                                {'label': '100', 'value': 100},
                                {'label': '250', 'value': 250},
                                {'label': '500', 'value': 500},
                                {'label': 'ALL', 'value': 10000}],
                        value=100, clearable=False, style={'fontSize': '10px'})
        ], width=1),
        dbc.Col([
            dcc.Dropdown(id='filter-sort',
                        options=[{'label': 'STASIS ↓', 'value': 'stasis'}, 
                                {'label': 'R:R ↓', 'value': 'rr'},
                                {'label': 'DUR ↓', 'value': 'duration'}, 
                                {'label': '52W ↑', 'value': '52w'}],
                        value='stasis', clearable=False, style={'fontSize': '10px'})
        ], width=1),
    ], className="mb-2 g-1"),
    
    # Table
    dbc.Row([
        dbc.Col([
            dash_table.DataTable(
                id='main-table',
                columns=[
                    {'name': '✓', 'id': 'Tradable', 'sortable': True},
                    {'name': 'SYMBOL', 'id': 'Symbol', 'sortable': True},
                    {'name': 'BAND', 'id': 'Band', 'sortable': True},
                    {'name': 'STASIS', 'id': 'Stasis', 'sortable': True},
                    {'name': 'DIR', 'id': 'Dir', 'sortable': True},
                    {'name': 'STR', 'id': 'Str', 'sortable': True},
                    {'name': 'CURRENT', 'id': 'Current', 'sortable': True},
                    {'name': 'ANCHOR', 'id': 'Anchor', 'sortable': True},
                    {'name': 'TP', 'id': 'TP', 'sortable': True},
                    {'name': 'SL', 'id': 'SL', 'sortable': True},
                    {'name': 'R:R', 'id': 'R:R', 'sortable': True},
                    {'name': '→TP', 'id': '→TP', 'sortable': True},
                    {'name': '→SL', 'id': '→SL', 'sortable': True},
                    {'name': 'STARTED', 'id': 'Started', 'sortable': True},
                    {'name': 'DUR', 'id': 'Duration', 'sortable': True},
                    {'name': 'CHG', 'id': 'Chg', 'sortable': True},
                    {'name': '52W', 'id': '52W', 'sortable': True},
                    {'name': 'BITS', 'id': 'Bits', 'sortable': True},
                    {'name': 'RECENT', 'id': 'Recent', 'sortable': False},
                ],
                sort_action='native',
                sort_mode='multi',
                sort_by=[{'column_id': 'Stasis', 'direction': 'desc'}],
                style_table={'height': '60vh', 'overflowY': 'auto'},
                style_cell={
                    'backgroundColor': '#1a1a2e', 
                    'color': 'white',
                    'padding': '4px 6px', 
                    'fontSize': '11px',
                    'fontFamily': 'Roboto Mono, monospace', 
                    'whiteSpace': 'nowrap',
                    'textAlign': 'right',
                    'minWidth': '50px',
                },
                style_cell_conditional=[
                    {'if': {'column_id': 'Symbol'}, 'textAlign': 'left', 'fontWeight': '600'},
                    {'if': {'column_id': 'Dir'}, 'textAlign': 'center'},
                    {'if': {'column_id': 'Str'}, 'textAlign': 'center'},
                    {'if': {'column_id': 'Tradable'}, 'textAlign': 'center'},
                    {'if': {'column_id': 'Recent'}, 'textAlign': 'left', 'minWidth': '100px'},
                ],
                style_header={
                    'backgroundColor': '#2a2a4e', 
                    'color': '#00ff88',
                    'fontWeight': '700', 
                    'fontSize': '10px',
                    'fontFamily': 'Orbitron, sans-serif',
                    'borderBottom': '2px solid #00ff88',
                    'textAlign': 'center',
                    'letterSpacing': '0.5px',
                },
                style_data_conditional=[
                    {'if': {'filter_query': '{Stasis} >= 10'}, 'backgroundColor': '#2d4a2d'},
                    {'if': {'filter_query': '{Stasis} >= 7 && {Stasis} < 10'}, 'backgroundColor': '#2a3a2a'},
                    {'if': {'filter_query': '{Stasis} >= 5 && {Stasis} < 7'}, 'backgroundColor': '#252a25'},
                    {'if': {'filter_query': '{Dir} = "LONG"', 'column_id': 'Dir'}, 'color': '#00ff00', 'fontWeight': 'bold'},
                    {'if': {'filter_query': '{Dir} = "SHORT"', 'column_id': 'Dir'}, 'color': '#ff4444', 'fontWeight': 'bold'},
                    {'if': {'filter_query': '{Str} = "VERY_STRONG"', 'column_id': 'Str'}, 'color': '#ffff00'},
                    {'if': {'filter_query': '{Str} = "STRONG"', 'column_id': 'Str'}, 'color': '#ffaa00'},
                    {'if': {'column_id': 'Current'}, 'color': '#00ffff', 'fontWeight': '600'},
                    {'if': {'column_id': 'Anchor'}, 'color': '#ffaa00'},
                    {'if': {'column_id': 'TP'}, 'color': '#00ff00'},
                    {'if': {'column_id': 'SL'}, 'color': '#ff4444'},
                    {'if': {'filter_query': '{RR_Val} >= 2', 'column_id': 'R:R'}, 'color': '#00ff00', 'fontWeight': '600'},
                    {'if': {'filter_query': '{RR_Val} >= 1 && {RR_Val} < 2', 'column_id': 'R:R'}, 'color': '#88ff88'},
                    {'if': {'filter_query': '{RR_Val} < 1 && {RR_Val} >= 0', 'column_id': 'R:R'}, 'color': '#ffaa00'},
                    {'if': {'filter_query': '{Chg} contains "+"', 'column_id': 'Chg'}, 'color': '#00ff00'},
                    {'if': {'filter_query': '{Chg} contains "-"', 'column_id': 'Chg'}, 'color': '#ff4444'},
                    {'if': {'filter_query': '{52W_Val} >= 0 && {52W_Val} <= 20', 'column_id': '52W'}, 'color': '#00ff00'},
                    {'if': {'filter_query': '{52W_Val} >= 80', 'column_id': '52W'}, 'color': '#ff4444'},
                    {'if': {'row_index': 'odd'}, 'backgroundColor': '#151520'},
                ]
            )
        ])
    ]),
    
    # Footer
    dbc.Row([
        dbc.Col([
            html.Div([
                html.Span("CURRENT", className="title-font", style={'color': '#00ffff', 'fontSize': '9px', 'fontWeight': 'bold'}),
                html.Span(" = Live Price | ", style={'fontSize': '9px'}),
                html.Span("ANCHOR", className="title-font", style={'color': '#ffaa00', 'fontSize': '9px', 'fontWeight': 'bold'}),
                html.Span(" = Stasis Start Price | ", style={'fontSize': '9px'}),
                html.Span("TP", className="title-font", style={'color': '#00ff00', 'fontSize': '9px', 'fontWeight': 'bold'}),
                html.Span(" = Take Profit | ", style={'fontSize': '9px'}),
                html.Span("SL", className="title-font", style={'color': '#ff4444', 'fontSize': '9px', 'fontWeight': 'bold'}),
                html.Span(" = Stop Loss | ", style={'fontSize': '9px'}),
                html.Span("STARTED", className="title-font", style={'color': '#aaaaaa', 'fontSize': '9px', 'fontWeight': 'bold'}),
                html.Span(" = Stasis Start Date/Time", style={'fontSize': '9px'}),
            ], className="text-center text-muted mt-1"),
            html.Hr(style={'borderColor': '#333', 'margin': '8px 0'}),
            html.P("© 2026 TRUTH COMMUNICATIONS LLC. ALL RIGHTS RESERVED.",
                  className="text-muted text-center mb-0 title-font",
                  style={'fontSize': '9px', 'letterSpacing': '2px'}),
        ])
    ]),
    
    dcc.Store(id='view-mode', data='tradable'),
    dcc.Interval(id='refresh-interval', interval=config.update_interval_ms, n_intervals=0)
    
], fluid=True, className="p-2", style={'backgroundColor': '#0a0a0a'})

# ============================================================================
# CALLBACKS
# ============================================================================

@app.callback(
    [Output('btn-all', 'active'), Output('btn-tradable', 'active'), Output('view-mode', 'data')],
    [Input('btn-all', 'n_clicks'), Input('btn-tradable', 'n_clicks')],
    [State('view-mode', 'data')]
)
def toggle_view(n1, n2, current):
    ctx = callback_context
    if not ctx.triggered:
        return False, True, 'tradable'
    btn = ctx.triggered[0]['prop_id'].split('.')[0]
    if btn == 'btn-all':
        return True, False, 'all'
    return False, True, 'tradable'

@app.callback(
    Output('connection-status', 'children'),
    Input('refresh-interval', 'n_intervals')
)
def update_status(n):
    if not manager.backfill_complete:
        return html.Span(f"⏳ LOADING... {manager.backfill_progress}%", className="text-warning")
    
    status = price_feed.get_status()
    if status['connected'] == 0:
        return html.Span(f"🔴 CONNECTING...", className="text-warning")
    elif status['connected'] < status['total']:
        return html.Span(f"🟡 {status['connected']}/{status['total']}", className="text-info data-font")
    return html.Span(f"🟢 LIVE {status['connected']}/{status['total']} | {status['message_count']:,}", 
                    className="text-success data-font")

@app.callback(
    Output('stats-display', 'children'),
    Input('refresh-interval', 'n_intervals')
)
def update_stats(n):
    if not manager.backfill_complete:
        return html.Span(f"⏳ LOADING {len(config.symbols)} STOCKS... {manager.backfill_progress}%", 
                        className="text-warning title-font")
    
    data = manager.get_data()
    if not data:
        return html.Span("LOADING...", className="text-muted title-font")
    
    tradable = [d for d in data if d['is_tradable']]
    with_rr = [d for d in tradable if d['risk_reward'] is not None and d['risk_reward'] > 0]
    avg_rr = np.mean([d['risk_reward'] for d in with_rr]) if with_rr else 0
    long_count = sum(1 for d in tradable if d['direction'] == 'LONG')
    short_count = sum(1 for d in tradable if d['direction'] == 'SHORT')
    max_stasis = max([d['stasis'] for d in data]) if data else 0
    with_52w = sum(1 for d in data if d['week52_percentile'] is not None)
    
    return html.Div([
        html.Span("🎯 TRADABLE: ", className="title-font", style={'fontSize': '11px'}),
        html.Span(f"{len(tradable)}", className="data-font text-success", style={'fontSize': '12px', 'fontWeight': '600'}),
        html.Span("  📈 LONG: ", className="title-font ms-3", style={'fontSize': '11px'}),
        html.Span(f"{long_count}", className="data-font text-success", style={'fontSize': '12px'}),
        html.Span("  📉 SHORT: ", className="title-font ms-3", style={'fontSize': '11px'}),
        html.Span(f"{short_count}", className="data-font text-danger", style={'fontSize': '12px'}),
        html.Span("  ⚡ MAX: ", className="title-font ms-3", style={'fontSize': '11px'}),
        html.Span(f"{max_stasis}", className="data-font text-warning", style={'fontSize': '12px'}),
        html.Span("  📊 AVG R:R: ", className="title-font ms-3", style={'fontSize': '11px'}),
        html.Span(f"{avg_rr:.2f}:1", className="data-font text-info", style={'fontSize': '12px'}),
        html.Span("  📅 52W: ", className="title-font ms-3", style={'fontSize': '11px'}),
        html.Span(f"{with_52w}", className="data-font text-muted", style={'fontSize': '12px'}),
    ])

@app.callback(
    Output('main-table', 'data'),
    [Input('refresh-interval', 'n_intervals'),
     Input('view-mode', 'data'),
     Input('filter-symbol', 'value'),
     Input('filter-threshold', 'value'),
     Input('filter-stasis', 'value'),
     Input('filter-direction', 'value'),
     Input('filter-rr', 'value'),
     Input('filter-52w', 'value'),
     Input('filter-duration', 'value'),
     Input('filter-rows', 'value'),
     Input('filter-sort', 'value')]
)
def update_table(n, view_mode, sym, thresh, stasis, direction, rr, w52, duration, rows, sort):
    df = get_table_data()
    if df.empty:
        return []
    
    if view_mode == 'tradable':
        df = df[df['Is_Tradable'] == True]
    
    if sym != 'ALL':
        df = df[df['Symbol'] == sym]
    if thresh != 'ALL':
        df = df[df['Band_Val'] == thresh]
    if stasis and stasis > 0:
        df = df[df['Stasis'] >= stasis]
    if direction != 'ALL':
        df = df[df['Dir'] == direction]
    if rr is not None and rr >= 0:
        df = df[(df['RR_Val'].notna()) & (df['RR_Val'] >= rr)]
    if w52 != 'ALL':
        ranges = {'0-20': (0, 20), '20-40': (20, 40), '40-60': (40, 60), '60-80': (60, 80), '80-100': (80, 100)}
        if w52 in ranges:
            lo, hi = ranges[w52]
            df = df[(df['52W_Val'] >= lo) & (df['52W_Val'] <= hi)]
    if duration and duration > 0:
        df = df[df['Dur_Val'] >= duration]
    
    if sort == 'stasis':
        df = df.sort_values(['Stasis', 'RR_Val'], ascending=[False, False])
    elif sort == 'rr':
        df = df.sort_values(['RR_Val', 'Stasis'], ascending=[False, False])
    elif sort == 'duration':
        df = df.sort_values(['Dur_Val', 'Stasis'], ascending=[False, False])
    elif sort == '52w':
        df = df.sort_values(['52W_Val', 'Stasis'], ascending=[True, False], na_position='last')
    
    df = df.head(rows)
    
    drop_cols = ['Band_Val', 'Current_Val', 'Anchor_Val', 'TP_Val', 'SL_Val', 
                 'RR_Val', '→TP_Val', '→SL_Val', 'Dur_Val', 'Chg_Val', '52W_Val', 'Is_Tradable']
    df = df.drop(columns=drop_cols, errors='ignore')
    
    return df.to_dict('records')

# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    print("=" * 70)
    print("  BEYOND PRICE AND TIME")
    print("  © 2026 Truth Communications LLC. All Rights Reserved.")
    print("=" * 70)
    
    print(f"\n🎯 Stocks: {len(config.symbols)}")
    print(f"📏 Thresholds: {len(config.thresholds)}")
    print(f"🔢 Bitstreams: {len(config.symbols) * len(config.thresholds)}")
    
    print("\n" + "=" * 70)
    print("📅 FETCHING 52-WEEK DATA")
    print("=" * 70)
    config.week52_data = fetch_52_week_data()
    
    valid_52w = sum(1 for v in config.week52_data.values() if v.get('high') is not None)
    print(f"📊 52-week data loaded for {valid_52w} symbols")
    
    print("\n" + "=" * 70)
    print("📊 FETCHING VOLUME DATA")
    print("=" * 70)
    config.volumes = fetch_volume_data()
    
    manager.backfill()
    
    price_feed.start()
    manager.start()
    
    print("\n✅ Server: http://127.0.0.1:8050")
    print("\n📋 COLUMNS (all sortable by clicking headers):")
    print("   STARTED = When stasis began (date/time)")
    print("   DUR = Duration of stasis")
    print("   CURRENT = Live price | ANCHOR = Stasis start price")
    print("   TP/SL = Take Profit/Stop Loss based on current bands")
    print("   R:R = Risk:Reward ratio")
    print("=" * 70 + "\n")
    
    threading.Thread(target=lambda: (time.sleep(2), webbrowser.open('http://127.0.0.1:8050')), daemon=True).start()
    
    app.run(debug=False, host='127.0.0.1', port=8050)