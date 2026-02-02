# -*- coding: utf-8 -*-
"""
Beyond Price and Time
Copyright © 2026 Truth Communications LLC. All Rights Reserved.

Top 1000 stocks with historical backfill for immediate trading signals
True R:R based on price position within stasis bands
TWO MERIT SCORES: 
  - Stasis Merit Score (SMS): Based on stasis patterns
  - Fundamental Merit Score (FMS): Based on fundamental growth SLOPES

Requirements:
    pip install dash dash-bootstrap-components pandas numpy websocket-client requests scipy

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
        # Finance
        'JPM', 'V', 'MA', 'BAC', 'WFC', 'GS', 'MS', 'C', 'AXP', 'BLK',
        'SCHW', 'SPGI', 'CME', 'ICE', 'CB', 'PGR', 'MMC', 'AON', 'TRV', 'MET',
        'COF', 'DFS', 'SYF', 'ALLY', 'SOFI', 'AFRM', 'UPST', 'HOOD', 'COIN',
        # Healthcare
        'UNH', 'JNJ', 'LLY', 'PFE', 'ABBV', 'MRK', 'TMO', 'ABT', 'DHR', 'BMY',
        'AMGN', 'GILD', 'VRTX', 'REGN', 'ISRG', 'MDT', 'SYK', 'BSX', 'MRNA', 'BIIB',
        # Consumer
        'HD', 'LOW', 'TJX', 'NKE', 'SBUX', 'MCD', 'CMG', 'YUM', 'DPZ', 'COST',
        'WMT', 'TGT', 'AMZN', 'BKNG', 'MAR', 'HLT', 'ABNB', 'DIS', 'NFLX',
        # Industrial
        'CAT', 'DE', 'UNP', 'HON', 'GE', 'RTX', 'BA', 'LMT', 'NOC', 'GD',
        'MMM', 'ITW', 'EMR', 'FDX', 'UPS',
        # Energy
        'XOM', 'CVX', 'COP', 'SLB', 'EOG', 'PSX', 'VLO', 'MPC', 'OXY', 'HAL',
        # Materials
        'LIN', 'APD', 'ECL', 'SHW', 'NUE', 'FCX', 'NEM',
        # Utilities/REITs
        'NEE', 'DUK', 'SO', 'AMT', 'PLD', 'CCI', 'EQIX', 'PSA', 'SPG', 'O',
        # Communication
        'T', 'VZ', 'TMUS', 'CMCSA', 'CHTR',
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
    
    # Fundamental data storage
    fundamental_data: Dict[str, Dict] = field(default_factory=dict)
    fundamental_slopes: Dict[str, Dict] = field(default_factory=dict)
    
    min_tradable_stasis: int = 3

config = Config()
config.symbols = list(dict.fromkeys(config.symbols))

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
# FUNDAMENTAL DATA FETCHER (POLYGON FINANCIALS API)
# ============================================================================

def fetch_fundamental_data_polygon(symbol: str) -> Optional[Dict]:
    """
    Fetch quarterly financial data from Polygon's Financials API.
    Returns dict with lists of quarterly values for each metric.
    """
    try:
        url = (
            f"{config.polygon_rest_url}/vX/reference/financials"
            f"?ticker={symbol}&timeframe=quarterly&limit=24&sort=filing_date"
            f"&order=desc&apiKey={config.polygon_api_key}"
        )
        
        response = requests.get(url, timeout=30)
        
        if response.status_code != 200:
            return None
        
        data = response.json()
        results = data.get('results', [])
        
        if not results:
            return None
        
        # Initialize lists for each metric
        fundamentals = {
            'dates': [],
            'revenue': [],
            'net_income': [],
            'operating_cash_flow': [],
            'capex': [],
            'fcf': [],
            'total_assets': [],
            'total_liabilities': [],
            'shareholders_equity': [],
            'current_assets': [],
            'current_liabilities': [],
            'total_debt': [],
            'eps': [],
        }
        
        for result in results:
            try:
                financials = result.get('financials', {})
                
                # Income Statement
                income = financials.get('income_statement', {})
                revenue = income.get('revenues', {}).get('value', 0) or 0
                net_income = income.get('net_income_loss', {}).get('value', 0) or 0
                eps = income.get('basic_earnings_per_share', {}).get('value', 0) or 0
                
                # Cash Flow Statement
                cash_flow = financials.get('cash_flow_statement', {})
                operating_cf = cash_flow.get('net_cash_flow_from_operating_activities', {}).get('value', 0) or 0
                # CapEx is usually negative, so we take absolute value
                capex_raw = cash_flow.get('net_cash_flow_from_investing_activities', {}).get('value', 0) or 0
                
                # Balance Sheet
                balance = financials.get('balance_sheet', {})
                total_assets = balance.get('assets', {}).get('value', 0) or 0
                total_liabilities = balance.get('liabilities', {}).get('value', 0) or 0
                equity = balance.get('equity', {}).get('value', 0) or 0
                current_assets = balance.get('current_assets', {}).get('value', 0) or 0
                current_liabilities = balance.get('current_liabilities', {}).get('value', 0) or 0
                
                # Calculate FCF (Operating Cash Flow - CapEx)
                # Note: capex from investing activities is usually negative
                fcf = operating_cf + capex_raw  # Adding because capex is negative
                
                # Get debt
                total_debt = balance.get('long_term_debt', {}).get('value', 0) or 0
                short_term_debt = balance.get('short_term_debt', {}).get('value', 0) or 0
                total_debt += short_term_debt
                
                # Append to lists
                fundamentals['dates'].append(result.get('filing_date', ''))
                fundamentals['revenue'].append(revenue)
                fundamentals['net_income'].append(net_income)
                fundamentals['operating_cash_flow'].append(operating_cf)
                fundamentals['capex'].append(abs(capex_raw))
                fundamentals['fcf'].append(fcf)
                fundamentals['total_assets'].append(total_assets)
                fundamentals['total_liabilities'].append(total_liabilities)
                fundamentals['shareholders_equity'].append(equity)
                fundamentals['current_assets'].append(current_assets)
                fundamentals['current_liabilities'].append(current_liabilities)
                fundamentals['total_debt'].append(total_debt)
                fundamentals['eps'].append(eps)
                
            except Exception as e:
                continue
        
        # Reverse to chronological order (oldest first)
        for key in fundamentals:
            fundamentals[key] = fundamentals[key][::-1]
        
        return fundamentals
        
    except Exception as e:
        print(f"Error fetching fundamentals for {symbol}: {e}")
        return None


def calculate_financial_ratios(fundamentals: Dict, current_price: float, market_cap: float) -> Dict:
    """
    Calculate financial ratios from raw fundamental data.
    """
    ratios = {
        'pe_ratio': [],
        'current_ratio': [],
        'roe': [],
        'roa': [],
        'net_profit_margin': [],
        'debt_to_equity': [],
        'price_to_book': [],
        'price_to_sales': [],
        'asset_turnover': [],
        'fcfy': [],
    }
    
    n = len(fundamentals.get('revenue', []))
    
    for i in range(n):
        try:
            revenue = fundamentals['revenue'][i]
            net_income = fundamentals['net_income'][i]
            total_assets = fundamentals['total_assets'][i]
            equity = fundamentals['shareholders_equity'][i]
            current_assets = fundamentals['current_assets'][i]
            current_liabilities = fundamentals['current_liabilities'][i]
            total_debt = fundamentals['total_debt'][i]
            eps = fundamentals['eps'][i]
            fcf = fundamentals['fcf'][i]
            
            # P/E Ratio
            pe = current_price / eps if eps and eps > 0 else None
            ratios['pe_ratio'].append(pe)
            
            # Current Ratio
            cr = current_assets / current_liabilities if current_liabilities else None
            ratios['current_ratio'].append(cr)
            
            # ROE
            roe = net_income / equity if equity and equity > 0 else None
            ratios['roe'].append(roe)
            
            # ROA
            roa = net_income / total_assets if total_assets else None
            ratios['roa'].append(roa)
            
            # Net Profit Margin
            npm = net_income / revenue if revenue else None
            ratios['net_profit_margin'].append(npm)
            
            # Debt to Equity
            de = total_debt / equity if equity and equity > 0 else None
            ratios['debt_to_equity'].append(de)
            
            # Price to Book (using most recent price for all)
            book_value_per_share = equity / (market_cap / current_price) if current_price and market_cap else None
            pb = current_price / book_value_per_share if book_value_per_share and book_value_per_share > 0 else None
            ratios['price_to_book'].append(pb)
            
            # Price to Sales (annualized)
            annual_revenue = revenue * 4  # Quarterly to annual
            ps = market_cap / annual_revenue if annual_revenue else None
            ratios['price_to_sales'].append(ps)
            
            # Asset Turnover
            at = revenue / total_assets if total_assets else None
            ratios['asset_turnover'].append(at)
            
            # FCFY (using trailing 4 quarters)
            if i >= 3:
                annual_fcf = sum(fundamentals['fcf'][max(0, i-3):i+1])
                fcfy = annual_fcf / market_cap if market_cap else None
            else:
                fcfy = None
            ratios['fcfy'].append(fcfy)
            
        except Exception as e:
            for key in ratios:
                ratios[key].append(None)
    
    return ratios


def calculate_slopes(series: List, span_short: int = 4, span_long: int = 20) -> Tuple[Optional[float], Optional[float]]:
    """
    Calculate EMA-based slopes for 5-quarter and 20-quarter periods.
    Returns (slope_5, slope_20)
    """
    if not series or len(series) < 5:
        return None, None
    
    # Convert to pandas series, handling None values
    s = pd.Series(series)
    s = s.replace([np.inf, -np.inf], np.nan)
    
    # 5-Quarter slope (using span=4)
    slope_5 = None
    if len(s.dropna()) >= 5:
        try:
            ema_short = s.ewm(span=span_short, adjust=False).mean()
            if len(ema_short) >= 5 and pd.notna(ema_short.iloc[-1]) and pd.notna(ema_short.iloc[-5]):
                if abs(ema_short.iloc[-5]) > 0.0001:
                    slope_5 = (ema_short.iloc[-1] - ema_short.iloc[-5]) / abs(ema_short.iloc[-5])
        except:
            pass
    
    # 20-Quarter slope
    slope_20 = None
    if len(s.dropna()) >= 21:
        try:
            ema_long = s.ewm(span=span_long, adjust=False).mean()
            if len(ema_long) >= 21 and pd.notna(ema_long.iloc[-1]) and pd.notna(ema_long.iloc[-21]):
                if abs(ema_long.iloc[-21]) > 0.0001:
                    slope_20 = (ema_long.iloc[-1] - ema_long.iloc[-21]) / abs(ema_long.iloc[-21])
        except:
            pass
    
    return slope_5, slope_20


def calculate_all_slopes(fundamentals: Dict, ratios: Dict) -> Dict:
    """
    Calculate all fundamental slopes for merit scoring.
    """
    slopes = {}
    
    # Revenue slopes
    slopes['Rev_Slope_5'], slopes['Rev_Slope_20'] = calculate_slopes(fundamentals.get('revenue', []))
    
    # FCF slopes
    slopes['FCF_Slope_5'], slopes['FCF_Slope_20'] = calculate_slopes(fundamentals.get('fcf', []))
    
    # Net Income slopes (as proxy for EPS/Deps)
    slopes['Deps_Slope_5'], slopes['Deps_Slope_20'] = calculate_slopes(fundamentals.get('net_income', []))
    
    # Ratio slopes
    slopes['P/E Ratio_Slope_5'], slopes['P/E Ratio_Slope_20'] = calculate_slopes(ratios.get('pe_ratio', []))
    slopes['Current Ratio_Slope_5'], slopes['Current Ratio_Slope_20'] = calculate_slopes(ratios.get('current_ratio', []))
    slopes['Return on Equity_Slope_5'], slopes['Return on Equity_Slope_20'] = calculate_slopes(ratios.get('roe', []))
    slopes['Return on Assets_Slope_5'], slopes['Return on Assets_Slope_20'] = calculate_slopes(ratios.get('roa', []))
    slopes['Net Profit Margin_Slope_5'], slopes['Net Profit Margin_Slope_20'] = calculate_slopes(ratios.get('net_profit_margin', []))
    slopes['Debt to Equity Ratio_Slope_5'], slopes['Debt to Equity Ratio_Slope_20'] = calculate_slopes(ratios.get('debt_to_equity', []))
    slopes['Price to Book Ratio_Slope_5'], slopes['Price to Book Ratio_Slope_20'] = calculate_slopes(ratios.get('price_to_book', []))
    slopes['Price to Sales Ratio_Slope_5'], slopes['Price to Sales Ratio_Slope_20'] = calculate_slopes(ratios.get('price_to_sales', []))
    slopes['Asset Turnover_Slope_5'], slopes['Asset Turnover_Slope_20'] = calculate_slopes(ratios.get('asset_turnover', []))
    
    # Get latest FCFY
    fcfy_list = ratios.get('fcfy', [])
    slopes['FCFY'] = fcfy_list[-1] if fcfy_list and fcfy_list[-1] is not None else None
    
    return slopes


def fetch_all_fundamental_data():
    """
    Fetch fundamental data for all symbols and calculate slopes.
    """
    print("\n📊 FETCHING FUNDAMENTAL DATA...")
    
    success_count = 0
    fail_count = 0
    
    for i, symbol in enumerate(config.symbols):
        try:
            # Fetch raw fundamental data
            fundamentals = fetch_fundamental_data_polygon(symbol)
            
            if fundamentals and len(fundamentals.get('revenue', [])) >= 4:
                # Get current price and market cap from week52 data
                current_price = None
                market_cap = None
                
                if symbol in config.week52_data:
                    w52 = config.week52_data[symbol]
                    if w52.get('high') and w52.get('low'):
                        # Estimate current price from percentile
                        current_price = (w52['high'] + w52['low']) / 2
                
                # Fetch current quote for accurate price
                try:
                    quote_url = f"{config.polygon_rest_url}/v2/aggs/ticker/{symbol}/prev?adjusted=true&apiKey={config.polygon_api_key}"
                    quote_resp = requests.get(quote_url, timeout=10)
                    if quote_resp.status_code == 200:
                        quote_data = quote_resp.json()
                        if quote_data.get('results') and len(quote_data['results']) > 0:
                            current_price = quote_data['results'][0].get('c', current_price)
                except:
                    pass
                
                # Estimate market cap (shares outstanding * price)
                # For now, use a rough estimate from fundamentals
                if fundamentals.get('shareholders_equity') and current_price:
                    # Very rough estimate
                    latest_equity = fundamentals['shareholders_equity'][-1]
                    if latest_equity and latest_equity > 0:
                        market_cap = latest_equity * 2  # Rough multiplier
                    else:
                        market_cap = 1e9  # Default 1B
                else:
                    market_cap = 1e9
                
                if current_price is None:
                    current_price = 100  # Default
                
                # Calculate ratios
                ratios = calculate_financial_ratios(fundamentals, current_price, market_cap)
                
                # Calculate all slopes
                slopes = calculate_all_slopes(fundamentals, ratios)
                
                # Store
                config.fundamental_data[symbol] = fundamentals
                config.fundamental_slopes[symbol] = slopes
                
                success_count += 1
            else:
                fail_count += 1
                
        except Exception as e:
            fail_count += 1
        
        if (i + 1) % 25 == 0:
            print(f"   📈 Fundamentals: {i + 1}/{len(config.symbols)} (✓{success_count} ✗{fail_count})")
        
        time.sleep(0.15)  # Rate limiting
    
    print(f"✅ Fundamental data: {success_count} success, {fail_count} failed\n")


# ============================================================================
# MERIT SCORE CALCULATIONS
# ============================================================================

def calculate_stasis_merit_score(snapshot: Dict) -> int:
    """
    Calculate Stasis Merit Score (SMS) based on:
    - Stasis count (pattern strength)
    - Risk:Reward ratio
    - Signal strength
    - Duration (confirmation)
    
    Max Score: 22 points
    """
    merit_score = 0
    
    # Stasis count scoring (0-10 points)
    stasis = snapshot.get('stasis', 0)
    if stasis >= 15:
        merit_score += 10
    elif stasis >= 12:
        merit_score += 9
    elif stasis >= 10:
        merit_score += 8
    elif stasis >= 8:
        merit_score += 7
    elif stasis >= 7:
        merit_score += 6
    elif stasis >= 6:
        merit_score += 5
    elif stasis >= 5:
        merit_score += 4
    elif stasis >= 4:
        merit_score += 3
    elif stasis >= 3:
        merit_score += 2
    elif stasis >= 2:
        merit_score += 1
    
    # Risk:Reward scoring (0-5 points)
    rr = snapshot.get('risk_reward')
    if rr is not None:
        if rr >= 3:
            merit_score += 5
        elif rr >= 2.5:
            merit_score += 4
        elif rr >= 2:
            merit_score += 3
        elif rr >= 1.5:
            merit_score += 2
        elif rr >= 1:
            merit_score += 1
    
    # Signal strength scoring (0-4 points)
    strength = snapshot.get('signal_strength')
    if strength == 'VERY_STRONG':
        merit_score += 4
    elif strength == 'STRONG':
        merit_score += 3
    elif strength == 'MODERATE':
        merit_score += 2
    elif strength == 'WEAK':
        merit_score += 1
    
    # Duration scoring (0-3 points)
    duration_seconds = snapshot.get('duration_seconds', 0)
    if duration_seconds >= 3600:
        merit_score += 3
    elif duration_seconds >= 1800:
        merit_score += 2
    elif duration_seconds >= 900:
        merit_score += 1
    
    return merit_score


def calculate_fundamental_merit_score(symbol: str, week52_percentile: Optional[float]) -> Tuple[int, Dict]:
    """
    Calculate Fundamental Merit Score (FMS) based on:
    - Fundamental growth SLOPES (steepness)
    - 52-week percentile (value position)
    - FCFY (cash yield)
    
    Max Score: ~55 points
    
    Returns: (score, slope_details_dict)
    """
    merit_score = 0
    slope_details = {}
    
    slopes = config.fundamental_slopes.get(symbol, {})
    
    if not slopes:
        # No fundamental data available - return with just 52W percentile scoring
        if week52_percentile is not None:
            if week52_percentile <= 5: merit_score += 8
            elif week52_percentile <= 15: merit_score += 7
            elif week52_percentile <= 25: merit_score += 6
            elif week52_percentile <= 35: merit_score += 5
            elif week52_percentile <= 45: merit_score += 4
            elif week52_percentile <= 55: merit_score += 3
            elif week52_percentile <= 65: merit_score += 2
            elif week52_percentile <= 75: merit_score += 1
        return merit_score, slope_details
    
    # ========================================
    # GROWTH METRICS - Positive slope = GOOD
    # ========================================
    
    # REVENUE SLOPES (critical growth indicator)
    rev_slope_5 = slopes.get('Rev_Slope_5')
    rev_slope_20 = slopes.get('Rev_Slope_20')
    slope_details['Rev_5'] = rev_slope_5
    slope_details['Rev_20'] = rev_slope_20
    
    if rev_slope_5 is not None:
        if rev_slope_5 >= 0.30: merit_score += 4      # 30%+ = exceptional growth
        elif rev_slope_5 >= 0.20: merit_score += 3    # 20%+ = strong
        elif rev_slope_5 >= 0.10: merit_score += 2    # 10%+ = solid
        elif rev_slope_5 >= 0.05: merit_score += 1    # 5%+ = positive
    
    if rev_slope_20 is not None:
        if rev_slope_20 >= 0.20: merit_score += 3
        elif rev_slope_20 >= 0.10: merit_score += 2
        elif rev_slope_20 >= 0.05: merit_score += 1
    
    # FCF SLOPES (cash generation acceleration)
    fcf_slope_5 = slopes.get('FCF_Slope_5')
    fcf_slope_20 = slopes.get('FCF_Slope_20')
    slope_details['FCF_5'] = fcf_slope_5
    slope_details['FCF_20'] = fcf_slope_20
    
    if fcf_slope_5 is not None:
        if fcf_slope_5 >= 0.40: merit_score += 4
        elif fcf_slope_5 >= 0.25: merit_score += 3
        elif fcf_slope_5 >= 0.10: merit_score += 2
        elif fcf_slope_5 >= 0.05: merit_score += 1
    
    if fcf_slope_20 is not None:
        if fcf_slope_20 >= 0.25: merit_score += 3
        elif fcf_slope_20 >= 0.15: merit_score += 2
        elif fcf_slope_20 >= 0.05: merit_score += 1
    
    # ROE SLOPES (profitability improvement)
    roe_slope_5 = slopes.get('Return on Equity_Slope_5')
    roe_slope_20 = slopes.get('Return on Equity_Slope_20')
    slope_details['ROE_5'] = roe_slope_5
    
    if roe_slope_5 is not None:
        if roe_slope_5 >= 0.20: merit_score += 2
        elif roe_slope_5 >= 0.10: merit_score += 1
    
    if roe_slope_20 is not None:
        if roe_slope_20 >= 0.15: merit_score += 2
        elif roe_slope_20 >= 0.08: merit_score += 1
    
    # ROA SLOPES
    roa_slope_5 = slopes.get('Return on Assets_Slope_5')
    slope_details['ROA_5'] = roa_slope_5
    
    if roa_slope_5 is not None:
        if roa_slope_5 >= 0.15: merit_score += 2
        elif roa_slope_5 >= 0.08: merit_score += 1
    
    # NET PROFIT MARGIN SLOPES (margin expansion)
    npm_slope_5 = slopes.get('Net Profit Margin_Slope_5')
    npm_slope_20 = slopes.get('Net Profit Margin_Slope_20')
    slope_details['NPM_5'] = npm_slope_5
    
    if npm_slope_5 is not None:
        if npm_slope_5 >= 0.20: merit_score += 2
        elif npm_slope_5 >= 0.10: merit_score += 1
    
    if npm_slope_20 is not None:
        if npm_slope_20 >= 0.15: merit_score += 2
        elif npm_slope_20 >= 0.08: merit_score += 1
    
    # ASSET TURNOVER & CURRENT RATIO
    at_slope_5 = slopes.get('Asset Turnover_Slope_5')
    cr_slope_5 = slopes.get('Current Ratio_Slope_5')
    
    if at_slope_5 is not None and at_slope_5 >= 0.10: merit_score += 1
    if cr_slope_5 is not None and cr_slope_5 >= 0.10: merit_score += 1
    
    # ========================================
    # VALUATION METRICS - Negative slope = GOOD
    # (Stock getting CHEAPER relative to fundamentals)
    # ========================================
    
    # P/E RATIO SLOPES (declining = getting cheaper)
    pe_slope_5 = slopes.get('P/E Ratio_Slope_5')
    pe_slope_20 = slopes.get('P/E Ratio_Slope_20')
    slope_details['PE_5'] = pe_slope_5
    
    if pe_slope_5 is not None:
        if pe_slope_5 <= -0.25: merit_score += 3
        elif pe_slope_5 <= -0.15: merit_score += 2
        elif pe_slope_5 <= -0.05: merit_score += 1
    
    if pe_slope_20 is not None:
        if pe_slope_20 <= -0.20: merit_score += 2
        elif pe_slope_20 <= -0.10: merit_score += 1
    
    # DEBT TO EQUITY SLOPES (declining = deleveraging)
    de_slope_5 = slopes.get('Debt to Equity Ratio_Slope_5')
    de_slope_20 = slopes.get('Debt to Equity Ratio_Slope_20')
    slope_details['DE_5'] = de_slope_5
    
    if de_slope_5 is not None:
        if de_slope_5 <= -0.20: merit_score += 2
        elif de_slope_5 <= -0.10: merit_score += 1
    
    if de_slope_20 is not None:
        if de_slope_20 <= -0.15: merit_score += 2
        elif de_slope_20 <= -0.08: merit_score += 1
    
    # PRICE TO BOOK & PRICE TO SALES SLOPES
    pb_slope_5 = slopes.get('Price to Book Ratio_Slope_5')
    ps_slope_5 = slopes.get('Price to Sales Ratio_Slope_5')
    
    if pb_slope_5 is not None and pb_slope_5 <= -0.20: merit_score += 1
    if ps_slope_5 is not None and ps_slope_5 <= -0.20: merit_score += 1
    
    # ========================================
    # 52-WEEK PERCENTILE (Lower = buying at value)
    # ========================================
    if week52_percentile is not None:
        if week52_percentile <= 5: merit_score += 8
        elif week52_percentile <= 15: merit_score += 7
        elif week52_percentile <= 25: merit_score += 6
        elif week52_percentile <= 35: merit_score += 5
        elif week52_percentile <= 45: merit_score += 4
        elif week52_percentile <= 55: merit_score += 3
        elif week52_percentile <= 65: merit_score += 2
        elif week52_percentile <= 75: merit_score += 1
    
    # ========================================
    # FCFY - Free Cash Flow Yield (Higher = value)
    # ========================================
    fcfy = slopes.get('FCFY')
    slope_details['FCFY'] = fcfy
    
    if fcfy is not None:
        if fcfy >= 0.15: merit_score += 3      # 15%+ FCFY = exceptional
        elif fcfy >= 0.10: merit_score += 2    # 10%+ = strong
        elif fcfy >= 0.05: merit_score += 1    # 5%+ = solid
    
    return merit_score, slope_details


def calculate_combined_merit_score(snapshot: Dict) -> Tuple[int, int, int, Dict]:
    """
    Calculate both merit scores.
    Returns: (stasis_merit, fundamental_merit, combined_total, slope_details)
    """
    sms = calculate_stasis_merit_score(snapshot)
    fms, slope_details = calculate_fundamental_merit_score(
        snapshot.get('symbol', ''),
        snapshot.get('week52_percentile')
    )
    return sms, fms, sms + fms, slope_details


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
                    closes = [bar['c'] for bar in data['results']]
                    
                    high_val = max(highs)
                    low_val = min(lows)
                    current_close = closes[-1] if closes else None
                    
                    week52_data[symbol] = {
                        'high': high_val,
                        'low': low_val,
                        'range': high_val - low_val,
                        'current': current_close,
                    }
                    success_count += 1
                else:
                    week52_data[symbol] = {'high': None, 'low': None, 'range': None, 'current': None}
                    fail_count += 1
            else:
                week52_data[symbol] = {'high': None, 'low': None, 'range': None, 'current': None}
                fail_count += 1
            
            if (i + 1) % 50 == 0:
                print(f"   📈 Processed {i + 1}/{len(config.symbols)} (✓{success_count} ✗{fail_count})...")
            
            time.sleep(0.12)
        except:
            week52_data[symbol] = {'high': None, 'low': None, 'range': None, 'current': None}
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
            
            if (i + 1) % 50 == 0:
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
            
            # Build base snapshot
            snapshot = {
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
            
            # Calculate merit scores
            sms, fms, combined, slope_details = calculate_combined_merit_score(snapshot)
            snapshot['stasis_merit_score'] = sms
            snapshot['fundamental_merit_score'] = fms
            snapshot['combined_merit_score'] = combined
            snapshot['slope_details'] = slope_details
            
            return snapshot


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
            
            if (i + 1) % 25 == 0:
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


def format_slope(slope: Optional[float]) -> str:
    if slope is None:
        return "—"
    sign = "+" if slope >= 0 else ""
    return f"{sign}{slope*100:.1f}%"


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
        
        # Get slope details
        slopes = d.get('slope_details', {})
        
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
            'TP': f"${d['take_profit']:.2f}" if d['take_profit'] else "—",
            'SL': f"${d['stop_loss']:.2f}" if d['stop_loss'] else "—",
            'R:R': format_rr(d['risk_reward']),
            'RR_Val': d['risk_reward'] if d['risk_reward'] is not None else -1,
            'Started': d['stasis_start_str'],
            'Duration': d['stasis_duration_str'],
            'Dur_Val': d['duration_seconds'],
            'Chg': chg_str,
            '52W': w52_str,
            '52W_Val': d['week52_percentile'] if d['week52_percentile'] is not None else -1,
            # Merit Scores
            'SMS': d.get('stasis_merit_score', 0),
            'FMS': d.get('fundamental_merit_score', 0),
            'TMS': d.get('combined_merit_score', 0),
            # Key Slopes for display
            'Rev5': format_slope(slopes.get('Rev_5')),
            'Rev5_Val': slopes.get('Rev_5') if slopes.get('Rev_5') is not None else -999,
            'FCF5': format_slope(slopes.get('FCF_5')),
            'FCF5_Val': slopes.get('FCF_5') if slopes.get('FCF_5') is not None else -999,
            'ROE5': format_slope(slopes.get('ROE_5')),
            'FCFY': f"{slopes.get('FCFY', 0)*100:.1f}%" if slopes.get('FCFY') else "—",
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

h1, h2, h3, h4, h5, h6, .btn, label, .nav-link, .card-header {
    font-family: 'Orbitron', sans-serif !important;
}

td, input, .form-control, pre, code {
    font-family: 'Roboto Mono', monospace !important;
}

.Select-control, .Select-menu-outer, .Select-option, .Select-value-label {
    font-family: 'Roboto Mono', monospace !important;
    font-size: 11px !important;
}
"""

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
app.title = "Beyond Price & Time"
server = app.server

app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>''' + CUSTOM_CSS + '''</style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
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
                    html.P("STASIS + FUNDAMENTAL SLOPE MERIT SCORING", className="text-muted title-font",
                          style={'fontSize': '10px', 'letterSpacing': '2px', 'marginBottom': '0'}),
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
                        options=[{'label': 'ALL', 'value': 'ALL'}] + 
                        [{'label': s, 'value': s} for s in config.symbols],
                        value='ALL', clearable=False, style={'fontSize': '10px'})
        ], width=1),
        dbc.Col([
            dcc.Input(id='filter-stasis', type='number', value=3, min=0,
                     placeholder="Min Stasis",
                     style={'width': '100%', 'fontSize': '11px', 'fontFamily': 'Roboto Mono'})
        ], width=1),
        dbc.Col([
            dcc.Dropdown(id='filter-direction',
                        options=[{'label': 'ALL', 'value': 'ALL'}, 
                                {'label': 'LONG', 'value': 'LONG'},
                                {'label': 'SHORT', 'value': 'SHORT'}],
                        value='ALL', clearable=False, style={'fontSize': '10px'})
        ], width=1),
        dbc.Col([
            dcc.Dropdown(id='filter-fms',
                        options=[{'label': 'ANY FMS', 'value': -1},
                                {'label': 'FMS ≥30', 'value': 30},
                                {'label': 'FMS ≥25', 'value': 25},
                                {'label': 'FMS ≥20', 'value': 20},
                                {'label': 'FMS ≥15', 'value': 15},
                                {'label': 'FMS ≥10', 'value': 10}],
                        value=-1, clearable=False, style={'fontSize': '10px'})
        ], width=1),
        dbc.Col([
            dcc.Dropdown(id='filter-tms',
                        options=[{'label': 'ANY TMS', 'value': -1},
                                {'label': 'TMS ≥50', 'value': 50},
                                {'label': 'TMS ≥40', 'value': 40},
                                {'label': 'TMS ≥30', 'value': 30},
                                {'label': 'TMS ≥20', 'value': 20}],
                        value=-1, clearable=False, style={'fontSize': '10px'})
        ], width=1),
        dbc.Col([
            dcc.Dropdown(id='filter-52w',
                        options=[{'label': 'ANY 52W', 'value': 'ALL'},
                                {'label': '0-20%', 'value': '0-20'}, 
                                {'label': '20-40%', 'value': '20-40'},
                                {'label': '40-60%', 'value': '40-60'}],
                        value='ALL', clearable=False, style={'fontSize': '10px'})
        ], width=1),
        dbc.Col([
            dcc.Dropdown(id='filter-rows',
                        options=[{'label': '50', 'value': 50}, 
                                {'label': '100', 'value': 100},
                                {'label': '250', 'value': 250},
                                {'label': 'ALL', 'value': 10000}],
                        value=100, clearable=False, style={'fontSize': '10px'})
        ], width=1),
        dbc.Col([
            dcc.Dropdown(id='filter-sort',
                        options=[{'label': 'TMS ↓', 'value': 'tms'},
                                {'label': 'FMS ↓', 'value': 'fms'},
                                {'label': 'SMS ↓', 'value': 'sms'},
                                {'label': 'REV SLOPE ↓', 'value': 'rev'},
                                {'label': 'FCF SLOPE ↓', 'value': 'fcf'},
                                {'label': 'STASIS ↓', 'value': 'stasis'}, 
                                {'label': '52W ↑', 'value': '52w'}],
                        value='tms', clearable=False, style={'fontSize': '10px'})
        ], width=1),
    ], className="mb-2 g-1"),
    
    # Table
    dbc.Row([
        dbc.Col([
            dash_table.DataTable(
                id='main-table',
                columns=[
                    {'name': '✓', 'id': 'Tradable'},
                    {'name': 'SYM', 'id': 'Symbol'},
                    {'name': 'BAND', 'id': 'Band'},
                    {'name': 'STS', 'id': 'Stasis'},
                    {'name': 'DIR', 'id': 'Dir'},
                    {'name': 'SMS', 'id': 'SMS'},
                    {'name': 'FMS', 'id': 'FMS'},
                    {'name': 'TMS', 'id': 'TMS'},
                    {'name': 'REV5', 'id': 'Rev5'},
                    {'name': 'FCF5', 'id': 'FCF5'},
                    {'name': 'ROE5', 'id': 'ROE5'},
                    {'name': 'FCFY', 'id': 'FCFY'},
                    {'name': '52W', 'id': '52W'},
                    {'name': 'PRICE', 'id': 'Current'},
                    {'name': 'TP', 'id': 'TP'},
                    {'name': 'SL', 'id': 'SL'},
                    {'name': 'R:R', 'id': 'R:R'},
                    {'name': 'DUR', 'id': 'Duration'},
                    {'name': 'CHG', 'id': 'Chg'},
                ],
                sort_action='native',
                sort_mode='multi',
                sort_by=[{'column_id': 'TMS', 'direction': 'desc'}],
                style_table={'height': '62vh', 'overflowY': 'auto'},
                style_cell={
                    'backgroundColor': '#1a1a2e', 
                    'color': 'white',
                    'padding': '4px 6px', 
                    'fontSize': '11px',
                    'fontFamily': 'Roboto Mono, monospace', 
                    'whiteSpace': 'nowrap',
                    'textAlign': 'right',
                    'minWidth': '40px',
                },
                style_cell_conditional=[
                    {'if': {'column_id': 'Symbol'}, 'textAlign': 'left', 'fontWeight': '600'},
                    {'if': {'column_id': 'Dir'}, 'textAlign': 'center'},
                    {'if': {'column_id': 'Tradable'}, 'textAlign': 'center'},
                ],
                style_header={
                    'backgroundColor': '#2a2a4e', 
                    'color': '#00ff88',
                    'fontWeight': '700', 
                    'fontSize': '10px',
                    'fontFamily': 'Orbitron, sans-serif',
                    'borderBottom': '2px solid #00ff88',
                    'textAlign': 'center',
                },
                style_data_conditional=[
                    {'if': {'filter_query': '{Stasis} >= 10'}, 'backgroundColor': '#2d4a2d'},
                    {'if': {'filter_query': '{Stasis} >= 7 && {Stasis} < 10'}, 'backgroundColor': '#2a3a2a'},
                    {'if': {'filter_query': '{Dir} = "LONG"', 'column_id': 'Dir'}, 'color': '#00ff00', 'fontWeight': 'bold'},
                    {'if': {'filter_query': '{Dir} = "SHORT"', 'column_id': 'Dir'}, 'color': '#ff4444', 'fontWeight': 'bold'},
                    {'if': {'column_id': 'Current'}, 'color': '#00ffff', 'fontWeight': '600'},
                    {'if': {'column_id': 'TP'}, 'color': '#00ff00'},
                    {'if': {'column_id': 'SL'}, 'color': '#ff4444'},
                    # TMS coloring
                    {'if': {'filter_query': '{TMS} >= 50', 'column_id': 'TMS'}, 'backgroundColor': '#00ff00', 'color': '#000'},
                    {'if': {'filter_query': '{TMS} >= 40 && {TMS} < 50', 'column_id': 'TMS'}, 'backgroundColor': '#44ff44', 'color': '#000'},
                    {'if': {'filter_query': '{TMS} >= 30 && {TMS} < 40', 'column_id': 'TMS'}, 'backgroundColor': '#88ff00', 'color': '#000'},
                    {'if': {'filter_query': '{TMS} >= 20 && {TMS} < 30', 'column_id': 'TMS'}, 'color': '#aaff00'},
                    # FMS coloring
                    {'if': {'filter_query': '{FMS} >= 30', 'column_id': 'FMS'}, 'backgroundColor': '#ffaa00', 'color': '#000'},
                    {'if': {'filter_query': '{FMS} >= 20 && {FMS} < 30', 'column_id': 'FMS'}, 'color': '#ffaa00'},
                    # Slope coloring (positive = green)
                    {'if': {'filter_query': '{Rev5} contains "+"', 'column_id': 'Rev5'}, 'color': '#00ff00'},
                    {'if': {'filter_query': '{Rev5} contains "-"', 'column_id': 'Rev5'}, 'color': '#ff4444'},
                    {'if': {'filter_query': '{FCF5} contains "+"', 'column_id': 'FCF5'}, 'color': '#00ff00'},
                    {'if': {'filter_query': '{FCF5} contains "-"', 'column_id': 'FCF5'}, 'color': '#ff4444'},
                    {'if': {'filter_query': '{ROE5} contains "+"', 'column_id': 'ROE5'}, 'color': '#00ff00'},
                    {'if': {'filter_query': '{ROE5} contains "-"', 'column_id': 'ROE5'}, 'color': '#ff4444'},
                    # 52W coloring
                    {'if': {'filter_query': '{52W_Val} >= 0 && {52W_Val} <= 20', 'column_id': '52W'}, 'color': '#00ff00'},
                    {'if': {'filter_query': '{52W_Val} >= 80', 'column_id': '52W'}, 'color': '#ff4444'},
                    # Chg coloring
                    {'if': {'filter_query': '{Chg} contains "+"', 'column_id': 'Chg'}, 'color': '#00ff00'},
                    {'if': {'filter_query': '{Chg} contains "-"', 'column_id': 'Chg'}, 'color': '#ff4444'},
                    {'if': {'row_index': 'odd'}, 'backgroundColor': '#151520'},
                ]
            )
        ])
    ]),
    
    # Footer
    dbc.Row([
        dbc.Col([
            html.Div([
                html.Span("SMS", style={'color': '#00ffff', 'fontSize': '9px', 'fontWeight': 'bold'}),
                html.Span(" = Stasis Merit | ", style={'fontSize': '9px'}),
                html.Span("FMS", style={'color': '#ffaa00', 'fontSize': '9px', 'fontWeight': 'bold'}),
                html.Span(" = Fundamental Merit (Slopes+52W+FCFY) | ", style={'fontSize': '9px'}),
                html.Span("REV5/FCF5/ROE5", style={'color': '#00ff00', 'fontSize': '9px', 'fontWeight': 'bold'}),
                html.Span(" = 5Q Growth Slopes", style={'fontSize': '9px'}),
            ], className="text-center text-muted mt-1"),
            html.Hr(style={'borderColor': '#333', 'margin': '8px 0'}),
            html.P("© 2026 TRUTH COMMUNICATIONS LLC",
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
    fund_count = len(config.fundamental_slopes)
    
    if status['connected'] == 0:
        return html.Span(f"🔴 CONNECTING... | 📊 {fund_count} fundamentals", className="text-warning")
    return html.Span(f"🟢 LIVE {status['connected']}/{status['total']} | 📊 {fund_count} fundamentals", 
                    className="text-success data-font")


@app.callback(
    Output('stats-display', 'children'),
    Input('refresh-interval', 'n_intervals')
)
def update_stats(n):
    if not manager.backfill_complete:
        return html.Span(f"⏳ LOADING... {manager.backfill_progress}%", className="text-warning title-font")
    
    data = manager.get_data()
    if not data:
        return html.Span("LOADING...", className="text-muted title-font")
    
    tradable = [d for d in data if d['is_tradable']]
    long_count = sum(1 for d in tradable if d['direction'] == 'LONG')
    short_count = sum(1 for d in tradable if d['direction'] == 'SHORT')
    
    avg_fms = np.mean([d.get('fundamental_merit_score', 0) for d in tradable]) if tradable else 0
    avg_tms = np.mean([d.get('combined_merit_score', 0) for d in tradable]) if tradable else 0
    max_tms = max([d.get('combined_merit_score', 0) for d in tradable]) if tradable else 0
    
    return html.Div([
        html.Span("🎯 TRADABLE: ", className="title-font", style={'fontSize': '11px'}),
        html.Span(f"{len(tradable)}", className="data-font text-success", style={'fontSize': '12px', 'fontWeight': '600'}),
        html.Span("  📈 LONG: ", className="title-font ms-2", style={'fontSize': '11px'}),
        html.Span(f"{long_count}", className="data-font text-success", style={'fontSize': '12px'}),
        html.Span("  📉 SHORT: ", className="title-font ms-2", style={'fontSize': '11px'}),
        html.Span(f"{short_count}", className="data-font text-danger", style={'fontSize': '12px'}),
        html.Span("  📊 AVG FMS: ", className="title-font ms-2", style={'fontSize': '11px'}),
        html.Span(f"{avg_fms:.1f}", className="data-font text-warning", style={'fontSize': '12px'}),
        html.Span("  🏆 AVG TMS: ", className="title-font ms-2", style={'fontSize': '11px'}),
        html.Span(f"{avg_tms:.1f}", className="data-font text-success", style={'fontSize': '12px'}),
        html.Span("  🥇 MAX TMS: ", className="title-font ms-2", style={'fontSize': '11px'}),
        html.Span(f"{max_tms}", className="data-font text-warning", style={'fontSize': '12px', 'fontWeight': '600'}),
    ])


@app.callback(
    Output('main-table', 'data'),
    [Input('refresh-interval', 'n_intervals'),
     Input('view-mode', 'data'),
     Input('filter-symbol', 'value'),
     Input('filter-stasis', 'value'),
     Input('filter-direction', 'value'),
     Input('filter-fms', 'value'),
     Input('filter-tms', 'value'),
     Input('filter-52w', 'value'),
     Input('filter-rows', 'value'),
     Input('filter-sort', 'value')]
)
def update_table(n, view_mode, sym, stasis, direction, fms_min, tms_min, w52, rows, sort):
    df = get_table_data()
    if df.empty:
        return []
    
    if view_mode == 'tradable':
        df = df[df['Is_Tradable'] == True]
    
    if sym != 'ALL':
        df = df[df['Symbol'] == sym]
    if stasis and stasis > 0:
        df = df[df['Stasis'] >= stasis]
    if direction != 'ALL':
        df = df[df['Dir'] == direction]
    if fms_min is not None and fms_min >= 0:
        df = df[df['FMS'] >= fms_min]
    if tms_min is not None and tms_min >= 0:
        df = df[df['TMS'] >= tms_min]
    if w52 != 'ALL':
        ranges = {'0-20': (0, 20), '20-40': (20, 40), '40-60': (40, 60)}
        if w52 in ranges:
            lo, hi = ranges[w52]
            df = df[(df['52W_Val'] >= lo) & (df['52W_Val'] <= hi)]
    
    # Sorting
    if sort == 'tms':
        df = df.sort_values(['TMS', 'FMS'], ascending=[False, False])
    elif sort == 'fms':
        df = df.sort_values(['FMS', 'TMS'], ascending=[False, False])
    elif sort == 'sms':
        df = df.sort_values(['SMS', 'TMS'], ascending=[False, False])
    elif sort == 'rev':
        df = df.sort_values(['Rev5_Val', 'TMS'], ascending=[False, False])
    elif sort == 'fcf':
        df = df.sort_values(['FCF5_Val', 'TMS'], ascending=[False, False])
    elif sort == 'stasis':
        df = df.sort_values(['Stasis', 'TMS'], ascending=[False, False])
    elif sort == '52w':
        df = df.sort_values(['52W_Val', 'TMS'], ascending=[True, False], na_position='last')
    
    df = df.head(rows)
    
    drop_cols = ['Band_Val', 'Current_Val', 'RR_Val', 'Dur_Val', '52W_Val', 
                 'Is_Tradable', 'Rev5_Val', 'FCF5_Val', 'Anchor']
    df = df.drop(columns=drop_cols, errors='ignore')
    
    return df.to_dict('records')


# ============================================================================
# INITIALIZATION
# ============================================================================

_initialized = False
_init_lock = threading.Lock()


def initialize_app():
    global _initialized
    
    with _init_lock:
        if _initialized:
            return
        
        print("=" * 70)
        print("  BEYOND PRICE AND TIME")
        print("  WITH FUNDAMENTAL SLOPE MERIT SCORING")
        print("  © 2026 Truth Communications LLC")
        print("=" * 70)
        
        print(f"\n🎯 Stocks: {len(config.symbols)}")
        
        # Fetch 52-week data
        print("\n📅 FETCHING 52-WEEK DATA...")
        config.week52_data = fetch_52_week_data()
        
        # Fetch volume data
        print("📊 FETCHING VOLUME DATA...")
        config.volumes = fetch_volume_data()
        
        # Fetch fundamental data and calculate slopes
        fetch_all_fundamental_data()
        
        # Backfill historical price data
        manager.backfill()
        
        # Start price feed and manager
        price_feed.start()
        manager.start()
        
        print("\n✅ Initialization complete!")
        print(f"📊 Fundamental slopes calculated for {len(config.fundamental_slopes)} symbols")
        print("=" * 70)
        
        _initialized = True


_init_thread = threading.Thread(target=initialize_app, daemon=True)
_init_thread.start()


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    import os
    
    _init_thread.join()
    
    print("\n✅ Server: http://127.0.0.1:8050")
    
    threading.Thread(
        target=lambda: (time.sleep(2), webbrowser.open('http://127.0.0.1:8050')), 
        daemon=True
    ).start()
    
    port = int(os.environ.get('PORT', 8050))
    app.run(debug=False, host='0.0.0.0', port=port)
