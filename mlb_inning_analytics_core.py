import pybaseball as pyb
import pandas as pd
from datetime import datetime, timedelta
import os
import logging
import glob
import statsapi
import requests
import json
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from itertools import combinations
import io # Added for in-memory PDF generation

# --- Configuration ---
BASE_CACHE_DIR = 'cache/raw_data_2025_by_inning' # Base directory for all inning data
FULL_GAME_CACHE_DIR = 'cache/raw_data_2025_full_game' # New: Base directory for full game data
LOG_FILENAME = 'multi_inning_data_pull.log' # Updated log filename
START_2025_SEASON = '2025-03-28'
CURRENT_DATE = datetime.now().strftime('%Y-%m-%d')
REPORTS_DIR = 'reports' # Base directory for all generated reports
INNINGS_TO_PROCESS = list(range(1, 10)) # Define which innings to process (now 1-9)
FULL_GAME_REPORTS_SUBDIR = 'full_game_reports' # Subdirectory for new full game reports


# --- Prediction Thresholds (Configurable) ---
# For 'Under/Over Recommendation'
OVER_UNDER_THRESHOLD_PCT = 0.10 # 10% deviation from average for Under/Over recommendation
MIN_GAMES_FOR_HIGH_CONFIDENCE = 10 # Minimum games for 'High' confidence in predictions
MIN_GAMES_FOR_MODERATE_CONFIDENCE = 5 # Minimum games for 'Moderate' confidence in predictions


# --- Logging Setup ---
os.makedirs('logs', exist_ok=True)
logging.basicConfig(filename=os.path.join('logs', LOG_FILENAME), level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Create base cache directory and reports directory
os.makedirs(BASE_CACHE_DIR, exist_ok=True)
os.makedirs(FULL_GAME_CACHE_DIR, exist_ok=True) # Create full game cache directory
os.makedirs(REPORTS_DIR, exist_ok=True)


# New: Team abbreviation map for standardizing team names
team_abbreviation_map = {
    "Cincinnati Reds": "CIN",
    "Pittsburgh Pirates": "PIT",
    "Chicago Cubs": "CHC",
    "Miami Marlins": "MIA",
    "New York Mets": "NYM",
    "Boston Red Sox": "BOS",
    "Houston Astros": "HOU",
    "Tampa Bay Rays": "TB",
    "Cleveland Guardians": "CLE",
    "Minnesota Twins": "MIN",
    "Seattle Mariners": "SEA",
    "Chicago White Sox": "CWS",
    "Baltimore Orioles": "BAL",
    "Milwaukee Brewers": "MIL",
    "Detroit Tigers": "DET",
    "St. Louis Cardinals": "STL",
    "Philadelphia Phillies": "PHI",
    "Colorado Rockies": "COL",
    "Kansas City Royals": "KC",
    "San Francisco Giants": "SF",
    "Los Angeles Angels": "LAA",
    "Athletics": "OAK", # Oakland Athletics
    "Oakland Athletics": "OAK",
    "Arizona Diamondbacks": "ARI",
    "Los Angeles Dodgers": "LAD",
    "New York Yankees": "NYY",
    "Toronto Blue Jays": "TOR",
    "Texas Rangers": "TEX",
    "Washington Nationals": "WSH",
    "Atlanta Braves": "ATL",
    "San Diego Padres": "SD", # Added San Diego Padres
    # Add any other team names from your probable pitchers list or Statcast data
}

# Global cache for player names to avoid repeated API calls
player_name_cache = {}

def get_standard_team_abbreviation(team_name):
    """
    Returns the standard abbreviation for a given team name using the mapping.
    """
    return team_abbreviation_map.get(team_name, team_name) # Default to the input if not found

def format_name_last_first(name_str):
    """
    Converts a name from 'First Last' to 'Last, First' format.
    If the name already contains a comma, assumes it's 'Last, First' and returns as is.
    Handles names with multiple parts (e.g., 'Juan F. Lopez' -> 'Lopez, Juan F.').
    """
    if not isinstance(name_str, str):
        return str(name_str) # Ensure it's a string, return as is if not convertible

    if ',' in name_str:
        return name_str # Already in 'Last, First' format

    parts = name_str.strip().split()
    if len(parts) > 1:
        last_name = parts[-1]
        first_names = ' '.join(parts[:-1])
        return f"{last_name}, {first_names}"
    return name_str # Return as is if only one part or empty

def get_player_name_from_id(player_id):
    """
    Looks up a player's full name from their MLB ID using statsapi.
    Caches results to avoid redundant API calls.
    Returns name in 'Last, First' format or 'Player ID: {id}' if not found.
    """
    if player_id in player_name_cache:
        return player_name_cache[player_id]
    try:
        # Use statsapi.get to query the 'people' endpoint with personIds
        player_info_response = statsapi.get('people', {'personIds': player_id})
        
        if player_info_response and 'people' in player_info_response and player_info_response['people']:
            # The 'people' key contains a list of player dictionaries
            player_data = player_info_response['people'][0] # Assuming the first result is the correct one for a given ID
            full_name = player_data.get('fullName')
            if full_name:
                formatted_name = format_name_last_first(full_name)
                player_name_cache[player_id] = formatted_name
                return formatted_name
    except Exception as e:
        logging.error(f"Error looking up player ID {player_id}: {e}")
    player_name_cache[player_id] = f"Player ID: {player_id}" # Cache fallback
    return f"Player ID: {player_id}"


# Define metrics for calculation and reporting - these will now be generated dynamically
# based on the inning number.


def get_pitcher_metrics_for_inning(inning_number: int):
    """
    Generates pitcher metrics configuration for a given inning.
    Each dictionary defines:
    - 'col_name': The column name in the raw data for the metric.
    - 'report_key_avg': The key for the average per game in the report.
    - 'report_key_rate': The key for rate per batters faced in the report (e.g., K rate, Walk rate).
    - 'report_key_per_game_pct': The key for the percentage of games where the metric occurred.
    - 'total_col': The column to sum for the rate calculation's denominator (e.g., 'batters_faced').
    - 'today_report_key': The key for today's metric value in the report.
    - 'last_game_key': The key for the metric's value in the last game.
    """
    return [
        # K's per game - now based on batters faced (K Rate %)
        {'col_name': f'inning_{inning_number}_strikeouts', 'report_key_avg': None, 'report_key_rate': 'PITCH K RATE %', 'report_key_per_game_pct': None, 'total_col': None, 'today_report_key': f'TODAY PITCHER STRIKEOUTS INNING {inning_number}', 'last_game_key': f'LAST GAME PITCHER STRIKEOUTS INNING {inning_number}'}, # total_col set to None
        # Runs Allowed
        {'col_name': f'inning_{inning_number}_runs_allowed', 'report_key_avg': 'PITCH RUNS ALLOWED/GM', 'report_key_rate': None, 'report_key_per_game_pct': 'PITCH RUNS ALLOWED PER GAME %', 'total_col': None, 'today_report_key': f'TODAY PITCHER RUNS ALLOWED INNING {inning_number}', 'last_game_key': f'LAST GAME PITCHER RUNS ALLOWED INNING {inning_number}'},
        # Hits Allowed - Updated names and calculation logic
        {'col_name': f'inning_{inning_number}_hits_allowed', 'report_key_avg': 'PITCH HIT AVG #', 'report_key_rate': None, 'report_key_per_game_pct': 'PITCH NRHI %', 'total_col': None, 'today_report_key': f'TODAY PITCHER HITS INNING {inning_number}', 'last_game_key': f'LAST GAME PITCHER HITS ALLOWED INNING {inning_number}'},
        # NEW: Venue Pitch Hit Avg -> PITCHER VENUE NRHI %
        {'col_name': f'inning_{inning_number}_hits_allowed', 'report_key_avg': 'PITCHER VENUE NRHI %', 'report_key_rate': None, 'report_key_per_game_pct': None, 'total_col': None, 'today_report_key': None, 'last_game_key': None},
        # Singles Allowed - Replicated Hits structure
        {'col_name': f'inning_{inning_number}_singles_allowed', 'report_key_avg': 'PITCH SINGLES ALLOWED/GM', 'report_key_rate': None, 'report_key_per_game_pct': 'PITCH SINGLES ALLOWED PER GAME %', 'total_col': None, 'today_report_key': f'TODAY PITCHER SINGLES INNING {inning_number}', 'last_game_key': f'LAST GAME PITCHER SINGLES ALLOWED INNING {inning_number}'},
        # NEW: Venue Pitch Singles Allowed/GM
        {'col_name': f'inning_{inning_number}_singles_allowed', 'report_key_avg': 'VENUE PITCH SINGLES ALLOWED/GM', 'report_key_rate': None, 'report_key_per_game_pct': None, 'total_col': None, 'today_report_key': None, 'last_game_key': None},
        # Doubles Allowed - Replicated Hits structure
        {'col_name': f'inning_{inning_number}_doubles_allowed', 'report_key_avg': 'PITCH DOUBLES ALLOWED/GM', 'report_key_rate': None, 'report_key_per_game_pct': 'PITCH DOUBLES ALLOWED PER GAME %', 'total_col': None, 'today_report_key': f'TODAY PITCHER DOUBLES INNING {inning_number}', 'last_game_key': f'LAST GAME PITCHER DOUBLES ALLOWED INNING {inning_number}'},
        # NEW: Venue Pitch Doubles Allowed/GM
        {'col_name': f'inning_{inning_number}_doubles_allowed', 'report_key_avg': 'VENUE PITCH DOUBLES ALLOWED/GM', 'report_key_rate': None, 'report_key_per_game_pct': None, 'total_col': None, 'today_report_key': None, 'last_game_key': None},
        # Triples Allowed - Replicated Hits structure
        {'col_name': f'inning_{inning_number}_triples_allowed', 'report_key_avg': 'PITCH TRIPLES ALLOWED/GM', 'report_key_rate': None, 'report_key_per_game_pct': 'PITCH TRIPLES ALLOWED PER GAME %', 'total_col': None, 'today_report_key': f'TODAY PITCHER TRIPLES INNING {inning_number}', 'last_game_key': f'LAST GAME PITCHER TRIPLES ALLOWED INNING {inning_number}'},
        # NEW: Venue Pitch Triples Allowed/GM
        {'col_name': f'inning_{inning_number}_triples_allowed', 'report_key_avg': 'VENUE PITCH TRIPLES ALLOWED/GM', 'report_key_rate': None, 'report_key_per_game_pct': None, 'total_col': None, 'today_report_key': None, 'last_game_key': None},
        # Homers Allowed - Replicated Hits structure
        {'col_name': f'inning_{inning_number}_homers_allowed', 'report_key_avg': 'PITCH HOMERS ALLOWED/GM', 'report_key_rate': None, 'report_key_per_game_pct': 'PITCH HOMERS ALLOWED PER GAME %', 'total_col': None, 'today_report_key': f'TODAY PITCHER HOMERS INNING {inning_number}', 'last_game_key': f'LAST GAME PITCHER HOMERS ALLOWED INNING {inning_number}'},
        # NEW: Venue Pitch Homers Allowed/GM
        {'col_name': f'inning_{inning_number}_homers_allowed', 'report_key_avg': 'VENUE PITCH HOMERS ALLOWED/GM', 'report_key_rate': None, 'report_key_per_game_pct': None, 'total_col': None, 'today_report_key': None, 'last_game_key': None},
        # Total Bases Allowed - Replicated Hits structure
        {'col_name': f'inning_{inning_number}_total_bases_allowed', 'report_key_avg': 'PITCH TOTAL BASES ALLOWED/GM', 'report_key_rate': None, 'report_key_per_game_pct': 'PITCH TOTAL BASES ALLOWED PER GAME %', 'total_col': None, 'today_report_key': f'TODAY PITCHER TOTAL BASES INNING {inning_number}', 'last_game_key': f'LAST GAME PITCHER TOTAL BASES ALLOWED INNING {inning_number}'},
        # NEW: Venue Pitch Total Bases Allowed/GM
        {'col_name': f'inning_{inning_number}_total_bases_allowed', 'report_key_avg': 'VENUE PITCH TOTAL BASES ALLOWED/GM', 'report_key_rate': None, 'report_key_per_game_pct': None, 'total_col': None, 'today_report_key': None, 'last_game_key': None},
        # Walks Allowed - PITCH WALKS ALLOWED/GM and PITCH WALKS ALLOWED RATE % are not generated here
        {'col_name': f'inning_{inning_number}_walks_allowed', 'report_key_avg': None, 'report_key_rate': None, 'report_key_per_game_pct': 'PITCH WALKS ALLOWED PER GAME %', 'total_col': f'inning_{inning_number}_batters_faced', 'today_report_key': f'TODAY PITCHER WALKS INNING {inning_number}', 'last_game_key': f'LAST GAME PITCHER WALKS ALLOWED INNING {inning_number}'},
        # NEW: Venue Pitch Walks Allowed/GM
        {'col_name': f'inning_{inning_number}_walks_allowed', 'report_key_avg': 'VENUE PITCH WALKS ALLOWED/GM', 'report_key_rate': None, 'total_col': None, 'today_report_key': None, 'last_game_key': None},
    ]


def get_batting_metrics_for_inning(inning_number: int):
    """
    Generates batting metrics configuration for a given inning.
    - 'last_game_key': NEW! The key for the metric's value in the last game.
    """
    return [
        {'col_name': f'inning_{inning_number}_strikeouts_batting', 'report_key_avg': 'OPP K/GM', 'report_key_rate': 'OPP K RATE %', 'total_col': None, 'today_report_key': f'TODAY OPPONENT STRIKEOUTS INNING {inning_number}', 'last_game_key': f'LAST GAME OPPONENT STRIKEOUTS BATTING INNING {inning_number}'}, # total_col set to None
        {'col_name': f'inning_{inning_number}_runs_scored', 'report_key_avg': 'OPP R/G', 'report_key_rate': None, 'total_col': None, 'today_report_key': f'TODAY OPPONENT RUNS SCORED INNING {inning_number}', 'last_game_key': f'LAST GAME OPPONENT RUNS SCORED INNING {inning_number}'},
        # Hits Batting - Updated names
        {'col_name': f'inning_{inning_number}_hits_batting', 'report_key_avg': 'BAT HIT AVG #', 'report_key_rate': 'BAT HIT AVG', 'total_col': f'inning_{inning_number}_batters_to_plate', 'today_report_key': f'TODAY OPPONENT HITS INNING {inning_number}', 'last_game_key': f'LAST GAME OPPONENT HITS BATTING INNING {inning_number}'},
        # NEW: Venue Bat Hit Avg -> BATTER VENUE NRHI %
        {'col_name': f'inning_{inning_number}_hits_batting', 'report_key_avg': 'BATTER VENUE NRHI %', 'report_key_rate': None, 'total_col': None, 'today_report_key': None, 'last_game_key': None},
        # NEW: Batting NRHI %
        {'col_name': f'inning_{inning_number}_hits_batting', 'report_key_avg': None, 'report_key_rate': None, 'report_key_per_game_pct': 'BAT NRHI %', 'total_col': None, 'today_report_key': None, 'last_game_key': None},
        # Singles Batting - Replicated Hits structure
        {'col_name': f'inning_{inning_number}_singles_batting', 'report_key_avg': 'OPP SINGLES BATTING/GM', 'report_key_rate': None, 'total_col': None, 'today_report_key': f'TODAY OPPONENT SINGLES INNING {inning_number}', 'last_game_key': f'LAST GAME OPPONENT SINGLES BATTING INNING {inning_number}'},
        # NEW: Venue Bat Singles Batting/GM
        {'col_name': f'inning_{inning_number}_singles_batting', 'report_key_avg': 'VENUE BAT SINGLES BATTING/GM', 'report_key_rate': None, 'total_col': None, 'today_report_key': None, 'last_game_key': None},
        # Doubles Batting - Replicated Hits structure
        {'col_name': f'inning_{inning_number}_doubles_batting', 'report_key_avg': 'OPP DOUBLES BATTING/GM', 'report_key_rate': None, 'total_col': None, 'today_report_key': f'TODAY OPPONENT DOUBLES INNING {inning_number}', 'last_game_key': f'LAST GAME OPPONENT DOUBLES BATTING INNING {inning_number}'},
        # NEW: Venue Bat Doubles Batting/GM
        {'col_name': f'inning_{inning_number}_doubles_batting', 'report_key_avg': 'VENUE BAT DOUBLES BATTING/GM', 'report_key_rate': None, 'total_col': None, 'today_report_key': None, 'last_game_key': None},
        # Triples Batting - Replicated Hits structure
        {'col_name': f'inning_{inning_number}_triples_batting', 'report_key_avg': 'OPP TRIPLES BATTING/GM', 'report_key_rate': None, 'total_col': None, 'today_report_key': f'TODAY OPPONENT TRIPLES INNING {inning_number}', 'last_game_key': f'LAST GAME OPPONENT TRIPLES BATTING INNING {inning_number}'},
        # NEW: Venue Bat Triples Batting/GM
        {'col_name': f'inning_{inning_number}_triples_batting', 'report_key_avg': 'VENUE BAT TRIPLES BATTING/GM', 'report_key_rate': None, 'total_col': None, 'today_report_key': None, 'last_game_key': None},
        # Homers Batting - Replicated Hits structure
        {'col_name': f'inning_{inning_number}_homers_batting', 'report_key_avg': 'OPP HOMERS BATTING/GM', 'report_key_rate': None, 'total_col': None, 'today_report_key': f'TODAY OPPONENT HOMERS INNING {inning_number}', 'last_game_key': f'LAST GAME OPPONENT HOMERS BATTING INNING {inning_number}'},
        # NEW: Venue Bat Homers Batting/GM
        {'col_name': f'inning_{inning_number}_homers_batting', 'report_key_avg': 'VENUE BAT HOMERS BATTING/GM', 'report_key_rate': None, 'total_col': None, 'today_report_key': None, 'last_game_key': None},
        # Total Bases Batting - Replicated Hits structure
        {'col_name': f'inning_{inning_number}_total_bases_batting', 'report_key_avg': 'OPP TOTAL BASES BATTING/GM', 'report_key_rate': None, 'total_col': None, 'today_report_key': f'TODAY OPPONENT TOTAL BASES INNING {inning_number}', 'last_game_key': f'LAST GAME OPPONENT TOTAL BASES BATTING INNING {inning_number}'},
        # NEW: Venue Bat Total Bases Batting/GM
        {'col_name': f'inning_{inning_number}_total_bases_batting', 'report_key_avg': 'VENUE BAT TOTAL BASES BATTING/GM', 'report_key_rate': None, 'total_col': None, 'today_report_key': None, 'last_game_key': None},
        # Walks Batting - OPP WALKS BATTING RATE % and TODAY OPPONENT WALKS BATTING are not generated here
        {'col_name': f'inning_{inning_number}_walks_batting', 'report_key_avg': 'OPP WALKS BATTING/GM', 'report_key_rate': None, 'total_col': f'inning_{inning_number}_batters_to_plate', 'today_report_key': None, 'last_game_key': f'LAST GAME OPPONENT WALKS BATTING INNING {inning_number}'},
        # NEW: Venue Bat Walks Batting/GM
        {'col_name': f'inning_{inning_number}_walks_batting', 'report_key_avg': 'VENUE BAT WALKS BATTING/GM', 'report_key_rate': None, 'total_col': None, 'today_report_key': None, 'last_game_key': None},
    ]


# Moved to global scope for accessibility
confidence_map = {"Low": 1, "Moderate": 2, "High": 3, "HIGH OVER": 4} # Added HIGH OVER to map
nrfi_yrfi_map = {"Low": 1, "Moderate (leaning NRFI)": 2.5, "High (NRFI)": 3, "Moderate (leaning YRFI)": 2.5, "High (YRFI)": 3}
over_under_map = {"Low": 1, "Moderate (leaning Under)": 2.5, "High (Under)": 3, "Moderate (leaning Over)": 2.5, "High (Over)": 3}


def get_report_metrics_config_for_inning(inning_number: int):
    """
    Generates the report metrics configuration for a given inning.
    This defines which columns to display, how to highlight, and PDF metadata.
    """
    return [
        {
            'name': 'Strikeouts',
            'filename_suffix': 'strikeouts',
            'title_prefix': 'Strikeouts',
            'pitcher_cols': [
                f'# TOTAL STARTS INNING {inning_number}',
                f'LAST GAME PITCHER STRIKEOUTS INNING {inning_number}', # Metric-specific last game
                'VENUE PITCH K RATE %', # Moved
                'PITCH K OVERALL RATE %', # Renamed
                'PITCHER K BET',
                f'TODAY PITCHER STRIKEOUTS INNING {inning_number}'
            ],
            'opponent_cols': [
                f'LAST GAME OPPONENT STRIKEOUTS BATTING INNING {inning_number}', # Metric-specific last game
                'VENUE OPP K RATE %', # Moved
                'OPP K OVERALL RATE %', # Renamed
                'OPPONENT K BET', # NEW COLUMN
                # Removed: 'OPP K/GM'
            ],
            'overall_conf_col': 'Overall K CONFIDENCE',
            'moved_pitcher_today_col': f'TODAY PITCHER STRIKEOUTS INNING {inning_number}',
            'highlight_cols': [
                {'col_conf': 'Overall K CONFIDENCE', 'is_inverse': False},
                {'col': 'PITCH K OVERALL RATE %', 'type': 'percentage_range_highlight'}, # New highlight type
                {'col': 'OPP K OVERALL RATE %', 'type': 'percentage_range_highlight'}, # New highlight type
                {'col': 'PITCHER K BET', 'type': 'bet_recommendation_k'}, # Changed to k-specific highlight
                {'col': 'OPPONENT K BET', 'type': 'bet_recommendation_k'}, # Changed to k-specific highlight
                {'col': f'TODAY PITCHER STRIKEOUTS INNING {inning_number}', 'type': 'positive_value_highlight'}, # Highlight if > 0
                {'col': f'LAST GAME PITCHER STRIKEOUTS INNING {inning_number}', 'type': 'zero_value_highlight'}, # Highlight if 0
                {'col': f'LAST GAME OPPONENT STRIKEOUTS BATTING INNING {inning_number}', 'type': 'zero_value_highlight'}, # Highlight if 0
                {'col': 'VENUE PITCH K RATE %', 'type': 'percentage_range_highlight'}, # New highlight type
                {'col': 'VENUE OPP K RATE %', 'type': 'percentage_range_highlight'} # New highlight type
            ]
        },
        {
            'name': 'Runs',
            'filename_suffix': 'runs',
            'title_prefix': 'Runs Allowed/Scored',
            'pitcher_cols': [
                f'# TOTAL STARTS INNING {inning_number}',
                f'LAST GAME PITCHER RUNS ALLOWED INNING {inning_number}', # Metric-specific last game
                f'PITCHER VENUE NRFI % INNING {inning_number}', # Existing
                f'PITCH NRFI % INNING {inning_number}', # Renamed and moved
                'PITCHER RUNS BET',
            ],
            'opponent_cols': [
                f'LAST GAME OPPONENT RUNS SCORED INNING {inning_number}', # Metric-specific last game
                f'OPPONENT VENUE NRFI % INNING {inning_number}',
                f'BAT NRFI % INNING {inning_number}', # New column
                'OPPONENT RUNS BET'
            ],
            'overall_conf_col': 'Overall CONFIDENCE FOR NRFI AND YRFI',
            'moved_pitcher_today_col': f'TODAY PITCHER RUNS ALLOWED INNING {inning_number}',
            'highlight_cols': [
                {'col_conf': 'Overall CONFIDENCE FOR NRFI AND YRFI', 'is_inverse': False},
                {'col': f'PITCH NRFI % INNING {inning_number}', 'type': 'nrfi_highlight'}, # Updated name
                {'col': f'PITCHER VENUE NRFI % INNING {inning_number}', 'type': 'nrfi_highlight'},
                {'col': f'OPPONENT VENUE NRFI % INNING {inning_number}', 'type': 'nrfi_highlight'},
                {'col': f'BAT NRFI % INNING {inning_number}', 'type': 'nrfi_highlight'}, # New highlight rule
                {'col': 'PITCHER RUNS BET', 'type': 'bet_recommendation'},
                {'col': 'OPPONENT RUNS BET', 'type': 'bet_recommendation'}, # NEW HIGHLIGHT
                {'col': f'TODAY PITCHER RUNS ALLOWED INNING {inning_number}', 'type': 'today_runs_highlight'},
                {'col': f'LAST GAME PITCHER RUNS ALLOWED INNING {inning_number}', 'type': 'positive_value_highlight'}, # Highlight if > 0
                {'col': f'LAST GAME OPPONENT RUNS SCORED INNING {inning_number}', 'type': 'positive_value_highlight'} # Highlight if > 0
            ]
        },
        {
            'name': 'Hits',
            'filename_suffix': 'hits',
            'title_prefix': 'Hits Allowed/Batting',
            'pitcher_cols': [
                f'# TOTAL STARTS INNING {inning_number}',
                f'LAST GAME PITCHER HITS ALLOWED INNING {inning_number}', # Metric-specific last game
                'PITCHER VENUE NRHI % INNING {inning_number}', # Renamed from VENUE PITCH HIT AVG
                'PITCH NRHI % INNING {inning_number}', # Renamed from PITCH HIT AVG
                'PITCH HIT AVG #', # Keep for now as average hits allowed
                'PITCHER HITS BET',
            ],
            'opponent_cols': [
                f'LAST GAME OPPONENT HITS BATTING INNING {inning_number}', # Metric-specific last game
                'BATTER VENUE NRHI % INNING {inning_number}', # Renamed from VENUE BAT HIT AVG
                'BAT NRHI % INNING {inning_number}', # Renamed from BAT HIT AVG
                'BAT HIT AVG #', # Keep for now as average hits batting
                'OPPONENT HITS BET', # NEW COLUMN
            ],
            'overall_conf_col': 'Overall HITS CONFIDENCE',
            'moved_pitcher_today_col': f'TODAY PITCHER HITS INNING {inning_number}',
            'highlight_cols': [
                {'col_conf': 'Overall HITS CONFIDENCE', 'is_inverse': False},
                {'col': 'PITCHER VENUE NRHI % INNING {inning_number}', 'type': 'nrfi_highlight'}, # Apply NRFI highlight logic
                {'col': 'PITCH NRHI % INNING {inning_number}', 'type': 'nrfi_highlight'}, # Apply NRFI highlight logic
                {'col': 'PITCH HIT AVG #', 'ascending': True}, # Keep for average hits allowed
                {'col': 'BATTER VENUE NRHI % INNING {inning_number}', 'type': 'nrfi_highlight'}, # Apply NRFI highlight logic
                {'col': 'BAT NRHI % INNING {inning_number}', 'type': 'nrfi_highlight'}, # Apply NRFI highlight logic
                {'col': 'PITCHER HITS BET', 'type': 'bet_recommendation'},
                {'col': 'OPPONENT HITS BET', 'type': 'bet_recommendation'}, # NEW HIGHLIGHT
                {'col': f'TODAY PITCHER HITS INNING {inning_number}', 'type': 'today_runs_highlight'}
            ]
        },
        {
            'name': 'Walks',
            'filename_suffix': 'walks',
            'title_prefix': 'Walks Allowed/Batting',
            'pitcher_cols': [
                f'# TOTAL STARTS INNING {inning_number}',
                f'LAST GAME PITCHER WALKS ALLOWED INNING {inning_number}', # Metric-specific last game
                'VENUE PITCH WALKS ALLOWED/GM', # NEW COLUMN, consistent with Hits
                f'PITCH WALKS ALLOWED PER GAME %', # Kept as requested
                'PITCHER WALKS BET',
                f'TODAY PITCHER WALKS INNING {inning_number}'
            ],
            'opponent_cols': [
                f'LAST GAME OPPONENT WALKS BATTING INNING {inning_number}', # Metric-specific last game
                'VENUE BAT WALKS BATTING/GM', # NEW COLUMN, consistent with Hits
                'OPP WALKS BATTING/GM', # Kept, will be multiplied by 100
                'OPPONENT WALKS BET', # NEW COLUMN
            ],
            'overall_conf_col': 'Overall WALKS CONFIDENCE',
            'moved_pitcher_today_col': f'TODAY PITCHER WALKS INNING {inning_number}',
            'highlight_cols': [
                {'col_conf': 'Overall WALKS CONFIDENCE', 'is_inverse': False},
                {'col': 'VENUE PITCH WALKS ALLOWED/GM', 'ascending': True}, # Highlight for new venue column
                {'col': 'PITCH WALKS ALLOWED/GM', 'ascending': True}, # Highlight for new AVG #
                {'col': f'PITCH WALKS ALLOWED PER GAME %', 'ascending': True, 'thresholds': [(8.0, 'green'), (12.0, 'yellow')]}, # Existing highlight
                {'col': 'VENUE BAT WALKS BATTING/GM', 'ascending': False}, # Highlight for new venue column
                {'col': 'OPP WALKS BATTING/GM', 'ascending': False}, # Existing highlight
                {'col': 'PITCHER WALKS BET', 'type': 'bet_recommendation'},
                {'col': 'OPPONENT WALKS BET', 'type': 'bet_recommendation'}, # NEW HIGHLIGHT
                {'col': f'TODAY PITCHER WALKS INNING {inning_number}', 'type': 'today_runs_highlight'}
            ]
        },
        {
            'name': 'Singles',
            'filename_suffix': 'singles',
            'title_prefix': 'Singles Allowed/Batting',
            'pitcher_cols': [
                f'# TOTAL STARTS INNING {inning_number}',
                f'LAST GAME PITCHER SINGLES ALLOWED INNING {inning_number}', # Metric-specific last game
                'VENUE PITCH SINGLES ALLOWED/GM', # NEW COLUMN
                'PITCH SINGLES ALLOWED/GM', # Added to match Hits AVG #
                f'PITCH SINGLES ALLOWED PER GAME %', # Matches Hits AVG
                'PITCHER SINGLES BET',
                f'TODAY PITCHER SINGLES INNING {inning_number}'
            ],
            'opponent_cols': [
                f'LAST GAME OPPONENT SINGLES BATTING INNING {inning_number}', # Metric-specific last game
                'VENUE BAT SINGLES BATTING/GM', # NEW COLUMN
                'OPP SINGLES BATTING/GM', # Will be multiplied by 100
                f'TODAY OPPONENT SINGLES INNING {inning_number}',
                'OPPONENT SINGLES BET', # NEW COLUMN
            ],
            'overall_conf_col': 'Overall SINGLES CONFIDENCE',
            'moved_pitcher_today_col': f'TODAY PITCHER SINGLES INNING {inning_number}',
            'highlight_cols': [
                {'col_conf': 'Overall SINGLES CONFIDENCE', 'is_inverse': False},
                {'col': 'VENUE PITCH SINGLES ALLOWED/GM', 'ascending': True}, # Highlight for new venue column
                {'col': 'PITCH SINGLES ALLOWED/GM', 'ascending': True}, # Highlight for new AVG #
                {'col': f'PITCH SINGLES ALLOWED PER GAME %', 'ascending': True, 'thresholds': [(50.0, 'green'), (75.0, 'yellow')]}, # Existing highlight
                {'col': 'VENUE BAT SINGLES BATTING/GM', 'ascending': False}, # Highlight for new venue column
                {'col': 'OPP SINGLES BATTING/GM', 'ascending': False}, # Existing highlight
                {'col': 'PITCHER SINGLES BET', 'type': 'bet_recommendation'},
                {'col': 'OPPONENT SINGLES BET', 'type': 'bet_recommendation'}, # NEW HIGHLIGHT
                {'col': f'TODAY PITCHER SINGLES INNING {inning_number}', 'type': 'today_runs_highlight'}
            ]
        },
        {
            'name': 'Doubles',
            'filename_suffix': 'doubles',
            'title_prefix': 'Doubles Allowed/Batting',
            'pitcher_cols': [
                f'# TOTAL STARTS INNING {inning_number}',
                f'LAST GAME PITCHER DOUBLES ALLOWED INNING {inning_number}', # Metric-specific last game
                'VENUE PITCH DOUBLES ALLOWED/GM', # NEW COLUMN
                'PITCH DOUBLES ALLOWED/GM', # Added to match Hits AVG #
                f'PITCH DOUBLES ALLOWED PER GAME %', # Matches Hits AVG
                'PITCHER DOUBLES BET',
                f'TODAY PITCHER DOUBLES INNING {inning_number}'
            ],
            'opponent_cols': [
                f'LAST GAME OPPONENT DOUBLES BATTING INNING {inning_number}', # Metric-specific last game
                'VENUE BAT DOUBLES BATTING/GM', # NEW COLUMN
                'OPP DOUBLES BATTING/GM', # Will be multiplied by 100
                f'TODAY OPPONENT DOUBLES INNING {inning_number}',
                'OPPONENT DOUBLES BET', # NEW COLUMN
            ],
            'overall_conf_col': 'Overall DOUBLES CONFIDENCE',
            'moved_pitcher_today_col': f'TODAY PITCHER DOUBLES INNING {inning_number}',
            'highlight_cols': [
                {'col_conf': 'Overall DOUBLES CONFIDENCE', 'is_inverse': False},
                {'col': 'VENUE PITCH DOUBLES ALLOWED/GM', 'ascending': True}, # Highlight for new venue column
                {'col': 'PITCH DOUBLES ALLOWED/GM', 'ascending': True}, # Highlight for new AVG #
                {'col': f'PITCH DOUBLES ALLOWED PER GAME %', 'ascending': True, 'thresholds': [(15.0, 'green'), (30.0, 'yellow')]}, # Existing highlight
                {'col': 'VENUE BAT DOUBLES BATTING/GM', 'ascending': False}, # Highlight for new venue column
                {'col': 'OPP DOUBLES BATTING/GM', 'ascending': False}, # Existing highlight
                {'col': 'PITCHER DOUBLES BET', 'type': 'bet_recommendation'},
                {'col': 'OPPONENT DOUBLES BET', 'type': 'bet_recommendation'}, # NEW HIGHLIGHT
                {'col': f'TODAY PITCHER DOUBLES INNING {inning_number}', 'type': 'today_runs_highlight'}
            ]
        },
        {
            'name': 'Triples',
            'filename_suffix': 'triples',
            'title_prefix': 'Triples Allowed/Batting',
            'pitcher_cols': [
                f'# TOTAL STARTS INNING {inning_number}',
                f'LAST GAME PITCHER TRIPLES ALLOWED INNING {inning_number}', # Metric-specific last game
                'VENUE PITCH TRIPLES ALLOWED/GM', # NEW COLUMN
                'PITCH TRIPLES ALLOWED/GM', # Added to match Hits AVG #
                f'PITCH TRIPLES ALLOWED PER GAME %', # Matches Hits AVG
                'PITCHER TRIPLES BET',
                f'TODAY PITCHER TRIPLES INNING {inning_number}'
            ],
            'opponent_cols': [
                f'LAST GAME OPPONENT TRIPLES BATTING INNING {inning_number}', # Metric-specific last game
                'VENUE BAT TRIPLES BATTING/GM', # NEW COLUMN
                'OPP TRIPLES BATTING/GM', # Will be multiplied by 100
                f'TODAY OPPONENT TRIPLES INNING {inning_number}',
                'OPPONENT TRIPLES BET', # NEW COLUMN
            ],
            'overall_conf_col': 'Overall TRIPLES CONFIDENCE',
            'moved_pitcher_today_col': f'TODAY PITCHER TRIPLES INNING {inning_number}',
            'highlight_cols': [
                {'col_conf': 'Overall TRIPLES CONFIDENCE', 'is_inverse': False},
                {'col': 'VENUE PITCH TRIPLES ALLOWED/GM', 'ascending': True}, # Highlight for new venue column
                {'col': 'PITCH TRIPLES ALLOWED/GM', 'ascending': True}, # Highlight for new AVG #
                {'col': f'PITCH TRIPLES ALLOWED PER GAME %', 'ascending': True, 'thresholds': [(2.0, 'green'), (5.0, 'yellow')]}, # Existing highlight
                {'col': 'VENUE BAT TRIPLES BATTING/GM', 'ascending': False}, # Highlight for new venue column
                {'col': 'PITCHER TRIPLES BET', 'type': 'bet_recommendation'},
                {'col': 'OPPONENT TRIPLES BET', 'type': 'bet_recommendation'}, # NEW HIGHLIGHT
                {'col': f'TODAY PITCHER TRIPLES INNING {inning_number}', 'type': 'today_runs_highlight'}
            ]
        },
        {
            'name': 'Homers',
            'filename_suffix': 'homers',
            'title_prefix': 'Homers Allowed/Batting',
            'pitcher_cols': [
                f'# TOTAL STARTS INNING {inning_number}',
                f'LAST GAME PITCHER HOMERS ALLOWED INNING {inning_number}', # Metric-specific last game
                'VENUE PITCH HOMERS ALLOWED/GM', # NEW COLUMN
                'PITCH HOMERS ALLOWED/GM', # Added to match Hits AVG #
                f'PITCH HOMERS ALLOWED PER GAME %', # Matches Hits AVG
                'PITCHER HOMERS BET',
                f'TODAY PITCHER HOMERS INNING {inning_number}'
            ],
            'opponent_cols': [
                f'LAST GAME OPPONENT HOMERS BATTING INNING {inning_number}', # Metric-specific last game
                'VENUE BAT HOMERS BATTING/GM', # NEW COLUMN
                'OPP HOMERS BATTING/GM', # Will be multiplied by 100
                f'TODAY OPPONENT HOMERS INNING {inning_number}',
                'OPPONENT HOMERS BET', # NEW COLUMN
            ],
            'overall_conf_col': 'Overall HOMERS CONFIDENCE',
            'moved_pitcher_today_col': f'TODAY PITCHER HOMERS INNING {inning_number}',
            'highlight_cols': [
                {'col_conf': 'Overall HOMERS CONFIDENCE', 'is_inverse': False},
                {'col': 'VENUE PITCH HOMERS ALLOWED/GM', 'ascending': True}, # Highlight for new venue column
                {'col': 'PITCH HOMERS ALLOWED/GM', 'ascending': True}, # Highlight for new AVG #
                {'col': f'PITCH HOMERS ALLOWED PER GAME %', 'ascending': True, 'thresholds': [(5.0, 'green'), (10.0, 'yellow')]}, # Existing highlight
                {'col': 'VENUE BAT HOMERS BATTING/GM', 'ascending': False}, # Highlight for new venue column
                {'col': 'PITCHER HOMERS BET', 'type': 'bet_recommendation'},
                {'col': 'OPPONENT HOMERS BET', 'type': 'bet_recommendation'}, # NEW HIGHLIGHT
                {'col': f'TODAY PITCHER HOMERS INNING {inning_number}', 'type': 'today_runs_highlight'}
            ]
        },
        {
            'name': 'Total Bases',
            'filename_suffix': 'total_bases',
            'title_prefix': 'Total Bases Allowed/Batting',
            'pitcher_cols': [
                f'# TOTAL STARTS INNING {inning_number}',
                f'LAST GAME PITCHER TOTAL BASES ALLOWED INNING {inning_number}', # Metric-specific last game
                'VENUE PITCH TOTAL BASES ALLOWED/GM', # NEW COLUMN
                'PITCH TOTAL BASES ALLOWED/GM', # Added to match Hits AVG #
                f'PITCH TOTAL BASES ALLOWED PER GAME %', # Matches Hits AVG
                'PITCHER TOTAL BASES BET',
                f'TODAY PITCHER TOTAL BASES INNING {inning_number}'
            ],
            'opponent_cols': [
                f'LAST GAME OPPONENT TOTAL BASES BATTING INNING {inning_number}', # Metric-specific last game
                'VENUE BAT TOTAL BASES BATTING/GM', # NEW COLUMN
                'OPP TOTAL BASES BATTING/GM', # Will be multiplied by 100
                f'TODAY OPPONENT TOTAL BASES INNING {inning_number}',
                'OPPONENT TOTAL BASES BET', # NEW COLUMN
            ],
            'overall_conf_col': 'Overall TOTAL BASES CONFIDENCE',
            'moved_pitcher_today_col': f'TODAY PITCHER TOTAL BASES INNING {inning_number}',
            'highlight_cols': [
                {'col_conf': 'Overall TOTAL BASES CONFIDENCE', 'is_inverse': False},
                {'col': 'VENUE PITCH TOTAL BASES ALLOWED/GM', 'ascending': True}, # Highlight for new venue column
                {'col': 'PITCH TOTAL BASES ALLOWED/GM', 'ascending': True}, # Highlight for new AVG #
                {'col': f'PITCH TOTAL BASES ALLOWED PER GAME %', 'ascending': True, 'thresholds': [(125.0, 'green'), (175.0, 'yellow')]}, # Existing highlight
                {'col': 'VENUE BAT TOTAL BASES BATTING/GM', 'ascending': False}, # Highlight for new venue column
                {'col': 'PITCHER TOTAL BASES BET', 'type': 'bet_recommendation'},
                {'col': 'OPPONENT TOTAL BASES BET', 'type': 'bet_recommendation'}, # NEW HIGHLIGHT
                {'col': f'TODAY PITCHER TOTAL BASES INNING {inning_number}', 'type': 'today_runs_highlight'}
            ]
        },
    ]


def generate_ranked_parlays(report_df):
    """
    Generates ranked parlays based on the overall confidence scores.
    This is a placeholder and can be expanded for more sophisticated parlay logic.
    """
    parlays = {
        "Strikeout Parlays": [],
        "NRFI Parlays": [],
        "YRFI Parlays": [],
        "Hits Under Parlays": [],
        "Hits Over Parlays": [],
        "Walks Under Parlays": [],
        "Walks Over Parlays": [],
        "Singles Under Parlays": [],
        "Singles Over Parlays": [],
        "Doubles Under Parlays": [],
        "Doubles Over Parlays": [],
        "Triples Under Parlays": [],
        "Triples Over Parlays": [],
        "Homers Under Parlays": [],
        "Homers Over Parlays": [],
        "Total Bases Under Parlays": [],
        "Total Bases Over Parlays": [],
    }


    # Example: Strikeout Parlays (combining 2 games with High K Confidence)
    high_k_games = report_df[report_df['Overall K CONFIDENCE'] == 'High']
    if len(high_k_games) >= 2:
        for combo in combinations(high_k_games.to_dict('records'), 2):
            game1 = combo[0]
            game2 = combo[1]
            parlay_str = f"{game1['Game']} ({game1['Pitcher']}) & {game2['Game']} ({game2['Pitcher']})"
            # Simple scoring for now, can be improved
            score = (confidence_map.get(game1['Overall K CONFIDENCE'], 0) + confidence_map.get(game2['Overall K CONFIDENCE'], 0)) / 2
            parlays["Strikeout Parlays"].append({"games": parlay_str, "score": score})
        parlays["Strikeout Parlays"].sort(key=lambda x: x['score'], reverse=True)


    # Example: NRFI Parlays
    high_nrfi_games = report_df[report_df['Overall CONFIDENCE FOR NRFI AND YRFI'] == 'High (NRFI)']
    if len(high_nrfi_games) >= 2:
        for combo in combinations(high_nrfi_games.to_dict('records'), 2):
            game1 = combo[0]
            game2 = combo[1]
            parlay_str = f"{game1['Game']} (NRFI) & {game2['Game']} (NRFI)"
            score = (nrfi_yrfi_map.get(game1['Overall CONFIDENCE FOR NRFI AND YRFI'], 0) + nrfi_yrfi_map.get(game2['Overall CONFIDENCE FOR NRFI AND YRFI'], 0)) / 2
            parlays["NRFI Parlays"].append({"games": parlay_str, "score": score})
        parlays["NRFI Parlays"].sort(key=lambda x: x['score'], reverse=True)


    # Example: YRFI Parlays
    high_yrfi_games = report_df[report_df['Overall CONFIDENCE FOR NRFI AND YRFI'].isin(['High (YRFI)', 'Moderate (leaning YRFI)'])]
    if len(high_yrfi_games) >= 2:
        for combo in combinations(high_yrfi_games.to_dict('records'), 2):
            game1 = combo[0]
            game2 = combo[1]
            parlay_str = f"{game1['Game']} (YRFI) & {game2['Game']} (YRFI)"
            score = (nrfi_yrfi_map.get(game1['Overall CONFIDENCE FOR NRFI AND YRFI'], 0) + nrfi_yrfi_map.get(game2['Overall CONFIDENCE FOR NRFI AND YRFI'], 0)) / 2
            parlays["YRFI Parlays"].append({"games": parlay_str, "score": score})
        parlays["YRFI Parlays"].sort(key=lambda x: x['score'], reverse=True)


    # Add similar logic for other metrics (Walks, Singles, Doubles, Triples, Homers, Total Bases)
    # This will require iterating through the `over_under_metrics_config` and applying similar logic.
    over_under_metrics_config = [
        {'name': 'Hits', 'overall_conf_col': 'Overall HITS CONFIDENCE'}, # Added Hits
        {'name': 'Walks', 'overall_conf_col': 'Overall WALKS CONFIDENCE'},
        {'name': 'Singles', 'overall_conf_col': 'Overall SINGLES CONFIDENCE'},
        {'name': 'Doubles', 'overall_conf_col': 'Overall DOUBLES CONFIDENCE'},
        {'name': 'Triples', 'overall_conf_col': 'Overall TRIPLES CONFIDENCE'},
        {'name': 'Homers', 'overall_conf_col': 'Overall HOMERS CONFIDENCE'},
        {'name': 'Total Bases', 'overall_conf_col': 'Overall TOTAL BASES CONFIDENCE'},
    ]


    for metric_info in over_under_metrics_config:
        metric_name = metric_info['name']
        overall_col = metric_info['overall_conf_col']


        # Under Parlays
        high_under_games = report_df[report_df[overall_col].isin(['High (Under)'])]
        if len(high_under_games) >= 2:
            for combo in combinations(high_under_games.to_dict('records'), 2):
                game1 = combo[0]
                game2 = combo[1]
                parlay_str = f"{game1['Game']} ({metric_name} Under) & {game2['Game']} ({metric_name} Under)"
                score = (over_under_map.get(game1[overall_col], 0) + over_under_map.get(game2[overall_col], 0)) / 2
                parlays[f"{metric_name} Under Parlays"].append({"games": parlay_str, "score": score})
            parlays[f"{metric_name} Under Parlays"].sort(key=lambda x: x['score'], reverse=True)


        # Over Parlays
        high_over_games = report_df[report_df[overall_col].isin(['High (Over)'])]
        if len(high_over_games) >= 2:
            for combo in combinations(high_over_games.to_dict('records'), 2):
                game1 = combo[0]
                game2 = combo[1]
                parlay_str = f"{game1['Game']} ({metric_name} Over) & {game2['Game']} ({metric_name} Over)"
                score = (over_under_map.get(game1[overall_col], 0) + over_under_map.get(game2[overall_col], 0)) / 2
                parlays[f"{metric_name} Over Parlays"].append({"games": parlay_str, "score": score})
            parlays[f"{metric_name} Over Parlays"].sort(key=lambda x: x['score'], reverse=True)


    return parlays


    
def get_inning_pitcher_columns(inning_number):
    return [
        'date', 'game_id', 'pitcher_id', 'pitcher_name', 'team_id', 'opponent_team_id',
        'is_home_pitcher', f'inning_{inning_number}_strikeouts', f'inning_{inning_number}_runs_allowed',
        f'inning_{inning_number}_batters_faced', f'inning_{inning_number}_hits_allowed',
        f'inning_{inning_number}_singles_allowed', f'inning_{inning_number}_doubles_allowed',
        f'inning_{inning_number}_triples_allowed', f'inning_{inning_number}_homers_allowed',
        f'inning_{inning_number}_total_bases_allowed', f'inning_{inning_number}_walks_allowed'
    ]

def get_inning_batting_columns(inning_number):
    return [
        'date', 'game_id', 'team_id', 'team_name', 'opponent_team_id', 'opponent_team_name',
        'is_home_team', f'inning_{inning_number}_runs_scored', f'inning_{inning_number}_strikeouts_batting',
        f'inning_{inning_number}_batters_to_plate', f'inning_{inning_number}_hits_batting',
        f'inning_{inning_number}_singles_batting', f'inning_{inning_number}_doubles_batting',
        f'inning_{inning_number}_triples_batting', f'inning_{inning_number}_homers_batting',
        f'inning_{inning_number}_total_bases_batting', f'inning_{inning_number}_walks_batting',
        f'inning_{inning_number}_at_bats_total_batting'
    ]

def get_inning_individual_batter_columns(inning_number):
    return [
        'date', 'game_id', 'batter_id', 'batter_name', 'team_id', 'opponent_team_id',
        'inning_number', f'inning_{inning_number}_runs_scored_batter', f'inning_{inning_number}_strikeouts_batter',
        f'inning_{inning_number}_hits_batter', f'inning_{inning_number}_singles_batter',
        f'inning_{inning_number}_doubles_batter', f'inning_{inning_number}_triples_batter',
        f'inning_{inning_number}_homers_batter', f'inning_{inning_number}_total_bases_batter',
        f'inning_{inning_number}_walks_batter', f'inning_{inning_number}_at_bats_batter'
    ]

def get_inning_team_pitching_columns(inning_number):
    return [
        'date', 'game_id', 'team_id', 'opponent_team_id', 'inning_number',
        f'inning_{inning_number}_team_strikeouts_pitching', f'inning_{inning_number}_team_runs_allowed_pitching',
        f'inning_{inning_number}_team_hits_allowed_pitching', f'inning_{inning_number}_team_walks_allowed_pitching',
        f'inning_{inning_number}_team_batters_faced_pitching'
    ]
# --- End: Helper functions for expected DataFrame columns ---


def fetch_and_process_inning_data(inning_number: int, start_date: str, end_date: str, raw_data_dir: str):
    """
    Fetches Statcast data for a specific inning, processes it for pitching and batting metrics,
    and saves daily CSVs to date-specific subdirectories within raw_data_dir.
    """
    # raw_data_dir is now the base for the inning, e.g., 'cache/raw_data_2025_by_inning/inning_1'
    os.makedirs(raw_data_dir, exist_ok=True) 
    logging.info(f"Fetching Statcast data for Inning {inning_number} from {start_date} to {end_date}...")
    print(f"INFO: Fetching Statcast data for Inning {inning_number} from {start_date} to {end_date}...")


    try:
        current_day = datetime.strptime(start_date, '%Y-%m-%d')
        end_day = datetime.strptime(end_date, '%Y-%m-%d')


        while current_day <= end_day:
            date_str = current_day.strftime('%Y-%m-%d')
            logging.info(f"Processing date: {date_str} for Inning {inning_number}")
            print(f"INFO: Processing date: {date_str} for Inning {inning_number}")

            # Create date-specific subdirectory
            daily_inning_dir = os.path.join(raw_data_dir, date_str)
            os.makedirs(daily_inning_dir, exist_ok=True)

            pitcher_output_file = os.path.join(daily_inning_dir, f'inning_{inning_number}_pitcher_data.csv')
            batting_team_output_file = os.path.join(daily_inning_dir, f'inning_{inning_number}_batting_data.csv')
            individual_batter_output_file = os.path.join(daily_inning_dir, f'inning_{inning_number}_individual_batter_data.csv')
            team_pitching_output_file = os.path.join(daily_inning_dir, f'inning_{inning_number}_team_pitching_data.csv')


            # Basic check for existing files. For more robust re-fetching, this could be enhanced
            # to check if files are empty or missing critical columns, similar to full_game_data.
            if (os.path.exists(pitcher_output_file) and
                os.path.exists(batting_team_output_file) and
                os.path.exists(individual_batter_output_file) and
                os.path.exists(team_pitching_output_file)):
                # Add a check to see if files are empty, if so, re-process
                all_exist_and_not_empty = True
                for f_path in [pitcher_output_file, batting_team_output_file, individual_batter_output_file, team_pitching_output_file]:
                    if os.path.getsize(f_path) < 50: # Assuming CSV header makes it at least a few bytes
                        all_exist_and_not_empty = False
                        logging.info(f"File {f_path} for {date_str}, Inning {inning_number} is empty or too small. Re-processing.")
                        print(f"INFO: File {f_path} for {date_str}, Inning {inning_number} is empty or too small. Re-processing.")
                        break
                if all_exist_and_not_empty:
                    logging.info(f"All data for {date_str} in Inning {inning_number} already exists and seems non-empty. Skipping.")
                    print(f"INFO: All data for {date_str} in Inning {inning_number} already exists and seems non-empty. Skipping.")
                    current_day += timedelta(days=1)
                    continue


            statcast_data = None
            try:
                statcast_data = pyb.statcast(start_dt=date_str, end_dt=date_str)
                if statcast_data is None or statcast_data.empty:
                    logging.warning(f"No Statcast data found for {date_str}.")
                    print(f"WARNING: No Statcast data found for {date_str}.")
                    # Create empty files to prevent FileNotFoundError later if no data for the day
                    # Ensure empty files have correct headers
                    pd.DataFrame(columns=get_inning_pitcher_columns(inning_number)).to_csv(pitcher_output_file, index=False)
                    pd.DataFrame(columns=get_inning_batting_columns(inning_number)).to_csv(batting_team_output_file, index=False)
                    pd.DataFrame(columns=get_inning_individual_batter_columns(inning_number)).to_csv(individual_batter_output_file, index=False)
                    pd.DataFrame(columns=get_inning_team_pitching_columns(inning_number)).to_csv(team_pitching_output_file, index=False)
                    current_day += timedelta(days=1)
                    continue
            except Exception as e:
                logging.error(f"Error fetching Statcast data for {date_str}: {e}")
                print(f"ERROR: Error fetching Statcast data for {date_str}: {e}")
                current_day += timedelta(days=1)
                continue

            # Check for critical columns and ensure they are string type
            required_cols = ['game_pk', 'pitcher', 'batter', 'home_team', 'away_team', 'events', 'inning_topbot']
            missing_col_found = False
            for col in required_cols:
                if col not in statcast_data.columns:
                    logging.error(f"Missing required column '{col}' in Statcast data for {date_str}. Skipping date.")
                    print(f"ERROR: Missing required column '{col}' in Statcast data for {date_str}. Skipping date.")
                    missing_col_found = True
                    break
            if missing_col_found:
                current_day += timedelta(days=1)
                continue
            
            for col in required_cols: # Ensure type after confirming existence
                 statcast_data[col] = statcast_data[col].astype(str)


            # Defensive check and formatting for player_name and bat_play_name
            if 'player_name' in statcast_data.columns:
                statcast_data['player_name'] = statcast_data['player_name'].apply(format_name_last_first)
            else:
                logging.warning(f"Column 'player_name' not found in Statcast data for {date_str}. Pitcher names will be looked up by ID.")
                print(f"WARNING: Column 'player_name' not found in Statcast data for {date_str}. Pitcher names will be looked up by ID.")
                statcast_data['player_name'] = statcast_data['pitcher'].astype(str).apply(get_player_name_from_id)

            if 'bat_play_name' in statcast_data.columns:
                statcast_data['bat_play_name'] = statcast_data['bat_play_name'].apply(format_name_last_first)
            else:
                logging.warning(f"Column 'bat_play_name' not found in Statcast data for {date_str}. Batter names will be looked up by ID.")
                print(f"WARNING: Column 'bat_play_name' not found in Statcast data for {date_str}. Batter names will be looked up by ID.")
                statcast_data['bat_play_name'] = statcast_data['batter'].astype(str).apply(get_player_name_from_id)

            # *** MODIFICATION START: Robust inning filtering ***
            if 'inning' not in statcast_data.columns:
                logging.error(f"Critical: 'inning' column missing from Statcast data for {date_str} in Inning {inning_number} processing. Skipping day.")
                print(f"ERROR: Critical: 'inning' column missing from Statcast data for {date_str} in Inning {inning_number} processing. Skipping day.")
                current_day += timedelta(days=1)
                continue

            # DEBUG PRINT: Before converting 'inning' to numeric
            print(f"DEBUG: Before converting 'inning' to numeric for {date_str}, Inning {inning_number}:")
            print(f"  'inning' column dtypes: {statcast_data['inning'].dtype}")
            print(f"  Unique 'inning' values: {statcast_data['inning'].unique()}")

            statcast_data['inning'] = pd.to_numeric(statcast_data['inning'], errors='coerce')
            
            unique_innings_in_data_str = 'N/A'
            if not statcast_data.empty and 'inning' in statcast_data.columns and not statcast_data['inning'].isnull().all():
                unique_innings_in_data = statcast_data['inning'].dropna().unique()
                unique_innings_in_data_str = str(sorted(list(unique_innings_in_data)))
                logging.debug(f"Statcast data for {date_str} contains innings: {unique_innings_in_data_str} (type: {statcast_data['inning'].dtype}) before filtering for inning {inning_number}.")
                print(f"DEBUG: Statcast data for {date_str} contains innings: {unique_innings_in_data_str} (type: {statcast_data['inning'].dtype}) after to_numeric, before filtering for inning {inning_number}.")
            else:
                logging.debug(f"Statcast data for {date_str} is empty or 'inning' column is all NaN/missing after to_numeric, before filtering for inning {inning_number}.")
                print(f"DEBUG: Statcast data for {date_str} is empty or 'inning' column is all NaN/missing after to_numeric, before filtering for inning {inning_number}.")

            # Filter for the specific inning
            inning_data = statcast_data[statcast_data['inning'] == float(inning_number)].copy() # Added .copy()

            # DEBUG PRINT: After filtering for inning
            print(f"DEBUG: After filtering for Inning {inning_number} on {date_str}:")
            print(f"  inning_data shape: {inning_data.shape}")
            if not inning_data.empty:
                print(f"  inning_data head:\n{inning_data.head()}")
            else:
                print(f"  inning_data is empty.")
            # *** MODIFICATION END ***

            if inning_data.empty:
                logging.info(f"No data found for Inning {inning_number} specifically on {date_str} after filtering. Original data had innings: {unique_innings_in_data_str}.")
                print(f"INFO: No data found for Inning {inning_number} specifically on {date_str} after filtering. Original data had innings: {unique_innings_in_data_str}.")
                # Create empty files if no data for this specific inning on this day
                pd.DataFrame(columns=get_inning_pitcher_columns(inning_number)).to_csv(pitcher_output_file, index=False)
                pd.DataFrame(columns=get_inning_batting_columns(inning_number)).to_csv(batting_team_output_file, index=False)
                pd.DataFrame(columns=get_inning_individual_batter_columns(inning_number)).to_csv(individual_batter_output_file, index=False)
                pd.DataFrame(columns=get_inning_team_pitching_columns(inning_number)).to_csv(team_pitching_output_file, index=False)
                current_day += timedelta(days=1)
                continue
            else:
                logging.info(f"Successfully filtered {len(inning_data)} rows for Inning {inning_number} on {date_str}.")
                print(f"INFO: Successfully filtered {len(inning_data)} rows for Inning {inning_number} on {date_str}.")


            pitcher_records = []
            batting_team_records = []
            individual_batter_records = [] 
            team_pitching_records = [] 


            unique_games = inning_data['game_pk'].unique()


            for game_pk in unique_games:
                game_inning_data = inning_data[inning_data['game_pk'] == game_pk].sort_values(by='at_bat_number', ascending=True).copy() # Added .copy()
                if game_inning_data.empty or 'game_date' not in game_inning_data.columns or game_inning_data['game_date'].isnull().all():
                    logging.warning(f"Skipping game_pk {game_pk} for inning {inning_number} on {date_str} due to missing game_date or empty game_inning_data.")
                    continue

                game_date_series = pd.to_datetime(game_inning_data['game_date'], errors='coerce').dropna()
                if game_date_series.empty:
                    logging.warning(f"Skipping game_pk {game_pk} for inning {inning_number} on {date_str} as game_date could not be parsed.")
                    continue
                game_date = game_date_series.iloc[0].strftime('%Y-%m-%d')
                
                home_team_abbr = game_inning_data['home_team'].iloc[0]
                away_team_abbr = game_inning_data['away_team'].iloc[0]

                # DEBUG PRINT: Game and Team info
                print(f"DEBUG: Processing Game {game_pk} ({away_team_abbr} @ {home_team_abbr}) for Inning {inning_number} on {date_str}")


                # --- Process Top of Inning (Away Team Batting, Home Team Pitching) ---
                top_inning_data = game_inning_data[game_inning_data['inning_topbot'] == 'Top'].copy() # Added .copy()
                if not top_inning_data.empty:
                    pitcher_id_top = top_inning_data['pitcher'].iloc[0]
                    # Use the 'player_name' column which is now populated by format_name_last_first or get_player_name_from_id
                    pitcher_name_series = top_inning_data[top_inning_data['pitcher'] == pitcher_id_top]['player_name']
                    pitcher_name_top = pitcher_name_series.iloc[0] if not pitcher_name_series.empty else get_player_name_from_id(pitcher_id_top)
                    
                    strikes_top = top_inning_data[top_inning_data['events'] == 'strikeout']['pitcher'].value_counts().get(pitcher_id_top, 0)
                    
                    # Ensure bat_score and post_bat_score are numeric
                    top_inning_data['bat_score'] = pd.to_numeric(top_inning_data['bat_score'], errors='coerce').fillna(0)
                    top_inning_data['post_bat_score'] = pd.to_numeric(top_inning_data['post_bat_score'], errors='coerce').fillna(0)
                    runs_allowed_top = top_inning_data['post_bat_score'].max() - top_inning_data['bat_score'].min()

                    batters_faced_top_pitcher = top_inning_data['batter'].nunique()


                    pitcher_hits_allowed_top = top_inning_data[top_inning_data['events'].isin(['single', 'double', 'triple', 'home_run'])]['pitcher'].value_counts().get(pitcher_id_top, 0)
                    pitcher_singles_allowed_top = top_inning_data[top_inning_data['events'] == 'single']['pitcher'].value_counts().get(pitcher_id_top, 0)
                    pitcher_doubles_allowed_top = top_inning_data[top_inning_data['events'] == 'double']['pitcher'].value_counts().get(pitcher_id_top, 0)
                    pitcher_triples_allowed_top = top_inning_data[top_inning_data['events'] == 'triple']['pitcher'].value_counts().get(pitcher_id_top, 0)
                    pitcher_homers_allowed_top = top_inning_data[top_inning_data['events'] == 'home_run']['pitcher'].value_counts().get(pitcher_id_top, 0)
                    pitcher_walks_allowed_top = top_inning_data[top_inning_data['events'] == 'walk']['pitcher'].value_counts().get(pitcher_id_top, 0)


                    pitcher_total_bases_allowed_top = (
                        (pitcher_singles_allowed_top * 1) +
                        (pitcher_doubles_allowed_top * 2) +
                        (pitcher_triples_allowed_top * 3) +
                        (pitcher_homers_allowed_top * 4)
                    )


                    pitcher_records.append({
                        'date': game_date,
                        'game_id': game_pk,
                        'pitcher_id': pitcher_id_top,
                        'pitcher_name': pitcher_name_top,
                        'team_id': home_team_abbr, 
                        'opponent_team_id': away_team_abbr, 
                        'is_home_pitcher': True,
                        f'inning_{inning_number}_strikeouts': strikes_top,
                        f'inning_{inning_number}_runs_allowed': runs_allowed_top,
                        f'inning_{inning_number}_batters_faced': batters_faced_top_pitcher,
                        f'inning_{inning_number}_hits_allowed': pitcher_hits_allowed_top,
                        f'inning_{inning_number}_singles_allowed': pitcher_singles_allowed_top,
                        f'inning_{inning_number}_doubles_allowed': pitcher_doubles_allowed_top,
                        f'inning_{inning_number}_triples_allowed': pitcher_triples_allowed_top,
                        f'inning_{inning_number}_homers_allowed': pitcher_homers_allowed_top,
                        f'inning_{inning_number}_total_bases_allowed': pitcher_total_bases_allowed_top,
                        f'inning_{inning_number}_walks_allowed': pitcher_walks_allowed_top
                    })
                    print(f"DEBUG: Added pitcher record for {pitcher_name_top} (Top Inning).")


                    batting_hits_away = top_inning_data[top_inning_data['events'].isin(['single', 'double', 'triple', 'home_run'])].shape[0]
                    batting_singles_away = top_inning_data[top_inning_data['events'] == 'single'].shape[0]
                    batting_doubles_away = top_inning_data[top_inning_data['events'] == 'double'].shape[0]
                    batting_triples_away = top_inning_data[top_inning_data['events'] == 'triple'].shape[0]
                    batting_homers_away = top_inning_data[top_inning_data['events'] == 'home_run'].shape[0]
                    batting_walks_away = top_inning_data[top_inning_data['events'] == 'walk'].shape[0]
                    batting_strikeouts_away = top_inning_data[top_inning_data['events'] == 'strikeout'].shape[0]
                    batters_to_plate_away = top_inning_data['batter'].nunique()
                    runs_scored_away_team = top_inning_data['post_bat_score'].max() - top_inning_data['bat_score'].min()

                    # Calculate at_bats_total_batting for the batting team
                    at_bats_total_batting_away = top_inning_data['at_bat_number'].nunique() # Count unique at_bat_numbers for the team

                    batting_total_bases_away = (
                        (batting_singles_away * 1) +
                        (batting_doubles_away * 2) +
                        (batting_triples_away * 3) +
                        (batting_homers_away * 4)
                    )


                    batting_team_records.append({
                        'date': game_date,
                        'game_id': game_pk,
                        'team_id': away_team_abbr,
                        'team_name': away_team_abbr,
                        'opponent_team_id': home_team_abbr,
                        'opponent_team_name': home_team_abbr,
                        'is_home_team': False,
                        f'inning_{inning_number}_runs_scored': runs_scored_away_team,
                        f'inning_{inning_number}_strikeouts_batting': batting_strikeouts_away,
                        f'inning_{inning_number}_batters_to_plate': batters_to_plate_away,
                        f'inning_{inning_number}_hits_batting': batting_hits_away,
                        f'inning_{inning_number}_singles_batting': batting_singles_away,
                        f'inning_{inning_number}_doubles_batting': batting_doubles_away,
                        f'inning_{inning_number}_triples_batting': batting_triples_away,
                        f'inning_{inning_number}_homers_batting': batting_homers_away,
                        f'inning_{inning_number}_total_bases_batting': batting_total_bases_away,
                        f'inning_{inning_number}_walks_batting': batting_walks_away,
                        f'inning_{inning_number}_at_bats_total_batting': at_bats_total_batting_away # Added this line
                    })
                    print(f"DEBUG: Added batting team record for {away_team_abbr} (Top Inning).")


                    for batter_id in top_inning_data['batter'].unique():
                        batter_data = top_inning_data[top_inning_data['batter'] == batter_id].copy() # Added .copy()
                        batter_name_series = batter_data['bat_play_name']
                        batter_name = batter_name_series.iloc[0] if not batter_name_series.empty else get_player_name_from_id(batter_id)

                        batter_data['bat_score'] = pd.to_numeric(batter_data['bat_score'], errors='coerce').fillna(0)
                        batter_data['post_bat_score'] = pd.to_numeric(batter_data['post_bat_score'], errors='coerce').fillna(0)
                        batter_runs_scored = batter_data['post_bat_score'].max() - batter_data['bat_score'].min()
                        
                        batter_strikeouts = batter_data[batter_data['events'] == 'strikeout'].shape[0]
                        batter_hits = batter_data[batter_data['events'].isin(['single', 'double', 'triple', 'home_run'])].shape[0]
                        batter_singles = batter_data[batter_data['events'] == 'single'].shape[0]
                        batter_doubles = batter_data[batter_data['events'] == 'double'].shape[0]
                        batter_triples = batter_data[batter_data['events'] == 'triple'].shape[0]
                        batter_homers = batter_data[batter_data['events'] == 'home_run'].shape[0]
                        batter_walks = batter_data[batter_data['events'] == 'walk'].shape[0]
                        batter_total_bases = (batter_singles * 1) + (batter_doubles * 2) + (batter_triples * 3) + (batter_homers * 4)


                        individual_batter_records.append({
                            'date': game_date,
                            'game_id': game_pk,
                            'batter_id': batter_id,
                            'batter_name': batter_name,
                            'team_id': away_team_abbr,
                            'opponent_team_id': home_team_abbr,
                            'inning_number': inning_number,
                            f'inning_{inning_number}_runs_scored_batter': batter_runs_scored,
                            f'inning_{inning_number}_strikeouts_batter': batter_strikeouts,
                            f'inning_{inning_number}_hits_batter': batter_hits,
                            f'inning_{inning_number}_singles_batter': batter_singles,
                            f'inning_{inning_number}_doubles_batter': batter_doubles,
                            f'inning_{inning_number}_triples_batter': batter_triples,
                            f'inning_{inning_number}_homers_batter': batter_homers,
                            f'inning_{inning_number}_total_bases_batter': batter_total_bases,
                            f'inning_{inning_number}_walks_batter': batter_walks,
                            f'inning_{inning_number}_at_bats_batter': batter_data['at_bat_number'].nunique() 
                        })
                        print(f"DEBUG: Added individual batter record for {batter_name} (Bot Inning).")


                    team_pitching_strikeouts_home = top_inning_data[top_inning_data['events'] == 'strikeout']['pitcher'].nunique() 
                    team_pitching_runs_allowed_home = runs_allowed_top 
                    team_pitching_hits_allowed_home = top_inning_data[top_inning_data['events'].isin(['single', 'double', 'triple', 'home_run'])].shape[0]
                    team_pitching_walks_allowed_home = top_inning_data[top_inning_data['events'] == 'walk'].shape[0]
                    team_batters_faced_home = batters_faced_top_pitcher 


                    team_pitching_records.append({
                        'date': game_date,
                        'game_id': game_pk,
                        'team_id': home_team_abbr, 
                        'opponent_team_id': away_team_abbr, 
                        'inning_number': inning_number,
                        f'inning_{inning_number}_team_strikeouts_pitching': team_pitching_strikeouts_home,
                        f'inning_{inning_number}_team_runs_allowed_pitching': team_pitching_runs_allowed_home,
                        f'inning_{inning_number}_team_hits_allowed_pitching': team_pitching_hits_allowed_home,
                        f'inning_{inning_number}_team_walks_allowed_pitching': team_pitching_walks_allowed_home,
                        f'inning_{inning_number}_team_batters_faced_pitching': team_batters_faced_home
                    })
                    print(f"DEBUG: Added team pitching record for {home_team_abbr} (Top Inning).")


                bot_inning_data = game_inning_data[game_inning_data['inning_topbot'] == 'Bot'].copy() # Added .copy()
                if not bot_inning_data.empty:
                    pitcher_id_bot = bot_inning_data['pitcher'].iloc[0]
                    # Use the 'player_name' column which is now populated by format_name_last_first or get_player_name_from_id
                    pitcher_name_series_bot = bot_inning_data[bot_inning_data['pitcher'] == pitcher_id_bot]['player_name']
                    pitcher_name_bot = pitcher_name_series_bot.iloc[0] if not pitcher_name_series_bot.empty else get_player_name_from_id(pitcher_id_bot)
                    
                    strikes_bot = bot_inning_data[bot_inning_data['events'] == 'strikeout']['pitcher'].value_counts().get(pitcher_id_bot, 0)

                    bot_inning_data['bat_score'] = pd.to_numeric(bot_inning_data['bat_score'], errors='coerce').fillna(0)
                    bot_inning_data['post_bat_score'] = pd.to_numeric(bot_inning_data['post_bat_score'], errors='coerce').fillna(0)
                    runs_allowed_bot = bot_inning_data['post_bat_score'].max() - bot_inning_data['bat_score'].min()
                    
                    batters_faced_bot_pitcher = bot_inning_data['batter'].nunique()


                    pitcher_hits_allowed_bot = bot_inning_data[bot_inning_data['events'].isin(['single', 'double', 'triple', 'home_run'])]['pitcher'].value_counts().get(pitcher_id_bot, 0)
                    pitcher_singles_allowed_bot = bot_inning_data[bot_inning_data['events'] == 'single']['pitcher'].value_counts().get(pitcher_id_bot, 0)
                    pitcher_doubles_allowed_bot = bot_inning_data[bot_inning_data['events'] == 'double']['pitcher'].value_counts().get(pitcher_id_bot, 0)
                    pitcher_triples_allowed_bot = bot_inning_data[bot_inning_data['events'] == 'triple']['pitcher'].value_counts().get(pitcher_id_bot, 0)
                    pitcher_homers_allowed_bot = bot_inning_data[bot_inning_data['events'] == 'home_run']['pitcher'].value_counts().get(pitcher_id_bot, 0)
                    pitcher_walks_allowed_bot = bot_inning_data[bot_inning_data['events'] == 'walk']['pitcher'].value_counts().get(pitcher_id_bot, 0)


                    pitcher_total_bases_allowed_bot = (
                        (pitcher_singles_allowed_bot * 1) +
                        (pitcher_doubles_allowed_bot * 2) +
                        (pitcher_triples_allowed_bot * 3) +
                        (pitcher_homers_allowed_bot * 4)
                    )


                    pitcher_records.append({
                        'date': game_date,
                        'game_id': game_pk,
                        'pitcher_id': pitcher_id_bot,
                        'pitcher_name': pitcher_name_bot,
                        'team_id': away_team_abbr, 
                        'opponent_team_id': home_team_abbr, 
                        'is_home_pitcher': False,
                        f'inning_{inning_number}_strikeouts': strikes_bot,
                        f'inning_{inning_number}_runs_allowed': runs_allowed_bot,
                        f'inning_{inning_number}_batters_faced': batters_faced_bot_pitcher,
                        f'inning_{inning_number}_hits_allowed': pitcher_hits_allowed_bot,
                        f'inning_{inning_number}_singles_allowed': pitcher_singles_allowed_bot,
                        f'inning_{inning_number}_doubles_allowed': pitcher_doubles_allowed_bot,
                        f'inning_{inning_number}_triples_allowed': pitcher_triples_allowed_bot,
                        f'inning_{inning_number}_homers_allowed': pitcher_homers_allowed_bot,
                        f'inning_{inning_number}_total_bases_allowed': pitcher_total_bases_allowed_bot,
                        f'inning_{inning_number}_walks_allowed': pitcher_walks_allowed_bot
                    })
                    print(f"DEBUG: Added pitcher record for {pitcher_name_bot} (Bot Inning).")


                    batting_hits_home = bot_inning_data[bot_inning_data['events'].isin(['single', 'double', 'triple', 'home_run'])].shape[0]
                    batting_singles_home = bot_inning_data[bot_inning_data['events'] == 'single'].shape[0]
                    batting_doubles_home = bot_inning_data[bot_inning_data['events'] == 'double'].shape[0]
                    batting_triples_home = bot_inning_data[bot_inning_data['events'] == 'triple'].shape[0]
                    batting_homers_home = bot_inning_data[bot_inning_data['events'] == 'home_run'].shape[0]
                    batting_walks_home = bot_inning_data[bot_inning_data['events'] == 'walk'].shape[0]
                    batting_strikeouts_home = bot_inning_data[bot_inning_data['events'] == 'strikeout'].shape[0]
                    batters_to_plate_home = bot_inning_data['batter'].nunique()
                    runs_scored_home_team = bot_inning_data['post_bat_score'].max() - bot_inning_data['bat_score'].min()

                    # Calculate at_bats_total_batting for the batting team
                    at_bats_total_batting_home = bot_inning_data['at_bat_number'].nunique() # Count unique at_bat_numbers for the team

                    batting_total_bases_home = (
                        (batting_singles_home * 1) +
                        (batting_doubles_home * 2) +
                        (batting_triples_home * 3) +
                        (batting_homers_home * 4)
                    )


                    batting_team_records.append({
                        'date': game_date,
                        'game_id': game_pk,
                        'team_id': home_team_abbr,
                        'team_name': home_team_abbr,
                        'opponent_team_id': away_team_abbr,
                        'opponent_team_name': away_team_abbr,
                        'is_home_team': True,
                        f'inning_{inning_number}_runs_scored': runs_scored_home_team,
                        f'inning_{inning_number}_strikeouts_batting': batting_strikeouts_home,
                        f'inning_{inning_number}_batters_to_plate': batters_to_plate_home,
                        f'inning_{inning_number}_hits_batting': batting_hits_home,
                        f'inning_{inning_number}_singles_batting': batting_singles_home,
                        f'inning_{inning_number}_doubles_batting': batting_doubles_home,
                        f'inning_{inning_number}_triples_batting': batting_triples_home,
                        f'inning_{inning_number}_homers_batting': batting_homers_home,
                        f'inning_{inning_number}_total_bases_batting': batting_total_bases_home,
                        f'inning_{inning_number}_walks_batting': batting_walks_home,
                        f'inning_{inning_number}_at_bats_total_batting': at_bats_total_batting_home # Added this line
                    })
                    print(f"DEBUG: Added batting team record for {home_team_abbr} (Bot Inning).")


                    for batter_id in bot_inning_data['batter'].unique():
                        batter_data = bot_inning_data[bot_inning_data['batter'] == batter_id].copy() # Added .copy()
                        batter_name_series_bot = batter_data['bat_play_name']
                        batter_name = batter_name_series_bot.iloc[0] if not batter_name_series_bot.empty else get_player_name_from_id(batter_id)

                        batter_data['bat_score'] = pd.to_numeric(batter_data['bat_score'], errors='coerce').fillna(0)
                        batter_data['post_bat_score'] = pd.to_numeric(batter_data['post_bat_score'], errors='coerce').fillna(0)
                        batter_runs_scored = batter_data['post_bat_score'].max() - batter_data['bat_score'].min()

                        batter_strikeouts = batter_data[batter_data['events'] == 'strikeout'].shape[0]
                        batter_hits = batter_data[batter_data['events'].isin(['single', 'double', 'triple', 'home_run'])].shape[0]
                        batter_singles = batter_data[batter_data['events'] == 'single'].shape[0]
                        batter_doubles = batter_data[batter_data['events'] == 'double'].shape[0]
                        batter_triples = batter_data[batter_data['events'] == 'triple'].shape[0]
                        batter_homers = batter_data[batter_data['events'] == 'home_run'].shape[0]
                        batter_walks = batter_data[batter_data['events'] == 'walk'].shape[0]
                        batter_total_bases = (batter_singles * 1) + (batter_doubles * 2) + (batter_triples * 3) + (batter_homers * 4)


                        individual_batter_records.append({
                            'date': game_date,
                            'game_id': game_pk,
                            'batter_id': batter_id,
                            'batter_name': batter_name,
                            'team_id': home_team_abbr,
                            'opponent_team_id': away_team_abbr,
                            'inning_number': inning_number,
                            f'inning_{inning_number}_runs_scored_batter': batter_runs_scored,
                            f'inning_{inning_number}_strikeouts_batter': batter_strikeouts,
                            f'inning_{inning_number}_hits_batter': batter_hits,
                            f'inning_{inning_number}_singles_batter': batter_singles,
                            f'inning_{inning_number}_doubles_batter': batter_doubles,
                            f'inning_{inning_number}_triples_batter': batter_triples,
                            f'inning_{inning_number}_homers_batter': batter_homers,
                            f'inning_{inning_number}_total_bases_batter': batter_total_bases,
                            f'inning_{inning_number}_walks_batter': batter_walks,
                            f'inning_{inning_number}_at_bats_batter': batter_data['at_bat_number'].nunique() 
                        })
                        print(f"DEBUG: Added individual batter record for {batter_name} (Bot Inning).")


                    team_pitching_strikeouts_away = bot_inning_data[bot_inning_data['events'] == 'strikeout']['pitcher'].nunique()
                    team_pitching_runs_allowed_away = runs_allowed_bot
                    team_pitching_hits_allowed_away = bot_inning_data[bot_inning_data['events'].isin(['single', 'double', 'triple', 'home_run'])].shape[0]
                    team_pitching_walks_allowed_away = bot_inning_data[bot_inning_data['events'] == 'walk'].shape[0]
                    team_batters_faced_away = batters_faced_bot_pitcher


                    team_pitching_records.append({
                        'date': game_date,
                        'game_id': game_pk,
                        'team_id': away_team_abbr, 
                        'opponent_team_id': home_team_abbr, 
                        'inning_number': inning_number,
                        f'inning_{inning_number}_team_strikeouts_pitching': team_pitching_strikeouts_away,
                        f'inning_{inning_number}_team_runs_allowed_pitching': team_pitching_runs_allowed_away,
                        f'inning_{inning_number}_team_hits_allowed_pitching': team_pitching_hits_allowed_away,
                        f'inning_{inning_number}_team_walks_allowed_pitching': team_pitching_walks_allowed_away,
                        f'inning_{inning_number}_team_batters_faced_pitching': team_batters_faced_away
                    })
                    print(f"DEBUG: Added team pitching record for {away_team_abbr} (Bot Inning).")


            # --- Save to CSV ---
            if pitcher_records:
                pitcher_df = pd.DataFrame(pitcher_records)
                pitcher_df.to_csv(pitcher_output_file, index=False)
                logging.info(f"Saved {len(pitcher_df)} pitcher Inning {inning_number} records for {date_str}.")
                print(f"INFO: Saved {len(pitcher_df)} pitcher Inning {inning_number} records for {date_str} to {pitcher_output_file}.")
            else:
                logging.info(f"No pitcher Inning {inning_number} records to save for {date_str}.")
                print(f"INFO: No pitcher Inning {inning_number} records to save for {date_str}. Creating empty file: {pitcher_output_file}")
                pd.DataFrame(columns=get_inning_pitcher_columns(inning_number)).to_csv(pitcher_output_file, index=False)


            if batting_team_records:
                batting_team_df = pd.DataFrame(batting_team_records)
                batting_team_df.to_csv(batting_team_output_file, index=False)
                logging.info(f"Saved {len(batting_team_df)} batting team Inning {inning_number} records for {date_str}.")
                print(f"INFO: Saved {len(batting_team_df)} batting team Inning {inning_number} records for {date_str} to {batting_team_output_file}.")
            else:
                logging.info(f"No batting team Inning {inning_number} records to save for {date_str}.")
                print(f"INFO: No batting team Inning {inning_number} records to save for {date_str}. Creating empty file: {batting_team_output_file}")
                pd.DataFrame(columns=get_inning_batting_columns(inning_number)).to_csv(batting_team_output_file, index=False)


            if individual_batter_records:
                individual_batter_df = pd.DataFrame(individual_batter_records)
                individual_batter_df.to_csv(individual_batter_output_file, index=False)
                logging.info(f"Saved {len(individual_batter_df)} individual batter Inning {inning_number} records for {date_str}.")
                print(f"INFO: Saved {len(individual_batter_df)} individual batter Inning {inning_number} records for {date_str} to {individual_batter_output_file}.")
            else:
                logging.info(f"No individual batter Inning {inning_number} records to save for {date_str}.")
                print(f"INFO: No individual batter Inning {inning_number} records to save for {date_str}. Creating empty file: {individual_batter_output_file}")
                pd.DataFrame(columns=get_inning_individual_batter_columns(inning_number)).to_csv(individual_batter_output_file, index=False)


            if team_pitching_records:
                team_pitching_df = pd.DataFrame(team_pitching_records)
                team_pitching_df.to_csv(team_pitching_output_file, index=False)
                logging.info(f"Saved {len(team_pitching_df)} team pitching Inning {inning_number} records for {date_str}.")
                print(f"INFO: Saved {len(team_pitching_df)} team pitching Inning {inning_number} records for {date_str} to {team_pitching_output_file}.")
            else:
                logging.info(f"No team pitching Inning {inning_number} records to save for {date_str}.")
                print(f"INFO: No team pitching Inning {inning_number} records to save for {date_str}. Creating empty file: {team_pitching_output_file}")
                pd.DataFrame(columns=get_inning_team_pitching_columns(inning_number)).to_csv(team_pitching_output_file, index=False)


            current_day += timedelta(days=1)


    except Exception as e:
        logging.critical(f"An unhandled error occurred during data fetching for Inning {inning_number}: {e}", exc_info=True)
        print(f"CRITICAL ERROR: An unhandled error occurred during data fetching for Inning {inning_number}: {e}")

def consolidate_daily_data(inning_raw_data_dir: str, inning_number: int):
    """
    Consolidates all daily CSVs for a specific inning into master CSVs.
    Now expects daily files to be in date-specific subdirectories.
    """
    logging.info(f"Consolidating daily data for Inning {inning_number} in {inning_raw_data_dir}...")
    print(f"INFO: Consolidating daily data for Inning {inning_number} in {inning_raw_data_dir}...")

    # Updated glob patterns to look into date subdirectories
    pitcher_files = glob.glob(os.path.join(inning_raw_data_dir, '*', f'inning_{inning_number}_pitcher_data.csv'))
    batting_files = glob.glob(os.path.join(inning_raw_data_dir, '*', f'inning_{inning_number}_batting_data.csv'))
    individual_batter_files = glob.glob(os.path.join(inning_raw_data_dir, '*', f'inning_{inning_number}_individual_batter_data.csv'))
    team_pitching_files = glob.glob(os.path.join(inning_raw_data_dir, '*', f'inning_{inning_number}_team_pitching_data.csv'))

    master_pitcher_path = os.path.join(inning_raw_data_dir, f'master_inning_{inning_number}_pitcher_data.csv')
    master_batting_path = os.path.join(inning_raw_data_dir, f'master_inning_{inning_number}_batting_data.csv')
    master_individual_batter_path = os.path.join(inning_raw_data_dir, f'master_inning_{inning_number}_individual_batter_data.csv')
    master_team_pitching_path = os.path.join(inning_raw_data_dir, f'master_inning_{inning_number}_team_pitching_data.csv')

    for files, master_path, df_name in [
        (pitcher_files, master_pitcher_path, "pitcher"),
        (batting_files, master_batting_path, "batting"),
        (individual_batter_files, master_individual_batter_path, "individual_batter"),
        (team_pitching_files, master_team_pitching_path, "team_pitching")
    ]:
        if not files:
            logging.warning(f"No daily {df_name} files found for Inning {inning_number} to consolidate.")
            print(f"WARNING: No daily {df_name} files found for Inning {inning_number} to consolidate. Creating empty master file.")
            # Ensure empty master files have correct headers
            if df_name == "pitcher":
                pd.DataFrame(columns=get_inning_pitcher_columns(inning_number)).to_csv(master_path, index=False)
            elif df_name == "batting":
                pd.DataFrame(columns=get_inning_batting_columns(inning_number)).to_csv(master_path, index=False)
            elif df_name == "individual_batter":
                pd.DataFrame(columns=get_inning_individual_batter_columns(inning_number)).to_csv(master_path, index=False)
            elif df_name == "team_pitching":
                pd.DataFrame(columns=get_inning_team_pitching_columns(inning_number)).to_csv(master_path, index=False)
            logging.info(f"Created empty master file for Inning {inning_number} at {master_path}.")
            continue

        all_data = []
        for f in files:
            try:
                # Check if file is empty before reading
                if os.path.getsize(f) > 0:
                    df = pd.read_csv(f)
                    if not df.empty:
                         all_data.append(df)
                    else:
                        logging.warning(f"Skipping empty file during consolidation: {f}")
                else:
                    logging.warning(f"Skipping empty file (zero size) during consolidation: {f}")
            except pd.errors.EmptyDataError:
                logging.warning(f"Skipping empty file (EmptyDataError) during consolidation: {f}")
            except Exception as e:
                logging.error(f"Error reading {f} for consolidation: {e}")
                print(f"ERROR: Error reading {f} for consolidation: {e}")
                continue

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            if df_name == "pitcher":
                combined_df.drop_duplicates(subset=['game_id', 'date', 'pitcher_id', f'inning_{inning_number}_strikeouts'], inplace=True) # Added metric to key
            elif df_name == "batting":
                combined_df.drop_duplicates(subset=['game_id', 'date', 'team_id', f'inning_{inning_number}_runs_scored'], inplace=True) # Added metric
            elif df_name == "individual_batter":
                combined_df.drop_duplicates(subset=['game_id', 'date', 'batter_id', f'inning_{inning_number}_hits_batter'], inplace=True) # Added metric
            elif df_name == "team_pitching":
                combined_df.drop_duplicates(subset=['game_id', 'date', 'team_id', f'inning_{inning_number}_team_strikeouts_pitching'], inplace=True) # Added metric

            combined_df.to_csv(master_path, index=False)
            logging.info(f"Successfully consolidated {len(combined_df)} {df_name} records for Inning {inning_number} to {master_path}.")
            print(f"INFO: Successfully consolidated {len(combined_df)} {df_name} records for Inning {inning_number} to {master_path}.")
        else:
            logging.warning(f"No valid data to consolidate for {df_name} for Inning {inning_number}.")
            print(f"WARNING: No valid data to consolidate for {df_name} for Inning {inning_number}. Creating empty master file.")
            # Ensure empty master files also have correct headers
            if df_name == "pitcher":
                pd.DataFrame(columns=get_inning_pitcher_columns(inning_number)).to_csv(master_path, index=False)
            elif df_name == "batting":
                pd.DataFrame(columns=get_inning_batting_columns(inning_number)).to_csv(master_path, index=False)
            elif df_name == "individual_batter":
                pd.DataFrame(columns=get_inning_individual_batter_columns(inning_number)).to_csv(master_path, index=False)
            elif df_name == "team_pitching":
                pd.DataFrame(columns=get_inning_team_pitching_columns(inning_number)).to_csv(master_path, index=False)
            logging.info(f"Created empty master file for Inning {inning_number} at {master_path}.")


def get_inning_pitchers_on_date(report_date, inning_number: int):
    """
    Retrieves the pitchers who pitched in the specified inning of games on the specified date
    from the master pitcher data file for that inning.
    Returns a list of dictionaries with home_team, away_team, and pitcher.
    """
    master_pitcher_file = os.path.join(BASE_CACHE_DIR, f'inning_{inning_number}', f'master_inning_{inning_number}_pitcher_data.csv')
    try:
        if not os.path.exists(master_pitcher_file) or os.path.getsize(master_pitcher_file) == 0:
            print(f"Warning: Master pitcher data file for Inning {inning_number} is missing or empty at {master_pitcher_file}.")
            return []
        master_pitcher_df = pd.read_csv(master_pitcher_file)
        if master_pitcher_df.empty:
            print(f"Warning: Master pitcher data file for Inning {inning_number} is empty after loading from {master_pitcher_file}.")
            return []

        master_pitcher_df['date'] = pd.to_datetime(master_pitcher_df['date'])
        report_date_dt = pd.to_datetime(report_date)


        games_on_date = master_pitcher_df[master_pitcher_df['date'] == report_date_dt].drop_duplicates(subset=['game_id', 'is_home_pitcher'])
        result = []
        for game_id_val in games_on_date['game_id'].unique(): # Renamed to avoid conflict
            game_data = games_on_date[games_on_date['game_id'] == game_id_val] # Use renamed variable
            home_pitcher_row = game_data[game_data['is_home_pitcher'] == True].iloc[0] if not game_data[game_data['is_home_pitcher'] == True].empty else None
            away_pitcher_row = game_data[game_data['is_home_pitcher'] == False].iloc[0] if not game_data[game_data['is_home_pitcher'] == False].empty else None


            current_game_info = {
                'game_id': game_id_val, # Use renamed variable
                'home_team': None,
                'away_team': None,
                'home_pitcher': None,
                'away_pitcher': None
            }

            if home_pitcher_row is not None:
                current_game_info['home_team'] = home_pitcher_row['team_id']
                current_game_info['away_team'] = home_pitcher_row['opponent_team_id']
                current_game_info['home_pitcher'] = home_pitcher_row['pitcher_name']
            
            if away_pitcher_row is not None:
                # If home_team/away_team not set by home_pitcher_row, set them here
                if current_game_info['home_team'] is None:
                     current_game_info['home_team'] = away_pitcher_row['opponent_team_id']
                if current_game_info['away_team'] is None:
                     current_game_info['away_team'] = away_pitcher_row['team_id']
                current_game_info['away_pitcher'] = away_pitcher_row['pitcher_name']
            
            if current_game_info['home_team'] or current_game_info['away_team']: # Only add if we have some team info
                 result.append(current_game_info)

        return result


    except FileNotFoundError:
        print(f"Error: Master pitcher data file for Inning {inning_number} not found at {master_pitcher_file}. This might mean data for this inning hasn't been fetched/consolidated yet.")
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred in get_inning_pitchers_on_date for Inning {inning_number}: {e}", exc_info=True)
        return []

def get_todays_probable_pitchers():
    """
    Fetches today's probable pitchers and their teams using statsapi.
    Returns a list of dictionaries with game_id, home_team, away_team, home_pitcher, away_pitcher.
    """
    today_str = datetime.now().strftime('%Y-%m-%d')
    logging.info(f"Attempting to retrieve probable pitchers for {today_str}...")
    print(f"INFO: Attempting to retrieve probable pitchers for {today_str}...")

    try:
        schedule = statsapi.schedule(date=today_str)
        probable_pitchers_info = []
        for game in schedule:
            game_id = game['game_id']
            home_team = get_standard_team_abbreviation(game['home_name'])
            away_team = get_standard_team_abbreviation(game['away_name'])
            
            home_pitcher = game['home_probable_pitcher']
            away_pitcher = game['away_probable_pitcher']

            # If probable pitcher is None, try to get from game content
            if not home_pitcher or not away_pitcher:
                try:
                    game_data = statsapi.get('game', {'gamePk': game_id})
                    if game_data and 'gameData' in game_data and 'probablePitchers' in game_data['gameData']:
                        if not home_pitcher and 'home' in game_data['gameData']['probablePitchers']:
                            home_pitcher = game_data['gameData']['probablePitchers']['home']['fullName']
                        if not away_pitcher and 'away' in game_data['gameData']['probablePitchers']:
                            away_pitcher = game_data['gameData']['probablePitchers']['away']['fullName']
                except Exception as e:
                    logging.warning(f"Could not get probable pitchers from game content for game {game_id}: {e}")
                    print(f"WARNING: Could not get probable pitchers from game content for game {game_id}: {e}")

            # Format names to 'Last, First' if they are available
            home_pitcher_formatted = format_name_last_first(home_pitcher) if home_pitcher else None
            away_pitcher_formatted = format_name_last_first(away_pitcher) if away_pitcher else None


            if home_pitcher_formatted or away_pitcher_formatted: # Only add if at least one pitcher is found
                probable_pitchers_info.append({
                    'game_id': game_id,
                    'home_team': home_team,
                    'away_team': away_team,
                    'home_pitcher': home_pitcher_formatted,
                    'away_pitcher': away_pitcher_formatted
                })
        
        if not probable_pitchers_info:
            logging.warning(f"No probable pitchers found for {today_str}.")
            print(f"WARNING: No probable pitchers found for {today_str}.")
        else:
            logging.info(f"Successfully retrieved {len(probable_pitchers_info)} probable pitcher games for {today_str}.")
            print(f"INFO: Successfully retrieved {len(probable_pitchers_info)} probable pitcher games for {today_str}.") # Corrected variable name
        
        return probable_pitchers_info

    except requests.exceptions.RequestException as e:
        logging.error(f"Network error fetching probable pitchers for {today_str}: {e}")
        print(f"ERROR: Network error fetching probable pitchers for {today_str}: {e}")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error fetching probable pitchers for {today_str}: {e}")
        print(f"ERROR: JSON decode error fetching probable pitchers for {today_str}: {e}")
        return []
    except Exception as e:
        logging.critical(f"An unexpected error occurred in get_todays_probable_pitchers: {e}", exc_info=True)
        print(f"CRITICAL ERROR: An unexpected error occurred in get_todays_probable_pitchers: {e}")
        return []


def analyze_games(game_pitcher_info, report_date, inning_number: int):
    """
    Analyzes historical data for pitchers and opponents for a specific inning
    to generate a report DataFrame.
    """
    master_pitcher_file = os.path.join(BASE_CACHE_DIR, f'inning_{inning_number}', f'master_inning_{inning_number}_pitcher_data.csv')
    master_batting_file = os.path.join(BASE_CACHE_DIR, f'inning_{inning_number}', f'master_inning_{inning_number}_batting_data.csv')


    try:
        if not os.path.exists(master_pitcher_file) or os.path.getsize(master_pitcher_file) == 0:
            print(f"Error: Master pitcher data file for Inning {inning_number} is missing or empty. Cannot analyze.")
            return pd.DataFrame()
        master_pitcher_df = pd.read_csv(master_pitcher_file)
        if master_pitcher_df.empty:
            print(f"Error: Master pitcher data file for Inning {inning_number} is empty after loading. Cannot analyze.")
            return pd.DataFrame()

        if not os.path.exists(master_batting_file) or os.path.getsize(master_batting_file) == 0:
            print(f"Error: Master batting data file for Inning {inning_number} is missing or empty. Cannot analyze.")
            return pd.DataFrame()
        master_batting_df = pd.read_csv(master_batting_file)
        if master_batting_df.empty:
            print(f"Error: Master batting data file for Inning {inning_number} is empty after loading. Cannot analyze.")
            return pd.DataFrame()

    except FileNotFoundError:
        print(f"Error: Master data files for Inning {inning_number} not found. Please ensure data is fetched and consolidated.")
        return pd.DataFrame() 
    except Exception as e:
        logging.error(f"Error loading master data files for Inning {inning_number}: {e}", exc_info=True)
        return pd.DataFrame()


    report_data = []


    master_pitcher_df['date'] = pd.to_datetime(master_pitcher_df['date'])
    master_batting_df['date'] = pd.to_datetime(master_batting_df['date'])
    report_date_dt = pd.to_datetime(report_date)

    # Ensure 'is_home_pitcher' and 'is_home_team' are boolean for correct filtering
    if 'is_home_pitcher' in master_pitcher_df.columns:
        master_pitcher_df['is_home_pitcher'] = master_pitcher_df['is_home_pitcher'].astype(bool)
    else:
        print("WARNING: 'is_home_pitcher' column not found in master_pitcher_df. Venue calculations may be inaccurate.")
        master_pitcher_df['is_home_pitcher'] = False # Default to False if column is missing

    if 'is_home_team' in master_batting_df.columns:
        master_batting_df['is_home_team'] = master_batting_df['is_home_team'].astype(bool)
    else:
        print("WARNING: 'is_home_team' column not found in master_batting_df. Venue calculations may be inaccurate.")
        master_batting_df['is_home_team'] = False # Default to False if column is missing


    PITCHER_METRICS_INNING = get_pitcher_metrics_for_inning(inning_number)
    BATTING_METRICS_INNING = get_batting_metrics_for_inning(inning_number)


    for game_info in game_pitcher_info:
        is_probable_pitcher_entry = False
        if game_info.get('away_pitcher') and isinstance(game_info['away_pitcher'], str) and ',' not in game_info['away_pitcher']:
            is_probable_pitcher_entry = True
        elif game_info.get('home_pitcher') and isinstance(game_info['home_pitcher'], str) and ',' not in game_info['home_pitcher']:
            is_probable_pitcher_entry = True


        home_team_display = game_info['home_team']
        away_team_display = game_info['away_team']
        game_id = game_info['game_id']


        pitchers_to_analyze = []
        if game_info.get('away_pitcher'):
            pitchers_to_analyze.append({
                'name_raw': game_info['away_pitcher'],
                'team_raw': game_info['away_team'],
                'opponent_raw': game_info['home_team'],
                'is_home': False # Pitcher is pitching away
            })
        if game_info.get('home_pitcher'):
            pitchers_to_analyze.append({
                'name_raw': game_info['home_pitcher'],
                'team_raw': game_info['home_team'],
                'opponent_raw': game_info['away_team'],
                'is_home': True # Pitcher is pitching at home
            })


        if not pitchers_to_analyze:
            print(f"DEBUG: No pitchers to analyze for game {game_id} ({away_team_display} @ {home_team_display}). Skipping game.")
            continue


        home_team_abbr = home_team_display
        away_team_abbr = away_team_display


        for pitcher_data in pitchers_to_analyze:
            pitcher_name_raw = pitcher_data['name_raw']
            team_raw = pitcher_data['team_raw']
            opponent_raw = pitcher_data['opponent_raw']
            is_pitcher_home_for_current_game = pitcher_data['is_home'] # True if pitcher is home, False if away
            current_game_id = game_id


            if pitcher_name_raw is None:
                print(f"DEBUG: Skipping pitcher data due to None pitcher_name_raw for game {current_game_id}")
                continue

            if is_probable_pitcher_entry: 
                pitcher_name_for_lookup = format_name_last_first(pitcher_name_raw)
            else: 
                pitcher_name_for_lookup = pitcher_name_raw
            
            opponent_team_abbr_for_lookup = opponent_raw 


            print(f"DEBUG: Analyzing Inning {inning_number} - Pitcher: Raw='{pitcher_name_raw}', Lookup='{pitcher_name_for_lookup}', Team='{team_raw}', Opponent='{opponent_team_abbr_for_lookup}' for Game ID: {current_game_id}")


            past_pitcher_data = master_pitcher_df[master_pitcher_df['date'] < report_date_dt].copy()
            past_batting_data = master_batting_df[master_batting_df['date'] < report_date_dt].copy()


            pitcher_history = past_pitcher_data[past_pitcher_data['pitcher_name'] == pitcher_name_for_lookup].copy()
            games_pitched = pitcher_history['game_id'].nunique()
            print(f"DEBUG: Pitcher {pitcher_name_for_lookup} history shape: {pitcher_history.shape}, games pitched: {games_pitched}")

            # NEW: Calculate last game result for each pitcher metric
            latest_outing_pitcher = None
            if not pitcher_history.empty:
                latest_outing_pitcher = pitcher_history.sort_values(by=['date', 'game_id'], ascending=[False, False]).iloc[0]
            
            # Defensive initialization for opponent_history and opponent_games_played
            opponent_history = past_batting_data[past_batting_data['team_id'] == opponent_team_abbr_for_lookup].copy()
            opponent_games_played = opponent_history['game_id'].nunique()
            print(f"DEBUG: Opponent {opponent_team_abbr_for_lookup} history shape: {opponent_history.shape}, games played: {opponent_games_played}")

            # NEW: Calculate last game result for each opponent metric
            latest_outing_opponent = None
            if not opponent_history.empty:
                latest_outing_opponent = opponent_history.sort_values(by=['date', 'game_id'], ascending=[False, False]).iloc[0]


            # NEW: Pitcher Venue NRFI % (calculated for both, then selected)
            # NOTE: NRFI is "No Runs First Inning", so this is inherently runs-based.
            pitcher_home_history = pitcher_history[pitcher_history['is_home_pitcher'] == True].copy()
            pitcher_away_history = pitcher_history[pitcher_history['is_home_pitcher'] == False].copy()

            pitcher_home_games = pitcher_home_history['game_id'].nunique()
            pitcher_away_games = pitcher_away_history['game_id'].nunique()

            pitcher_home_nrfi_count = pitcher_home_history[pitcher_home_history[f'inning_{inning_number}_runs_allowed'] == 0]['game_id'].nunique()
            pitcher_away_nrfi_count = pitcher_away_history[pitcher_away_history[f'inning_{inning_number}_runs_allowed'] == 0]['game_id'].nunique()

            pitcher_home_nrfi_pct = (pitcher_home_nrfi_count / pitcher_home_games) * 100 if pitcher_home_games > 0 else 0.0
            pitcher_away_nrfi_pct = (pitcher_away_nrfi_count / pitcher_away_games) * 100 if pitcher_away_games > 0 else 0.0

            # Select the correct pitcher venue NRFI % for the current game
            pitcher_venue_nrfi_pct = pitcher_home_nrfi_pct if is_pitcher_home_for_current_game else pitcher_away_nrfi_pct


            # NEW: Opponent Venue NRFI % (calculated for both, then selected)
            # NOTE: NRFI is "No Runs First Inning", so this is inherently runs-based.
            opponent_home_history = opponent_history[opponent_history['is_home_team'] == True].copy()
            opponent_away_history = opponent_history[opponent_history['is_home_team'] == False].copy()

            opponent_home_games = opponent_home_history['game_id'].nunique()
            opponent_away_games = opponent_away_history['game_id'].nunique()

            opponent_home_nrfi_count = opponent_home_history[opponent_home_history[f'inning_{inning_number}_runs_scored'] == 0]['game_id'].nunique()
            opponent_away_nrfi_count = opponent_away_history[opponent_away_history[f'inning_{inning_number}_runs_scored'] == 0]['game_id'].nunique()

            opponent_home_nrfi_pct = (opponent_home_nrfi_count / opponent_home_games) * 100 if opponent_home_games > 0 else 0.0
            opponent_away_nrfi_pct = (opponent_away_nrfi_count / opponent_away_games) * 100 if opponent_away_games > 0 else 0.0

            # Select the correct opponent venue NRFI % for the current game
            # The opponent's home status is the inverse of the pitcher's home status for the same game
            is_opponent_home_for_current_game = not is_pitcher_home_for_current_game
            opponent_venue_nrfi_pct = opponent_home_nrfi_pct if is_opponent_home_for_current_game else opponent_away_nrfi_pct


            # NEW: Pitcher Venue NRHI % (No Hits Inning)
            print(f"DEBUG: Columns in pitcher_history before NRHI venue calc: {pitcher_history.columns.tolist()}")
            pitcher_home_nrhi_count = pitcher_home_history[pitcher_home_history[f'inning_{inning_number}_hits_allowed'] == 0]['game_id'].nunique()
            pitcher_away_nrhi_count = pitcher_away_history[pitcher_away_history[f'inning_{inning_number}_hits_allowed'] == 0]['game_id'].nunique()

            pitcher_home_nrhi_pct = (pitcher_home_nrhi_count / pitcher_home_games) * 100 if pitcher_home_games > 0 else 0.0
            pitcher_away_nrhi_pct = (pitcher_away_nrhi_count / pitcher_away_games) * 100 if pitcher_away_games > 0 else 0.0
            pitcher_venue_nrhi_pct = pitcher_home_nrhi_pct if is_pitcher_home_for_current_game else pitcher_away_nrhi_pct
            print(f"DEBUG: Calculated pitcher_venue_nrhi_pct: {pitcher_venue_nrhi_pct}")


            # NEW: Batting Venue NRHI % (No Hits Inning)
            print(f"DEBUG: Columns in opponent_history before NRHI venue calc: {opponent_history.columns.tolist()}")
            opponent_home_nrhi_count = opponent_home_history[opponent_home_history[f'inning_{inning_number}_hits_batting'] == 0]['game_id'].nunique()
            opponent_away_nrhi_count = opponent_away_history[opponent_away_history[f'inning_{inning_number}_hits_batting'] == 0]['game_id'].nunique()

            opponent_home_nrhi_pct = (opponent_home_nrhi_count / opponent_home_games) * 100 if opponent_home_games > 0 else 0.0
            opponent_away_nrhi_pct = (opponent_away_nrhi_count / opponent_away_games) * 100 if opponent_away_games > 0 else 0.0
            batter_venue_nrhi_pct = opponent_home_nrhi_pct if is_opponent_home_for_current_game else opponent_away_nrhi_pct
            print(f"DEBUG: Calculated batter_venue_nrhi_pct: {batter_venue_nrhi_pct}")


            # NEW: Pitcher Venue K Rate % (percentage of games with at least one strikeout)
            print(f"DEBUG: Pitcher {pitcher_name_for_lookup} home history shape for K rate: {pitcher_home_history.shape}")
            print(f"DEBUG: Pitcher {pitcher_name_for_lookup} away history shape for K rate: {pitcher_away_history.shape}")

            pitcher_home_k_games_count = pitcher_home_history[pd.to_numeric(pitcher_home_history[f'inning_{inning_number}_strikeouts'], errors='coerce') >= 1]['game_id'].nunique()
            pitcher_home_k_rate = (pitcher_home_k_games_count / pitcher_home_games) * 100 if pitcher_home_games > 0 else 0.0
            print(f"DEBUG: Pitcher {pitcher_name_for_lookup} home K games count: {pitcher_home_k_games_count}, games: {pitcher_home_games}, rate: {pitcher_home_k_rate}")

            pitcher_away_k_games_count = pitcher_away_history[pd.to_numeric(pitcher_away_history[f'inning_{inning_number}_strikeouts'], errors='coerce') >= 1]['game_id'].nunique()
            pitcher_away_k_rate = (pitcher_away_k_games_count / pitcher_away_games) * 100 if pitcher_away_games > 0 else 0.0
            print(f"DEBUG: Pitcher {pitcher_name_for_lookup} away K games count: {pitcher_away_k_games_count}, games: {pitcher_away_games}, rate: {pitcher_away_k_rate}")

            pitcher_venue_k_rate = pitcher_home_k_rate if is_pitcher_home_for_current_game else pitcher_away_k_rate
            print(f"DEBUG: Pitcher {pitcher_name_for_lookup} is_home_for_current_game: {is_pitcher_home_for_current_game}, selected venue K rate: {pitcher_venue_k_rate}")

            # NEW: Opponent Venue K Rate % (Batting - percentage of games with at least one strikeout)
            is_opponent_home_for_current_game = not is_pitcher_home_for_current_game

            print(f"DEBUG: Opponent {opponent_team_abbr_for_lookup} home history shape for K rate: {opponent_home_history.shape}")
            print(f"DEBUG: Opponent {opponent_team_abbr_for_lookup} away history shape for K rate: {opponent_away_history.shape}")

            opponent_home_k_batting_games_count = opponent_home_history[pd.to_numeric(opponent_home_history[f'inning_{inning_number}_strikeouts_batting'], errors='coerce') >= 1]['game_id'].nunique()
            opponent_home_k_batting_rate = (opponent_home_k_batting_games_count / opponent_home_games) * 100 if opponent_home_games > 0 else 0.0
            print(f"DEBUG: Opponent {opponent_team_abbr_for_lookup} home K batting games count: {opponent_home_k_batting_games_count}, games: {opponent_home_games}, rate: {opponent_home_k_batting_rate}")

            opponent_away_k_batting_games_count = opponent_away_history[pd.to_numeric(opponent_away_history[f'inning_{inning_number}_strikeouts_batting'], errors='coerce') >= 1]['game_id'].nunique()
            opponent_away_k_batting_rate = (opponent_away_k_batting_games_count / opponent_away_games) * 100 if opponent_away_games > 0 else 0.0
            print(f"DEBUG: Opponent {opponent_team_abbr_for_lookup} away K batting games count: {opponent_away_k_batting_games_count}, games: {opponent_away_games}, rate: {opponent_away_k_batting_rate}")

            opponent_venue_k_rate = opponent_home_k_batting_rate if is_opponent_home_for_current_game else opponent_away_k_batting_rate
            print(f"DEBUG: Opponent {opponent_team_abbr_for_lookup} is_opponent_home_for_current_game: {is_opponent_home_for_current_game}, selected venue K rate: {opponent_venue_k_rate}")


            parts = pitcher_name_raw.strip().split()
            if len(parts) > 1:
                first_name = parts[-1].rstrip(",")
                last_name = " ".join(parts[:-1]).rstrip(",")
                pfull_name = f"{first_name} {last_name}"
            else:
                pfull_name = pitcher_name_raw.rstrip(",")    
            
            report_entry = {
                'Game': f"{away_team_abbr} @ {home_team_abbr}", 
                'Pitcher': pfull_name,
                f'# TOTAL STARTS INNING {inning_number}': games_pitched,
                'Pitcher Team': team_raw, 
                'Opponent': opponent_team_abbr_for_lookup, 
                'Game ID': game_id,
            }

            # Initialize all possible metric keys with default values to prevent KeyError
            all_potential_report_keys = set()
            for metric_list in [PITCHER_METRICS_INNING, BATTING_METRICS_INNING]:
                for metric in metric_list:
                    if metric.get('report_key_avg'): all_potential_report_keys.add(metric['report_key_avg'])
                    if metric.get('report_key_rate'): all_potential_report_keys.add(metric['report_key_rate'])
                    if metric.get('report_key_per_game_pct'): all_potential_report_keys.add(metric['report_key_per_game_pct'])
                    if metric.get('last_game_key'): all_potential_report_keys.add(metric['last_game_key'])
                    if metric.get('today_report_key'): all_potential_report_keys.add(metric['today_report_key'])

            # Add all confidence and bet columns that are calculated in generate_first_inning_report
            all_potential_report_keys.update([
                f'PITCHER VENUE NRFI % INNING {inning_number}',
                f'OPPONENT VENUE NRFI % INNING {inning_number}',
                f'PITCHER VENUE NRHI % INNING {inning_number}',
                f'BATTER VENUE NRHI % INNING {inning_number}',
                'VENUE PITCH K RATE %', # Explicitly added here
                'VENUE OPP K RATE %', # Explicitly added here
                'PITCH K CONF', 'OPP K CONF', 'Overall K CONFIDENCE', 'PITCHER K BET', 'OPPONENT K BET', # Added OPPONENT K BET
                f'PITCH NRFI % INNING {inning_number}', f'PITCH NRFI CONF',
                'OPP R/G CONF', 'PITCH RUNS ALLOWED CONF', 'Overall Run Prevention Confidence',
                'Overall CONFIDENCE FOR NRFI AND YRFI', 'PITCHER RUNS BET', 'OPPONENT RUNS BET',
                'PITCH NRHI CONF', 'PITCHER VENUE NRHI CONF', 'BAT NRHI CONF', 'BATTER VENUE NRHI CONF',
                'Overall HITS CONFIDENCE', 'PITCH HITS ALLOWED CONF', 'OPP HITS BATTING CONF', 'OPPONENT HITS BET', # Added OPPONENT HITS BET
                'PITCH WALKS ALLOWED CONF', 'OPP WALKS BATTING CONF', 'Overall WALKS CONFIDENCE', 'PITCHER WALKS BET', 'OPPONENT WALKS BET', # Added OPPONENT WALKS BET
                'PITCH SINGLES ALLOWED CONF', 'OPP SINGLES BATTING CONF', 'Overall SINGLES CONFIDENCE', 'PITCHER SINGLES BET', 'OPPONENT SINGLES BET', # Added OPPONENT SINGLES BET
                'PITCH DOUBLES ALLOWED CONF', 'OPP DOUBLES BATTING CONF', 'Overall DOUBLES CONFIDENCE', 'PITCHER DOUBLES BET', 'OPPONENT DOUBLES BET', # Added OPPONENT DOUBLES BET
                'PITCH TRIPLES ALLOWED CONF', 'OPP TRIPLES BATTING CONF', 'Overall TRIPLES CONFIDENCE', 'PITCHER TRIPLES BET', 'OPPONENT TRIPLES BET', # Added OPPONENT TRIPLES BET
                'PITCH HOMERS ALLOWED CONF', 'OPP HOMERS BATTING CONF', 'Overall HOMERS CONFIDENCE', 'PITCHER HOMERS BET', 'OPPONENT HOMERS BET', # Added OPPONENT HOMERS BET
                'PITCH TOTAL BASES ALLOWED CONF', 'OPP TOTAL BASES BATTING CONF', 'Overall TOTAL BASES CONFIDENCE', 'PITCHER TOTAL BASES BET', 'OPPONENT TOTAL BASES BET', # Added OPPONENT TOTAL BASES BET
            ])

            for key in all_potential_report_keys:
                if key not in report_entry:
                    # Determine default value based on expected type
                    if 'CONFIDENCE' in key or 'CONF' in key or 'BET' in key:
                        report_entry[key] = "Neutral"
                    elif 'LAST GAME' in key:
                        report_entry[key] = "N/A"
                    else:
                        report_entry[key] = 0.0


            # Explicitly set the venue percentages/rates, which are always calculated
            report_entry[f'PITCHER VENUE NRFI % INNING {inning_number}'] = pitcher_venue_nrfi_pct
            report_entry[f'OPPONENT VENUE NRFI % INNING {inning_number}'] = opponent_venue_nrfi_pct
            report_entry[f'PITCHER VENUE NRHI % INNING {inning_number}'] = pitcher_venue_nrhi_pct
            report_entry[f'BATTER VENUE NRHI % INNING {inning_number}'] = batter_venue_nrhi_pct
            report_entry['VENUE PITCH K RATE %'] = pitcher_venue_k_rate
            report_entry['VENUE OPP K RATE %'] = opponent_venue_k_rate
            print(f"DEBUG: Final report_entry VENUE PITCH K RATE %: {report_entry['VENUE PITCH K RATE %']}")
            print(f"DEBUG: Final report_entry VENUE OPP K RATE %: {report_entry['VENUE OPP K RATE %']}")


            # Populate metric-specific last game results for pitcher
            for metric in PITCHER_METRICS_INNING:
                col_name = metric['col_name']
                last_game_key = metric['last_game_key']
                if last_game_key and latest_outing_pitcher is not None and col_name in latest_outing_pitcher:
                    report_entry[last_game_key] = latest_outing_pitcher[col_name]
                    print(f"DEBUG: Setting {last_game_key} for pitcher {pitcher_name_for_lookup}: {latest_outing_pitcher[col_name]}")
                elif last_game_key: # Only set if key exists
                    report_entry[last_game_key] = "N/A" # Or 0, depending on desired default
                    print(f"DEBUG: Setting {last_game_key} for pitcher {pitcher_name_for_lookup} to N/A (no latest outing or col missing).")


            # Populate metric-specific last game results for opponent
            for metric in BATTING_METRICS_INNING:
                col_name = metric['col_name']
                last_game_key = metric['last_game_key']
                if last_game_key and latest_outing_opponent is not None and col_name in latest_outing_opponent:
                    report_entry[last_game_key] = latest_outing_opponent[col_name]
                    print(f"DEBUG: Setting {last_game_key} for opponent {opponent_team_abbr_for_lookup}: {latest_outing_opponent[col_name]}")
                elif last_game_key: # Only set if key exists
                    report_entry[last_game_key] = "N/A" # Or 0
                    print(f"DEBUG: Setting {last_game_key} for opponent {opponent_team_abbr_for_lookup} to N/A (no latest outing or col missing).")


            for metric in PITCHER_METRICS_INNING: 
                col_name = metric['col_name']
                report_key_avg = metric.get('report_key_avg')
                report_key_rate = metric.get('report_key_rate') 
                report_key_per_game_pct = metric.get('report_key_per_game_pct')
                total_col = metric.get('total_col')

                # Handle 'PITCH K RATE %' specifically for the new calculation logic
                if report_key_rate == 'PITCH K RATE %':
                    if games_pitched > 0:
                        # Count games where pitcher had at least one strikeout
                        games_with_k = pitcher_history[pd.to_numeric(pitcher_history[col_name], errors='coerce') >= 1]['game_id'].nunique()
                        report_entry[report_key_rate] = (games_with_k / games_pitched) * 100
                    else:
                        report_entry[report_key_rate] = 0.0
                elif col_name in pitcher_history.columns:
                    if report_key_per_game_pct: # This is for 'PITCH HIT AVG' (formerly AVG HITS ALLOWED/INN)
                        if games_pitched > 0:
                            # For percentage per game, we count games where the metric was > 0
                            # This applies to hits, singles, doubles, triples, homers, walks allowed
                            # For strikeouts, it might be different (e.g., K rate is already handled by report_key_rate)
                            # Runs allowed percentage is also handled separately
                            if col_name == f'inning_{inning_number}_hits_allowed': # Specific for NRHI
                                games_with_zero_hits_allowed = pitcher_history[pitcher_history[col_name] == 0]['game_id'].nunique()
                                report_entry[report_key_per_game_pct] = (games_with_zero_hits_allowed / games_pitched) * 100
                            elif col_name not in [f'inning_{inning_number}_strikeouts', f'inning_{inning_number}_runs_allowed']:
                                games_with_metric_allowed = pitcher_history[pitcher_history[col_name] > 0]['game_id'].nunique()
                                report_entry[report_key_per_game_pct] = (games_with_metric_allowed / games_pitched) * 100
                            else: # For runs and strikeouts, percentage per game might not be relevant or handled differently
                                report_entry[report_key_per_game_pct] = 0.0 # Default or specific logic if needed
                        else:
                            report_entry[report_key_per_game_pct] = 0.0
                    
                    if report_key_avg: # This is for 'PITCH HIT AVG #' (formerly PITCH HITS ALLOWED/GM or PITCH HITS ALLOWED RATE %)
                        if 'VENUE' in report_key_avg: # Generic check for venue metrics
                            venue_pitcher_history = pitcher_history[pitcher_history['is_home_pitcher'] == is_pitcher_home_for_current_game].copy()
                            venue_games_pitched = venue_pitcher_history['game_id'].nunique()
                            
                            if 'NRHI' in report_key_avg: # Specific for NRHI
                                games_with_zero_hits_allowed_at_venue = venue_pitcher_history[venue_pitcher_history[f'inning_{inning_number}_hits_allowed'] == 0]['game_id'].nunique()
                                report_entry[report_key_avg] = (games_with_zero_hits_allowed_at_venue / venue_games_pitched) * 100 if venue_games_pitched > 0 else 0.0
                            else: # Original average calculation
                                total_metric_allowed_at_venue = venue_pitcher_history[col_name].sum()
                                report_entry[report_key_avg] = total_metric_allowed_at_venue / venue_games_pitched if venue_games_pitched > 0 else 0.0
                        else:
                            total_sum = pitcher_history[col_name].sum()
                            report_entry[report_key_avg] = total_sum / games_pitched if games_pitched > 0 else 0.0

                    # This block is now only for other rates that still use total_col
                    if report_key_rate and total_col and total_col in pitcher_history.columns: 
                        total_sum = pitcher_history[col_name].sum()
                        total_for_rate = pitcher_history[total_col].sum()
                        report_entry[report_key_rate] = (total_sum / total_for_rate) * 100 if total_for_rate > 0 else 0.0
                    elif report_key_rate: 
                        report_entry[report_key_rate] = 0.0
                else:
                    if report_key_avg: report_entry[report_key_avg] = 0.0
                    if report_key_rate: report_entry[report_key_rate] = 0.0
                    if report_key_per_game_pct: report_entry[report_key_per_game_pct] = 0.0
                    print(f"DEBUG: Pitcher metric column '{col_name}' not found in pitcher_history for {pitcher_name_for_lookup}.")


            # Calculate Pitcher NRFI % based on runs allowed
            runs_allowed_col = f'inning_{inning_number}_runs_allowed'
            if runs_allowed_col in pitcher_history.columns:
                 pitcher_nrfi_count = pitcher_history[pitcher_history[runs_allowed_col] == 0]['game_id'].nunique()
                 zero_run_games = pitcher_nrfi_count
                 report_entry[f'PITCH NRFI % INNING {inning_number}'] = (zero_run_games / games_pitched) * 100 if games_pitched > 0 else 0.0
            else:
                report_entry[f'PITCH NRFI % INNING {inning_number}'] = 0.0
                print(f"DEBUG: Pitcher metric column '{runs_allowed_col}' not found for NRFI/%0Runs calc.")


            for metric in BATTING_METRICS_INNING: 
                col_name = metric['col_name']
                report_key_avg = metric.get('report_key_avg')
                report_key_rate = metric.get('report_key_rate')
                report_key_per_game_pct = metric.get('report_key_per_game_pct') # Added for batting NRHI
                total_col = metric.get('total_col')

                # Handle 'OPP K RATE %' specifically for the new calculation logic
                if report_key_rate == 'OPP K RATE %':
                    if opponent_games_played > 0:
                        # Count games where opponent had at least one strikeout
                        games_with_k = opponent_history[pd.to_numeric(opponent_history[col_name], errors='coerce') >= 1]['game_id'].nunique()
                        report_entry[report_key_rate] = (games_with_k / opponent_games_played) * 100
                    else:
                        report_entry[report_key_rate] = 0.0
                elif col_name in opponent_history.columns:
                    if report_key_per_game_pct: # This is for BAT NRHI %
                        if opponent_games_played > 0:
                            games_with_zero_hits_batting = opponent_history[opponent_history[col_name] == 0]['game_id'].nunique()
                            report_entry[report_key_per_game_pct] = (games_with_zero_hits_batting / opponent_games_played) * 100
                        else:
                            report_entry[report_key_per_game_pct] = 0.0

                    if report_key_avg: # This is for 'BAT HIT AVG #'
                        if 'VENUE' in report_key_avg: # Generic check for venue metrics
                            venue_opponent_history = opponent_history[opponent_history['is_home_team'] == is_opponent_home_for_current_game].copy()
                            venue_games_played = venue_opponent_history['game_id'].nunique()
                            
                            if 'NRHI' in report_key_avg: # Specific for NRHI
                                games_with_zero_hits_batting_at_venue = venue_opponent_history[venue_opponent_history[f'inning_{inning_number}_hits_batting'] == 0]['game_id'].nunique()
                                report_entry[report_key_avg] = (games_with_zero_hits_batting_at_venue / venue_games_played) * 100 if venue_games_played > 0 else 0.0
                            else: # Original average calculation
                                total_metric_batting_at_venue = venue_opponent_history[col_name].sum()
                                report_entry[report_key_avg] = total_metric_batting_at_venue / venue_games_played if venue_games_played > 0 else 0.0
                        elif report_key_avg.startswith('OPP ') and report_key_avg.endswith('/GM'): # Revert * 100 for these specific metrics
                            total_sum = opponent_history[col_name].sum()
                            report_entry[report_key_avg] = total_sum / opponent_games_played if opponent_games_played > 0 else 0.0
                        else:
                            total_sum = opponent_history[col_name].sum()
                            report_entry[report_key_avg] = total_sum / opponent_games_played if opponent_games_played > 0 else 0.0

                    # This block is now only for other rates that still use total_col
                    if report_key_rate and total_col and total_col in opponent_history.columns:
                        total_sum = opponent_history[col_name].sum()
                        total_for_rate = opponent_history[total_col].sum()
                        report_entry[report_key_rate] = (total_sum / total_for_rate) * 100 if total_for_rate > 0 else 0.0
                    elif report_key_rate: 
                        report_entry[report_key_rate] = 0.0
                else:
                    if report_key_avg: report_entry[report_key_avg] = 0.0
                    if report_key_rate: report_entry[report_key_rate] = 0.0
                    if report_key_per_game_pct: report_entry[report_key_per_game_pct] = 0.0 # Added for batting NRHI
                    print(f"DEBUG: Opponent metric column '{col_name}' not found in opponent_history for {opponent_team_abbr_for_lookup}.")


            # Calculate Opponent NRFI % based on runs scored and add to report_entry
            runs_scored_col = f'inning_{inning_number}_runs_scored'
            if runs_scored_col in opponent_history.columns:
                opponent_nrfi_count = opponent_history[opponent_history[runs_scored_col] == 0]['game_id'].nunique()
                report_entry[f'BAT NRFI % INNING {inning_number}'] = (opponent_nrfi_count / opponent_games_played) * 100 if opponent_games_played > 0 else 0.0
            else:
                report_entry[f'BAT NRFI % INNING {inning_number}'] = 0.0
                print(f"DEBUG: Opponent metric column '{runs_scored_col}' not found for NRFI calc.")


            # Get 'TODAY's data (actuals for the game on report_date)
            # For probable pitchers, this will be 0 as it's a future game.
            # For historical dates, it will pull the actual data for that game.
            todays_pitcher_data_game = master_pitcher_df[
                (master_pitcher_df['game_id'] == current_game_id) &
                (master_pitcher_df['pitcher_name'] == pitcher_name_for_lookup) &
                (master_pitcher_df['date'] == report_date_dt)
            ].copy()

            todays_opponent_batting_data_game = master_batting_df[
                (master_batting_df['game_id'] == current_game_id) &
                (master_batting_df['team_id'] == opponent_team_abbr_for_lookup) & 
                (master_batting_df['date'] == report_date_dt)
            ].copy()


            for metric in PITCHER_METRICS_INNING: 
                col_name = metric['col_name']
                today_key = metric['today_report_key']
                if today_key and not todays_pitcher_data_game.empty and col_name in todays_pitcher_data_game.columns:
                    report_entry[today_key] = todays_pitcher_data_game[col_name].sum() 
                    print(f"DEBUG: TODAY PITCHER {col_name} for {pitcher_name_for_lookup}: {report_entry[today_key]}")
                elif today_key: # Only set if key exists
                    report_entry[today_key] = 0
                    print(f"DEBUG: TODAY PITCHER {col_name} for {pitcher_name_for_lookup}: 0 (no data).")


            for metric in BATTING_METRICS_INNING: 
                col_name = metric['col_name']
                today_key = metric['today_report_key']
                if today_key and not todays_opponent_batting_data_game.empty and col_name in todays_opponent_batting_data_game.columns:
                    report_entry[today_key] = todays_opponent_batting_data_game[col_name].sum()
                    print(f"DEBUG: TODAY OPPONENT {col_name} for {opponent_team_abbr_for_lookup}: {report_entry[today_key]}")
                elif today_key: # Only set if key exists
                    report_entry[today_key] = 0
                    print(f"DEBUG: TODAY OPPONENT {col_name} for {opponent_team_abbr_for_lookup}: 0 (no data).")


            report_data.append(report_entry)
            print(f"DEBUG: Appended report_entry for game {current_game_id}, pitcher {pitcher_name_for_lookup}. Keys: {report_entry.keys()}")
            print(f"DEBUG: Sample report_entry: {report_entry.get(f'LAST GAME PITCHER STRIKEOUTS INNING {inning_number}', 'N/A')}, {report_entry.get(f'LAST GAME OPPONENT RUNS SCORED INNING {inning_number}', 'N/A')}, {report_entry.get(f'PITCHER VENUE NRHI % INNING {inning_number}', 'N/A')}")


    print(f"DEBUG: Final report_data for Inning {inning_number} before DataFrame creation (length {len(report_data)}): {report_data[:2]}...")
    if not report_data:
        print(f"WARNING: No report data compiled for Inning {inning_number}. Returning empty DataFrame.")
        return pd.DataFrame(), {}, {}, {}, {}, {}, {} # Return empty dictionaries for recommendations and parlays
    
    # DEBUG: Print all columns in report_data before creating DataFrame
    if report_data:
        first_entry_keys = set(report_data[0].keys())
        all_keys = set().union(*[d.keys() for d in report_data])
        if first_entry_keys != all_keys:
            print(f"WARNING: Inconsistent keys in report_data. Missing keys in first entry: {all_keys - first_entry_keys}")
            print(f"WARNING: Extra keys in first entry: {first_entry_keys - all_keys}")
    
    return pd.DataFrame(report_data)


def calculate_overall_nrfi_yrfi_confidence(pitcher_nrfi_pct, opponent_nrfi_pct, overall_run_prevention_conf):
    """
    Calculates the overall NRFI/YRFI confidence based on pitcher and opponent NRFI percentages
    and overall run prevention confidence.
    """
    # Convert confidence string to a numeric value for easier comparison
    run_prevention_score = confidence_map.get(overall_run_prevention_conf, 0)

    # NRFI Logic (Lower runs allowed and scored -> Higher NRFI confidence)
    if pitcher_nrfi_pct >= 70 and opponent_nrfi_pct >= 70 and run_prevention_score >= 2: # High or Moderate
        return "High (NRFI)"
    elif pitcher_nrfi_pct >= 60 and opponent_nrfi_pct >= 60 and run_prevention_score >= 1: # Low or better
        return "Moderate (leaning NRFI)"
    
    # YRFI Logic (Lower runs allowed and scored -> Higher NRFI confidence)
    # This is inverse of NRFI. If NRFI conditions are not met, consider YRFI.
    # YRFI means runs ARE scored, so we look for lower NRFI percentages.
    pitcher_yrfi_pct = 100 - pitcher_nrfi_pct
    opponent_yrfi_pct = 100 - opponent_nrfi_pct

    if pitcher_yrfi_pct >= 70 and opponent_yrfi_pct >= 70:
        return "High (YRFI)"
    elif pitcher_yrfi_pct >= 60 and opponent_yrfi_pct >= 60:
        return "Moderate (leaning YRFI)"

    return "Low" # Default if neither high NRFI nor high YRFI conditions are met


def calculate_overall_nrhi_confidence(pitcher_nrhi_pct, batting_nrhi_pct):
    """
    Calculates the overall NRHI confidence based on pitcher and batting NRHI percentages.
    Higher percentages mean higher confidence in No Hits.
    """
    if pitcher_nrhi_pct >= 70 and batting_nrhi_pct >= 70:
        return "High (Under)" # High confidence for Under Hits
    elif pitcher_nrhi_pct >= 60 and batting_nrhi_pct >= 60:
        return "Moderate (leaning Under)"
    elif pitcher_nrhi_pct <= 30 and batting_nrhi_pct <= 30: # Low NRHI means high hits
        return "High (Over)" # High confidence for Over Hits
    elif pitcher_nrhi_pct <= 40 and batting_nrhi_pct <= 40:
        return "Moderate (leaning Over)"
    return "Neutral"


def calculate_overall_over_under_confidence(pitcher_conf, opponent_conf):
    """
    Calculates overall confidence for Over/Under metrics (Hits, Walks, etc.).
    Assumes 'High' for pitcher means 'Under' (good pitching), and 'High' for opponent means 'Over' (good batting).
    """
    pitcher_score = confidence_map.get(pitcher_conf, 0)
    opponent_score = confidence_map.get(opponent_conf, 0)

    # High (Under) scenario: Pitcher is good at preventing, Opponent is bad at hitting
    if pitcher_score == 3 and opponent_score == 1: # Pitcher High, Opponent Low
        return "High (Under)"
    elif pitcher_score >= 2 and opponent_score <= 2: # Pitcher Moderate/High, Opponent Low/Moderate
        return "Moderate (leaning Under)"

    # High (Over) scenario: Pitcher is bad at preventing, Opponent is good at hitting
    elif pitcher_score == 1 and opponent_score == 3: # Pitcher Low, Opponent High
        return "High (Over)"
    elif pitcher_score <= 2 and opponent_score >= 2: # Pitcher Low/Moderate, Opponent Moderate/High
        return "Moderate (leaning Over)"

    return "Neutral"


def generate_report_data_and_pdfs(report_date=None, inning_number: int = 1):
    """
    Generates the dataframes and recommendations for reports for a specific inning.
    Also generates PDFs and returns their paths.
    """
    is_today_report = False
    if report_date is None or report_date.lower() == 'today':
        report_date = datetime.now().strftime('%Y-%m-%d')
        is_today_report = True
        print(f"Generating report data for today's games ({report_date}) for Inning {inning_number}...")
        game_info_list = get_todays_probable_pitchers() 
        if not game_info_list:
            print(f"Could not retrieve probable pitchers for today ({report_date}) for Inning {inning_number}.")
            return pd.DataFrame(), {}, {}, {}, {}, {}, {}, [] # Return empty dataframes/dicts and empty pdf_paths
    else:
        try:
            datetime.strptime(report_date, '%Y-%m-%d')
            print(f"Generating report data for games on {report_date} for Inning {inning_number}...")
            game_info_list = get_inning_pitchers_on_date(report_date, inning_number)
            if not game_info_list:
                print(f"Could not retrieve historical pitchers for {report_date}, Inning {inning_number}.")
                return pd.DataFrame(), {}, {}, {}, {}, {}, {}, [] # Return empty dataframes/dicts and empty pdf_paths
        except ValueError:
            print(f"Invalid date format: {report_date}. Please useYYYY-MM-DD or 'today'.")
            return pd.DataFrame(), {}, {}, {}, {}, {}, {}, [] # Return empty dataframes/dicts and empty pdf_paths

    if game_info_list is None:
        print(f"game_info_list is None for Inning {inning_number}, Date {report_date}. Aborting report generation.")
        return pd.DataFrame(), {}, {}, {}, {}, {}, {}, [] # Return empty dataframes/dicts and empty pdf_paths

    report_df = analyze_games(game_info_list, report_date=report_date, inning_number=inning_number)


    if report_df is not None and not report_df.empty:
        # Initialize NRHI columns to prevent KeyError for non-Hits metrics
        for col_name in [f'PITCH NRHI % INNING {inning_number}', f'PITCHER VENUE NRHI % INNING {inning_number}',
                         f'BAT NRHI % INNING {inning_number}', f'BATTER VENUE NRHI % INNING {inning_number}',
                         'VENUE PITCH K RATE %', 'VENUE OPP K RATE %']: # Added new K rate venue columns
            if col_name not in report_df.columns:
                report_df[col_name] = 0.0 # Initialize with a default value

        # Initialize other confidence columns to prevent KeyError
        for col_name in [
            'PITCH WALKS ALLOWED CONF', 'OPP WALKS BATTING CONF',
            'PITCH SINGLES ALLOWED CONF', 'OPP SINGLES BATTING CONF',
            'PITCH DOUBLES ALLOWED CONF', 'OPP DOUBLES BATTING CONF',
            'PITCH TRIPLES ALLOWED CONF', 'OPP TRIPLES BATTING CONF',
            'PITCH HOMERS ALLOWED CONF', 'OPP HOMERS BATTING CONF',
            'PITCH TOTAL BASES ALLOWED CONF', 'OPP TOTAL BASES BATTING CONF'
        ]:
            if col_name not in report_df.columns:
                report_df[col_name] = "Neutral" # Initialize with a default confidence level

        # --- Perform column renames BEFORE confidence calculations ---
        if 'PITCH K RATE %' in report_df.columns:
            report_df.rename(columns={'PITCH K RATE %': 'PITCH K OVERALL RATE %'}, inplace=True)
        if 'OPP K RATE %' in report_df.columns:
            report_df.rename(columns={'OPP K RATE %': 'OPP K OVERALL RATE %'}, inplace=True)
        # --- End column renames ---

        # New K confidence and bet logic
        def calculate_k_bet_and_confidence(row, inning_num):
            pitch_venue_k_rate = row['VENUE PITCH K RATE %']
            pitch_overall_k_rate = row['PITCH K OVERALL RATE %']
            opp_venue_k_rate = row['VENUE OPP K RATE %']
            opp_overall_k_rate = row['OPP K OVERALL RATE %']

            # Calculate PITCHER K BET
            if pitch_venue_k_rate >= 80 and pitch_overall_k_rate >= 80:
                pitcher_k_bet = "HIGH OVER"
            elif pitch_venue_k_rate >= 80 or pitch_overall_k_rate >= 80:
                pitcher_k_bet = "OVER"
            else:
                # Fallback to existing logic for Under/Neutral if not Over
                if row['PITCH K CONF'] == "Low": # Assuming 'Low' pitcher K conf means 'Under'
                    pitcher_k_bet = "Under K Bet"
                else:
                    pitcher_k_bet = "Neutral"

            # Calculate OPPONENT K BET
            if opp_venue_k_rate >= 80 and opp_overall_k_rate >= 80:
                opponent_k_bet = "HIGH OVER"
            elif opp_venue_k_rate >= 80 or opp_overall_k_rate >= 80:
                opponent_k_bet = "OVER"
            else:
                # Fallback to existing logic for Under/Neutral if not Over
                if row['OPP K CONF'] == "Low": # Assuming 'Low' opponent K conf means 'Under'
                    opponent_k_bet = "Under K Bet"
                else:
                    opponent_k_bet = "Neutral"

            # Calculate Overall K CONFIDENCE
            k_rate_high_80_count = 0
            if pitch_venue_k_rate >= 80: k_rate_high_80_count += 1
            if pitch_overall_k_rate >= 80: k_rate_high_80_count += 1
            if opp_venue_k_rate >= 80: k_rate_high_80_count += 1
            if opp_overall_k_rate >= 80: k_rate_high_80_count += 1

            if k_rate_high_80_count == 4:
                overall_k_confidence = "HIGH OVER"
            elif k_rate_high_80_count >= 2:
                overall_k_confidence = "High"
            else:
                # Existing logic for Moderate/Low if not High/High Over
                if row['PITCH K CONF'] == "High" and row['OPP K CONF'] == "High":
                    overall_k_confidence = "High"
                elif row['PITCH K CONF'] == "Low" and row['OPP K CONF'] == "Low":
                    overall_k_confidence = "Low"
                else:
                    overall_k_confidence = "Moderate"

            return pitcher_k_bet, opponent_k_bet, overall_k_confidence

        # First, calculate PITCH K CONF and OPP K CONF based on original thresholds for fallback
        report_df['PITCH K CONF'] = report_df['PITCH K OVERALL RATE %'].apply(lambda x: "High" if x >= 25 else "Moderate" if x >= 15 else "Low")
        report_df['OPP K CONF'] = report_df['OPP K OVERALL RATE %'].apply(lambda x: "High" if x >= 25 else "Low" if x <= 15 else "Moderate")

        # Apply the new combined function for K bets and overall confidence
        report_df[['PITCHER K BET', 'OPPONENT K BET', 'Overall K CONFIDENCE']] = report_df.apply(
            lambda row: calculate_k_bet_and_confidence(row, inning_number), axis=1, result_type='expand'
        )


        # Renamed % GMS 0 RUNS INNING {inning_number} to PITCH NRFI % INNING {inning_number}
        report_df.rename(columns={f'% GMS 0 RUNS INNING {inning_number}': f'PITCH NRFI % INNING {inning_number}'}, inplace=True)

        report_df[f'PITCH NRFI CONF'] = report_df[f'PITCH NRFI % INNING {inning_number}'].apply(
            lambda x: "High" if x >= 75
            else "Moderate" if 50 <= x <= 74
            else "Low"
        )


        report_df['OPP R/G CONF'] = report_df['OPP R/G'].apply(
            lambda x: "Low" if x <= 0.35  
            else "Moderate" if 0.36 <= x <= 0.65
            else "High"  
        )

        report_df['PITCH RUNS ALLOWED CONF'] = report_df['PITCH RUNS ALLOWED PER GAME %'].apply(
            lambda x: "High" if x <= 25.0
            else "Moderate" if x <= 50.0
            else "Low"
        )

        report_df['Overall Run Prevention Confidence'] = report_df.apply(
            lambda row: "High" if row['PITCH RUNS ALLOWED PER GAME %'] < 50.0 and row[f'PITCH NRFI % INNING {inning_number}'] > 70 and row['OPP R/G'] < 0.4
            else "Low" if row['PITCH RUNS ALLOWED PER GAME %'] > 100.0 and row[f'PITCH NRFI % INNING {inning_number}'] < 50 and row['OPP R/G'] > 0.7
            else "Moderate", axis=1
        )
        
        # Calculate NRFI/YRFI percentages for use in confidence calculation, even if not displayed
        # Use the NRFI percentages that were added to report_df in analyze_games
        # Using .mean() here as a simple aggregation across all rows in the report_df
        # for the overall confidence calculation.
        pitcher_nrfi_pct_overall = report_df[f'PITCHER VENUE NRFI % INNING {inning_number}'].mean()
        opponent_nrfi_pct_overall = report_df[f'OPPONENT VENUE NRFI % INNING {inning_number}'].mean()

        report_df['Overall CONFIDENCE FOR NRFI AND YRFI'] = report_df.apply(
            lambda row: calculate_overall_nrfi_yrfi_confidence(
                row[f'PITCHER VENUE NRFI % INNING {inning_number}'], # Use row-specific venue NRFI
                row[f'OPPONENT VENUE NRFI % INNING {inning_number}'], # Use row-specific venue NRFI
                row['Overall Run Prevention Confidence']
            ), axis=1
        )

        report_df['PITCHER RUNS BET'] = report_df['Overall CONFIDENCE FOR NRFI AND YRFI'].apply(
            lambda x: "Under Runs Bet" if x == "High (NRFI)" else "Over Runs Bet" if x == "High (YRFI)" else "Neutral"
        )
        report_df['OPPONENT RUNS BET'] = report_df['Overall CONFIDENCE FOR NRFI AND YRFI'].apply(
            lambda x: "Over Runs Bet" if x == "High (YRFI)" else "Under Runs Bet" if x == "High (NRFI)" else "Neutral" # Corrected logic
        )


        # Hits specific confidence calculations
        report_df['PITCH NRHI CONF'] = report_df[f'PITCH NRHI % INNING {inning_number}'].apply(
            lambda x: "High" if x >= 75 else "Moderate" if 50 <= x <= 74 else "Low"
        )
        report_df['PITCHER VENUE NRHI CONF'] = report_df[f'PITCHER VENUE NRHI % INNING {inning_number}'].apply(
            lambda x: "High" if x >= 75 else "Moderate" if 50 <= x <= 74 else "Low"
        )

        report_df['BAT NRHI CONF'] = report_df[f'BAT NRHI % INNING {inning_number}'].apply(
            lambda x: "High" if x >= 75 else "Moderate" if 50 <= x <= 74 else "Low"
        )
        report_df['BATTER VENUE NRHI CONF'] = report_df[f'BATTER VENUE NRHI % INNING {inning_number}'].apply(
            lambda x: "High" if x >= 75 else "Moderate" if 50 <= x <= 74 else "Low"
        )
        
        # Overall NRHI Confidence (similar to NRFI)
        report_df['Overall HITS CONFIDENCE'] = report_df.apply(
            lambda row: calculate_overall_nrhi_confidence(
                row[f'PITCH NRHI % INNING {inning_number}'],
                row[f'BAT NRHI % INNING {inning_number}']
            ), axis=1
        )

        # Original Hits Allowed/Batting Avg (still used for some calculations/display)
        report_df['PITCH HITS ALLOWED CONF'] = report_df['PITCH HIT AVG #'].apply(lambda x: "High" if x <= 0.5 else "Moderate" if x <= 1.0 else "Low")
        report_df['OPP HITS BATTING CONF'] = report_df['BAT HIT AVG #'].apply(lambda x: "Low" if x <= 0.5 else "Moderate" if x <= 1.0 else "High")

        # Walks specific confidence calculations
        report_df['PITCH WALKS ALLOWED CONF'] = report_df['PITCH WALKS ALLOWED PER GAME %'].apply(
            lambda x: calculate_confidence_level(x, {'high': 8.0, 'moderate': 12.0}, 'less')
        )
        report_df['OPP WALKS BATTING CONF'] = report_df['OPP WALKS BATTING/GM'].apply(
            lambda x: calculate_confidence_level(x, {'high': 0.5, 'moderate': 0.3}, 'greater')
        )

        # Singles specific confidence calculations
        report_df['PITCH SINGLES ALLOWED CONF'] = report_df['PITCH SINGLES ALLOWED/GM'].apply(
            lambda x: calculate_confidence_level(x, {'high': 0.3, 'moderate': 0.6}, 'less')
        )
        report_df['OPP SINGLES BATTING CONF'] = report_df['OPP SINGLES BATTING/GM'].apply(
            lambda x: calculate_confidence_level(x, {'high': 0.6, 'moderate': 0.3}, 'greater')
        )

        # Doubles specific confidence calculations
        report_df['PITCH DOUBLES ALLOWED CONF'] = report_df['PITCH DOUBLES ALLOWED/GM'].apply(
            lambda x: calculate_confidence_level(x, {'high': 0.05, 'moderate': 0.15}, 'less')
        )
        report_df['OPP DOUBLES BATTING CONF'] = report_df['OPP DOUBLES BATTING/GM'].apply(
            lambda x: calculate_confidence_level(x, {'high': 0.15, 'moderate': 0.05}, 'greater')
        )

        # Triples specific confidence calculations
        report_df['PITCH TRIPLES ALLOWED CONF'] = report_df['PITCH TRIPLES ALLOWED/GM'].apply(
            lambda x: calculate_confidence_level(x, {'high': 0.01, 'moderate': 0.03}, 'less')
        )
        report_df['OPP TRIPLES BATTING CONF'] = report_df['OPP TRIPLES BATTING/GM'].apply(
            lambda x: calculate_confidence_level(x, {'high': 0.03, 'moderate': 0.01}, 'greater')
        )

        # Homers specific confidence calculations
        report_df['PITCH HOMERS ALLOWED CONF'] = report_df['PITCH HOMERS ALLOWED/GM'].apply(
            lambda x: calculate_confidence_level(x, {'high': 0.05, 'moderate': 0.15}, 'less')
        )
        report_df['OPP HOMERS BATTING CONF'] = report_df['OPP HOMERS BATTING/GM'].apply(
            lambda x: calculate_confidence_level(x, {'high': 0.15, 'moderate': 0.05}, 'greater')
        )

        # Total Bases specific confidence calculations
        report_df['PITCH TOTAL BASES ALLOWED CONF'] = report_df['PITCH TOTAL BASES ALLOWED/GM'].apply(
            lambda x: calculate_confidence_level(x, {'high': 1.0, 'moderate': 2.0}, 'less')
        )
        report_df['OPP TOTAL BASES BATTING CONF'] = report_df['OPP TOTAL BASES BATTING/GM'].apply(
            lambda x: calculate_confidence_level(x, {'high': 2.0, 'moderate': 1.0}, 'greater')
        )


        report_df['Overall WALKS CONFIDENCE'] = report_df.apply(lambda row: calculate_overall_over_under_confidence(row['PITCH WALKS ALLOWED CONF'], row['OPP WALKS BATTING CONF']), axis=1)
        report_df['Overall SINGLES CONFIDENCE'] = report_df.apply(lambda row: calculate_overall_over_under_confidence(row['PITCH SINGLES ALLOWED CONF'], row['OPP SINGLES BATTING CONF']), axis=1)
        report_df['Overall DOUBLES CONFIDENCE'] = report_df.apply(lambda row: calculate_overall_over_under_confidence(row['PITCH DOUBLES ALLOWED CONF'], row['OPP DOUBLES BATTING CONF']), axis=1)
        report_df['Overall TRIPLES CONFIDENCE'] = report_df.apply(lambda row: calculate_overall_over_under_confidence(row['PITCH TRIPLES ALLOWED CONF'], row['OPP TRIPLES BATTING CONF']), axis=1)
        report_df['Overall HOMERS CONFIDENCE'] = report_df.apply(lambda row: calculate_overall_over_under_confidence(row['PITCH HOMERS ALLOWED CONF'], row['OPP HOMERS BATTING CONF']), axis=1)
        report_df['Overall TOTAL BASES CONFIDENCE'] = report_df.apply(lambda row: calculate_overall_over_under_confidence(row['PITCH TOTAL BASES ALLOWED CONF'], row['OPP TOTAL BASES BATTING CONF']), axis=1)

        report_df['PITCHER HITS BET'] = report_df['Overall HITS CONFIDENCE'].apply(
            lambda x: "Under Hits Bet" if x == "High (Under)" else "Over Hits Bet" if x == "High (Over)" else "Neutral"
        )
        report_df['OPPONENT HITS BET'] = report_df['Overall HITS CONFIDENCE'].apply( # NEW
            lambda x: "Over Hits Bet" if x == "High (Over)" else "Under Hits Bet" if x == "High (Under)" else "Neutral"
        )
        report_df['PITCHER WALKS BET'] = report_df['Overall WALKS CONFIDENCE'].apply(
            lambda x: "Under Walks Bet" if x == "High (Under)" else "Over Walks Bet" if x == "High (Over)" else "Neutral"
        )
        report_df['OPPONENT WALKS BET'] = report_df['Overall WALKS CONFIDENCE'].apply( # NEW
            lambda x: "Over Walks Bet" if x == "High (Over)" else "Under Walks Bet" if x == "High (Under)" else "Neutral"
        )
        report_df['PITCHER SINGLES BET'] = report_df['Overall SINGLES CONFIDENCE'].apply(
            lambda x: "Under Singles Bet" if x == "High (Under)" else "Over Singles Bet" if x == "High (Over)" else "Neutral"
        )
        report_df['OPPONENT SINGLES BET'] = report_df['Overall SINGLES CONFIDENCE'].apply( # NEW
            lambda x: "Over Singles Bet" if x == "High (Over)" else "Under Singles Bet" if x == "High (Under)" else "Neutral"
        )
        report_df['PITCHER DOUBLES BET'] = report_df['Overall DOUBLES CONFIDENCE'].apply(
            lambda x: "Under Doubles Bet" if x == "High (Under)" else "Over Doubles Bet" if x == "High (Over)" else "Neutral"
        )
        report_df['OPPONENT DOUBLES BET'] = report_df['Overall DOUBLES CONFIDENCE'].apply( # NEW
            lambda x: "Over Doubles Bet" if x == "High (Over)" else "Under Doubles Bet" if x == "High (Under)" else "Neutral"
        )
        report_df['PITCHER TRIPLES BET'] = report_df['Overall TRIPLES CONFIDENCE'].apply(
            lambda x: "Under Triples Bet" if x == "High (Under)" else "Over Triples Bet" if x == "High (Over)" else "Neutral"
        )
        report_df['OPPONENT TRIPLES BET'] = report_df['Overall TRIPLES CONFIDENCE'].apply( # NEW
            lambda x: "Over Triples Bet" if x == "High (Over)" else "Under Triples Bet" if x == "High (Under)" else "Neutral"
        )
        report_df['PITCHER HOMERS BET'] = report_df['Overall HOMERS CONFIDENCE'].apply(
            lambda x: "Under Homers Bet" if x == "High (Under)" else "Over Homers Bet" if x == "High (Over)" else "Neutral"
        )
        report_df['OPPONENT HOMERS BET'] = report_df['Overall HOMERS CONFIDENCE'].apply( # NEW
            lambda x: "Over Homers Bet" if x == "High (Over)" else "Under Homers Bet" if x == "High (Under)" else "Neutral"
        )
        report_df['PITCHER TOTAL BASES BET'] = report_df['Overall TOTAL BASES CONFIDENCE'].apply(
            lambda x: "Under Total Bases Bet" if x == "High (Under)" else "Over Total Bases Bet" if x == "High (Over)" else "Neutral"
        )
        report_df['OPPONENT TOTAL BASES BET'] = report_df['Overall TOTAL BASES CONFIDENCE'].apply( # NEW
            lambda x: "Over Total Bases Bet" if x == "High (Over)" else "Under Total Bases Bet" if x == "High (Under)" else "Neutral"
        )


        top_k = report_df.sort_values(by=['Overall K CONFIDENCE', 'PITCH K OVERALL RATE %'], key=lambda x: x.map(confidence_map) if x.name == 'Overall K CONFIDENCE' else x, ascending=[False, False]).head(4) # Updated column name
        
        top_run_prevention = report_df.sort_values(by=['Overall Run Prevention Confidence', 'PITCH RUNS ALLOWED PER GAME %'], key=lambda x: x.map(confidence_map) if x.name == 'Overall Run Prevention Confidence' else x, ascending=[False, True]).head(4)[['Game', 'Pitcher', 'Opponent', 'Overall Run Prevention Confidence', 'PITCH RUNS ALLOWED PER GAME %', f'TODAY PITCHER RUNS ALLOWED INNING {inning_number}', 'OPP R/G', f'PITCH NRFI % INNING {inning_number}']] # Updated column name
        
        # Note: NRFI/YRFI columns are no longer in report_df, so they are not included in top_nrfi/top_yrfi display.
        top_nrfi = report_df.sort_values(by=['Overall CONFIDENCE FOR NRFI AND YRFI'], key=lambda x: x.map(nrfi_yrfi_map) if x.name == 'Overall CONFIDENCE FOR NRFI AND YRFI' else x, ascending=[False]).head(4)
        top_yrfi = report_df[report_df['Overall CONFIDENCE FOR NRFI AND YRFI'].isin(['High (YRFI)', 'Moderate (leaning YRFI)'])].sort_values(by=['Overall CONFIDENCE FOR NRFI AND YRFI'], key=lambda x: x.map(nrfi_yrfi_map) if x.name == 'Overall CONFIDENCE FOR NRFI AND YRFI' else x, ascending=[False]).head(4)


        strikeout_recommendations = {
            "Top 4 Strikeout": top_k[['Game', 'Pitcher', 'Opponent', 'Overall K CONFIDENCE', 'PITCH K OVERALL RATE %', 'PITCHER K BET', 'OPPONENT K BET']].to_dict('records') # Updated column name, added OPPONENT K BET
        }


        runs_recommendations = {
            "Top 4 Run Prevention": top_run_prevention.to_dict('records'),
            "Top 4 NRFI": top_nrfi[['Game', 'Pitcher', 'Opponent', 'Overall CONFIDENCE FOR NRFI AND YRFI', 'PITCHER RUNS BET', 'OPPONENT RUNS BET']].to_dict('records'),
            "Top 4 YRFI": top_yrfi[['Game', 'Pitcher', 'Opponent', 'Overall CONFIDENCE FOR NRFI AND YRFI', 'PITCHER RUNS BET', 'OPPONENT RUNS BET']].to_dict('records')
        }

        # Hits recommendations, now using NRHI
        top_nrhi = report_df[report_df['Overall HITS CONFIDENCE'].isin(['High (Under)', 'Moderate (leaning Under)'])].sort_values(by=['Overall HITS CONFIDENCE'], key=lambda x: x.map(over_under_map) if x.name == 'Overall HITS CONFIDENCE' else x, ascending=[False]).head(4)
        top_hits_over = report_df[report_df['Overall HITS CONFIDENCE'].isin(['High (Over)', 'Moderate (leaning Over)'])].sort_values(by=['Overall HITS CONFIDENCE'], key=lambda x: x.map(over_under_map) if x.name == 'Overall HITS CONFIDENCE' else x, ascending=[False]).head(4)


        other_metrics_recs = {}
        # Updated over_under_metrics_config to reflect new column names for pitcher_metric_col and opponent_metric_col
        over_under_metrics_config = [
            {'name': 'Hits', 'overall_conf_col': 'Overall HITS CONFIDENCE', 'pitcher_metric_col': f'PITCH NRHI % INNING {inning_number}', 'opponent_metric_col': f'BAT NRHI % INNING {inning_number}', 'bet_col': 'PITCHER HITS BET', 'opponent_bet_col': 'OPPONENT HITS BET'}, # Updated to use NRHI, added opponent_bet_col
            {'name': 'Walks', 'overall_conf_col': 'Overall WALKS CONFIDENCE', 'pitcher_metric_col': 'PITCH WALKS ALLOWED PER GAME %', 'opponent_metric_col': 'OPP WALKS BATTING/GM', 'bet_col': 'PITCHER WALKS BET', 'opponent_bet_col': 'OPPONENT WALKS BET'}, # Updated to use the remaining pitcher column, added opponent_bet_col
            {'name': 'Singles', 'overall_conf_col': 'Overall SINGLES CONFIDENCE', 'pitcher_metric_col': 'PITCH SINGLES ALLOWED/GM', 'opponent_metric_col': 'OPP SINGLES BATTING/GM', 'bet_col': 'PITCHER SINGLES BET', 'opponent_bet_col': 'OPPONENT SINGLES BET'}, # Updated, added opponent_bet_col
            {'name': 'Doubles', 'overall_conf_col': 'Overall DOUBLES CONFIDENCE', 'pitcher_metric_col': 'PITCH DOUBLES ALLOWED/GM', 'opponent_metric_col': 'OPP DOUBLES BATTING/GM', 'bet_col': 'PITCHER DOUBLES BET', 'opponent_bet_col': 'OPPONENT DOUBLES BET'}, # Updated, added opponent_bet_col
            {'name': 'Triples', 'overall_conf_col': 'Overall TRIPLES CONFIDENCE', 'pitcher_metric_col': 'PITCH TRIPLES ALLOWED/GM', 'opponent_metric_col': 'OPP TRIPLES BATTING/GM', 'bet_col': 'PITCHER TRIPLES BET', 'opponent_bet_col': 'OPPONENT TRIPLES BET'}, # Updated, added opponent_bet_col
            {'name': 'Homers', 'overall_conf_col': 'Overall HOMERS CONFIDENCE', 'pitcher_metric_col': 'PITCH HOMERS ALLOWED/GM', 'opponent_metric_col': 'OPP HOMERS BATTING/GM', 'bet_col': 'PITCHER HOMERS BET', 'opponent_bet_col': 'OPPONENT HOMERS BET'}, # Updated, added opponent_bet_col
            {'name': 'Total Bases', 'overall_conf_col': 'Overall TOTAL BASES CONFIDENCE', 'pitcher_metric_col': 'PITCH TOTAL BASES ALLOWED/GM', 'opponent_metric_col': 'OPP TOTAL BASES BATTING/GM', 'bet_col': 'PITCHER TOTAL BASES BET', 'opponent_bet_col': 'OPPONENT TOTAL BASES BET'}, # Updated, added opponent_bet_col
        ]


        for metric_info in over_under_metrics_config:
            metric_name = metric_info['name']
            overall_col = metric_info['overall_conf_col']
            pitcher_col = metric_info['pitcher_metric_col'] # Defined here
            opponent_col = metric_info['opponent_metric_col'] # Defined here
            bet_col = metric_info['bet_col'] # Defined here
            opponent_bet_col = metric_info['opponent_bet_col'] # Defined here

            if metric_name == 'Hits': # Special handling for Hits due to NRHI
                other_metrics_recs[f"Top 4 {metric_name} Under"] = top_nrhi[['Game', 'Pitcher', 'Opponent', overall_col, pitcher_col, opponent_col, bet_col, opponent_bet_col]].to_dict('records')
                other_metrics_recs[f"Top 4 {metric_name} Over"] = top_hits_over[['Game', 'Pitcher', 'Opponent', overall_col, pitcher_col, opponent_col, bet_col, opponent_bet_col]].to_dict('records')
            else:
                top_under = report_df[report_df[overall_col].isin(['High (Under)'])] \
                    .sort_values(by=[overall_col, pitcher_col, opponent_col],
                                 key=lambda x: x.map(over_under_map) if x.name == overall_col else x,
                                 ascending=[False, True, True]).head(4) 
                other_metrics_recs[f"Top 4 {metric_name} Under"] = top_under[['Game', 'Pitcher', 'Opponent', overall_col, pitcher_col, opponent_col, bet_col, opponent_bet_col]].to_dict('records')


                top_over = report_df[report_df[overall_col].isin(['High (Over)'])] \
                    .sort_values(by=[overall_col, pitcher_col, opponent_col],
                                 key=lambda x: x.map(over_under_map) if x.name == overall_col else x,
                                 ascending=[False, False, False]).head(4) 
                other_metrics_recs[f"Top 4 {metric_name} Over"] = top_over[['Game', 'Pitcher', 'Opponent', overall_col, pitcher_col, opponent_col, bet_col, opponent_bet_col]].to_dict('records')


        parlays = generate_ranked_parlays(report_df)
        strikeout_parlays = {"Strikeout Parlays": parlays.get("Strikeout Parlays", [])}
        runs_parlays = {
            "NRFI Parlays": parlays.get("NRFI Parlays", []),
            "YRFI Parlays": parlays.get("YRFI Parlays", []) 
        }


        other_metrics_parlays = {}
        for metric in over_under_metrics_config:
            metric_name = metric['name']
            other_metrics_parlays[f"{metric_name} Under Parlays"] = parlays.get(f"{metric_name} Under Parlays", [])
            other_metrics_parlays[f"{metric_name} Over Parlays"] = parlays.get(f"{metric_name} Over Parlays", [])


        all_recs_for_inning = {
            'strikeout_recs_gen': strikeout_recommendations,
            'runs_recs_gen': runs_recommendations,
            'other_metrics_recs_gen': other_metrics_recs
        }
        all_parlays_for_inning = {
            'strikeout_parlays_gen': strikeout_parlays,
            'runs_parlays_gen': runs_parlays,
            'other_metrics_parlays_gen': other_metrics_parlays
        }

        actual_report_date_for_pdf = datetime.now().strftime('%Y-%m-%d') \
            if report_date.lower() == 'today' else report_date

        REPORT_METRICS_CONFIG_FOR_INNING = get_report_metrics_config_for_inning(inning_number)

        pdf_buffers = []

        # Generate individual metric reports (in-memory)
        for metric_cfg in REPORT_METRICS_CONFIG_FOR_INNING:
            current_recs_for_pdf = {}
            current_parlays_for_pdf = {}

            metric_name_lower = metric_cfg['name'].lower()
            if metric_name_lower == 'strikeouts':
                current_recs_for_pdf = strikeout_recommendations
                current_parlays_for_pdf = strikeout_parlays
            elif metric_name_lower == 'runs':
                current_recs_for_pdf = runs_recommendations
                current_parlays_for_pdf = runs_parlays
            else: # For Hits, Walks, Singles, Doubles, Triples, Homers, Total Bases
                filtered_recs = {}
                for rec_type, rec_list in other_metrics_recs.items():
                    if metric_name_lower in rec_type.lower():
                        filtered_recs[rec_type] = rec_list
                current_recs_for_pdf = filtered_recs

                filtered_parlays = {}
                for parlay_type, parlay_list in other_metrics_parlays.items():
                    if metric_name_lower in parlay_type.lower():
                        filtered_parlays[parlay_type] = parlay_list
                current_parlays_for_pdf = filtered_parlays
            
            pdf_buffer = generate_individual_metric_pdf_in_memory(
                report_df.copy(),
                metric_cfg,
                actual_report_date_for_pdf,
                current_recs_for_pdf,
                current_parlays_for_pdf,
                inning_number=inning_number
            )
            if pdf_buffer:
                pdf_buffers.append({
                    'name': f"{metric_cfg['filename_suffix']}_report_{actual_report_date_for_pdf}.pdf",
                    'buffer': pdf_buffer
                })

        # Generate consolidated report (in-memory)
        consolidated_pdf_buffer = generate_consolidated_inning_pdf_in_memory(
            report_df.copy(),
            REPORT_METRICS_CONFIG_FOR_INNING, # Pass the full config list
            actual_report_date_for_pdf,
            all_recs_for_inning, # Pass consolidated recommendations
            all_parlays_for_inning, # Pass consolidated parlays
            inning_number=inning_number
        )
        if consolidated_pdf_buffer:
            pdf_buffers.append({
                'name': f"inning_{inning_number}_consolidated_report_{actual_report_date_for_pdf}.pdf",
                'buffer': consolidated_pdf_buffer
            })

        return report_df, strikeout_recommendations, runs_recommendations, strikeout_parlays, runs_parlays, other_metrics_recs, other_metrics_parlays, pdf_buffers

    else:
        print(f"No data available to generate the report for Inning {inning_number} on {report_date}.")
        return pd.DataFrame(), {}, {}, {}, {}, {}, {}, [] # Return empty dataframes/dicts and empty pdf_paths


def apply_confidence_highlight(df, col_name, style_list, is_inverse=False):
    if col_name not in df.columns: return
    col_idx = df.columns.get_loc(col_name)
    for i, row_val in enumerate(df[col_name]):
        row_index = i + 1
        confidence = str(row_val)
        if 'High' in confidence: 
            style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.lightgreen if not is_inverse else colors.salmon))
        elif 'Moderate' in confidence: 
            style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.lightyellow))
        elif 'Low' in confidence:
            style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.salmon if not is_inverse else colors.lightgreen))

def apply_bet_recommendation_highlight(df, col_name, style_list):
    """
    Applies highlighting based on 'Under/Over Bet' recommendations.
    Green for 'Under Bet', Salmon for 'Over Bet', Light Yellow for 'Neutral'.
    """
    if col_name not in df.columns: return
    col_idx = df.columns.get_loc(col_name)
    for i, row_val in enumerate(df[col_name]):
        row_index = i + 1
        bet_recommendation = str(row_val)
        
        if 'UNDER' in bet_recommendation.upper():
            style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.lightgreen))
        elif 'OVER' in bet_recommendation.upper():
            style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.salmon))
        else: # Neutral
            style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.lightyellow))

def apply_bet_recommendation_k_highlight(df, col_name, style_list):
    """
    Applies highlighting for Strikeout 'Bet' recommendations.
    Green for 'OVER' and 'HIGH OVER', Salmon for 'Under K Bet', Light Yellow for 'Neutral'.
    """
    if col_name not in df.columns: return
    col_idx = df.columns.get_loc(col_name)
    for i, row_val in enumerate(df[col_name]):
        row_index = i + 1
        bet_recommendation = str(row_val).upper() # Convert to uppercase for case-insensitive comparison
        
        if 'OVER' in bet_recommendation: # Includes "OVER" and "HIGH OVER"
            style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.lightgreen))
        elif 'UNDER' in bet_recommendation: # "Under K Bet"
            style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.salmon))
        else: # Neutral
            style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.lightyellow))

def apply_nrfi_highlight(df, col_name, style_list):
    """
    Applies highlighting to the NRFI % column based on specified thresholds.
    Green: > 80%
    Yellow: >= 65% and <= 79%
    Salmon (Red): < 50%
    """
    if col_name not in df.columns: return
    col_idx = df.columns.get_loc(col_name)
    for i, row_val in enumerate(df[col_name]):
        row_index = i + 1
        try:
            nrfi_pct = float(row_val)
            if nrfi_pct > 80:
                style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.lightgreen))
            elif 65 <= nrfi_pct <= 79:
                style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.lightyellow))
            elif nrfi_pct < 50:
                style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.salmon))
        except ValueError:
            pass

def apply_today_runs_highlight(df, col_name, style_list):
    """
    Applies highlighting to the 'TODAY PITCHER RUNS ALLOWED' or 'TODAY PITCHER HITS' column.
    Green if value is 0.
    """
    if col_name not in df.columns: return
    col_idx = df.columns.get_loc(col_name)
    for i, row_val in enumerate(df[col_name]):
        row_index = i + 1
        try:
            value = float(row_val)
            if value == 0:
                style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.lightgreen))
        except ValueError:
            pass

def apply_zero_value_highlight(df, col_name, style_list):
    """
    Applies highlighting to a column if its numeric value is 0.
    Green if value is 0.
    """
    if col_name not in df.columns: return
    col_idx = df.columns.get_loc(col_name)
    for i, row_val in enumerate(df[col_name]):
        row_index = i + 1
        try:
            value = float(row_val)
            if value == 0:
                style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.lightgreen))
        except ValueError:
            pass

def apply_positive_value_highlight(df, col_name, style_list):
    """
    Applies highlighting to a column if its numeric value is greater than 0.
    Green if value > 0.
    """
    if col_name not in df.columns: return
    col_idx = df.columns.get_loc(col_name)
    for i, row_val in enumerate(df[col_name]):
        row_index = i + 1
        try:
            value = float(row_val)
            if value > 0:
                style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.lightgreen))
        except ValueError:
            pass

def apply_percentage_range_highlight(df, col_name, style_list):
    """
    Applies highlighting based on percentage ranges:
    >= 80: Green
    70-79: Blue
    """
    if col_name not in df.columns: return
    col_idx = df.columns.get_loc(col_name)
    for i, row_val in enumerate(df[col_name]):
        row_index = i + 1
        try:
            value = float(row_val)
            if value >= 80:
                style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.lightgreen))
            elif 70 <= value <= 79:
                style_list.append(('BACKGROUND', (col_idx, row_index), (col_idx, row_index), colors.lightblue))
        except ValueError:
            pass


def apply_top_bottom_highlight(display_df, full_df, col_name, style_list, n=3, ascending=False):
    if col_name not in display_df.columns or col_name not in full_df.columns: return
    
    col_idx = display_df.columns.get_loc(col_name)
    
    try:
        numeric_col_full = pd.to_numeric(full_df[col_name], errors='coerce')
        numeric_col_display = pd.to_numeric(display_df[col_name], errors='coerce')
    except Exception as e:
        print(f"Error converting column {col_name} to numeric for highlighting: {e}")
        return

    temp_full_df = pd.DataFrame({'metric': numeric_col_full, 'original_index': full_df.index})
    
    sorted_full_df = temp_full_df.sort_values(by='metric', ascending=ascending, na_position='last')


    for i in range(min(n, len(sorted_full_df))):
        original_row_idx_val = sorted_full_df.iloc[i]['original_index']
        if original_row_idx_val in display_df.index:
            row_index_in_display_df = display_df.index.get_loc(original_row_idx_val) + 1 
            
            color_to_apply = colors.lightgrey
            if i == 0: color_to_apply = colors.lightgreen
            elif i == 1: color_to_apply = colors.lightblue
            elif i == 2: color_to_apply = colors.lightyellow
            style_list.append(('BACKGROUND', (col_idx, row_index_in_display_df), (col_idx, row_index_in_display_df), color_to_apply))


    inverse_ascending = not ascending
    sorted_full_df_inverse = temp_full_df.sort_values(by='metric', ascending=inverse_ascending, na_position='last')
    for i in range(min(n, len(sorted_full_df_inverse))):
        original_row_idx_val = sorted_full_df_inverse.iloc[i]['original_index']
        is_already_good = False
        for j in range(min(n, len(sorted_full_df))):
            if sorted_full_df.iloc[j]['original_index'] == original_row_idx_val:
                is_already_good = True
                break
        
        if not is_already_good and original_row_idx_val in display_df.index:
            row_index_in_display_df = display_df.index.get_loc(original_row_idx_val) + 1
            style_list.append(('BACKGROUND', (col_idx, row_index_in_display_df), (col_idx, row_index_in_display_df), colors.salmon))


def generate_individual_metric_pdf_in_memory(report_df, metric_cfg, report_date, recommendations, parlays, inning_number: int):
    """
    Generates a single PDF report for an individual metric with its recommendations and parlays,
    returning the PDF as a BytesIO object.
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(letter), topMargin=0.5*inch, bottomMargin=0.5*inch, leftMargin=0.5*inch, rightMargin=0.5*inch)
    styles = getSampleStyleSheet()
    
    normal_style = ParagraphStyle(name='NormalSmall', parent=styles['Normal'], fontSize=7, fontName='Helvetica', leading=9, alignment=1)
    title_style = ParagraphStyle(name='TitleSmall', parent=styles['h1'], fontSize=12, fontName='Helvetica-Bold', alignment=1)
    h2_style = ParagraphStyle(name='H2Small', parent=styles['h2'], fontSize=10, fontName='Helvetica-Bold', spaceBefore=6, spaceAfter=2, alignment=1)
    h3_style = ParagraphStyle(name='H3Small', parent=styles['h3'], fontSize=9, fontName='Helvetica-Bold', spaceBefore=4, spaceAfter=1, alignment=0) # Left-aligned for sub-sections
    table_header_style = ParagraphStyle(name='TableHeaderSmall', parent=normal_style, fontName='Helvetica-Bold', fontSize=6, alignment=1)

    story = []

    filename_prefix = "probable_pitchers" if report_date == datetime.now().strftime('%Y-%m-%d') else "historical"
    title_text = f"Inning {inning_number} {metric_cfg['title_prefix']} Report ({filename_prefix.replace('_', ' ').title()} - {report_date})"
    story.append(Paragraph(title_text, title_style))
    story.append(Spacer(1, 0.1*inch))

    display_cols = ['Game', 'Pitcher', 'Opponent'] + metric_cfg['pitcher_cols'] + metric_cfg['opponent_cols']
    if metric_cfg.get('overall_conf_col') and metric_cfg['overall_conf_col'] in report_df.columns:
        display_cols.append(metric_cfg['overall_conf_col'])
    if metric_cfg.get('moved_pitcher_today_col') and metric_cfg['moved_pitcher_today_col'] in report_df.columns:
        display_cols.append(metric_cfg['moved_pitcher_today_col'])

    # Ensure all display columns exist in the DataFrame, add as 'N/A' if missing
    for col in display_cols:
        if col not in report_df.columns:
            report_df[col] = "N/A"

    metric_df = report_df[display_cols].copy()

    # Convert numerical columns to appropriate format
    numerical_columns = []
    PITCHER_METRICS_INNING = get_pitcher_metrics_for_inning(inning_number)
    BATTING_METRICS_INNING = get_batting_metrics_for_inning(inning_number)

    for metric in PITCHER_METRICS_INNING:
        if metric.get('report_key_avg') and metric['report_key_avg'] in metric_df.columns: numerical_columns.append(metric['report_key_avg'])
        if metric.get('report_key_rate') and metric['report_key_rate'] in metric_df.columns: numerical_columns.append(metric['report_key_rate'])
        if metric.get('report_key_per_game_pct') and metric['report_key_per_game_pct'] in metric_df.columns: numerical_columns.append(metric['report_key_per_game_pct'])
        if metric.get('last_game_key') and metric['last_game_key'] in metric_df.columns: numerical_columns.append(metric['last_game_key'])

    for metric in BATTING_METRICS_INNING:
        if metric.get('report_key_avg') and metric['report_key_avg'] in metric_df.columns: numerical_columns.append(metric['report_key_avg'])
        if metric.get('report_key_rate') and metric['report_key_rate'] in metric_df.columns: numerical_columns.append(metric['report_key_rate'])
        if metric.get('report_key_per_game_pct') and metric['report_key_per_game_pct'] in metric_df.columns: numerical_columns.append(metric['report_key_per_game_pct'])
        if metric.get('last_game_key') and metric['last_game_key'] in metric_df.columns: numerical_columns.append(metric['last_game_key'])
    
    for col_suffix in [
        f'PITCH NRFI % INNING {inning_number}',
        f'# TOTAL STARTS INNING {inning_number}',
        f'PITCHER VENUE NRFI % INNING {inning_number}',
        f'OPPONENT VENUE NRFI % INNING {inning_number}',
        f'BAT NRFI % INNING {inning_number}',
        'VENUE PITCH HIT AVG',
        'VENUE BAT HIT AVG',
        'VENUE PITCH WALKS ALLOWED/GM',
        'VENUE BAT WALKS BATTING/GM',
        'VENUE PITCH SINGLES ALLOWED/GM',
        'VENUE BAT SINGLES BATTING/GM',
        'VENUE PITCH DOUBLES ALLOWED/GM',
        'VENUE BAT DOUBLES BATTING/GM',
        'VENUE PITCH TRIPLES ALLOWED/GM',
        'VENUE BAT TRIPLES BATTING/GM',
        'VENUE PITCH HOMERS ALLOWED/GM',
        'VENUE BAT HOMERS BATTING/GM',
        'VENUE PITCH TOTAL BASES ALLOWED/GM',
        'VENUE BAT TOTAL BASES BATTING/GM',
        f'PITCHER VENUE NRHI % INNING {inning_number}',
        f'BATTER VENUE NRHI % INNING {inning_number}',
        'VENUE PITCH K RATE %',
        'VENUE OPP K RATE %',
        'PITCH K OVERALL RATE %',
        'OPP K OVERALL RATE %',
    ]:
        if col_suffix in metric_df.columns: numerical_columns.append(col_suffix)
    
    if metric_cfg.get('moved_pitcher_today_col') and metric_cfg['moved_pitcher_today_col'] in metric_df.columns:
        numerical_columns.append(metric_cfg['moved_pitcher_today_col'])

    numerical_columns = list(set(numerical_columns))

    for col in metric_df.columns:
        if col in numerical_columns:
            try:
                if col == f'LAST GAME PITCHER STRIKEOUTS INNING {inning_number}' or col == f'LAST GAME OPPONENT STRIKEOUTS BATTING INNING {inning_number}':
                    metric_df[col] = pd.to_numeric(metric_df[col], errors='coerce').fillna(0).astype(int)
                else:
                    metric_df[col] = pd.to_numeric(metric_df[col], errors='coerce').round(2)
            except Exception as e:
                print(f"Error converting column {col} to numeric for PDF: {e}")
                metric_df[col] = metric_df[col].astype(str)
        else:
            metric_df[col] = metric_df[col].astype(str)

    header_row = [Paragraph(col.replace(f' INNING {inning_number}', ''), table_header_style) for col in metric_df.columns.tolist()]
    table_data_list = [header_row]
    for index, row in metric_df.iterrows():
        row_data = []
        for col_name in metric_df.columns:
            val = row[col_name]
            if isinstance(val, pd.Series):
                val = val.iloc[0] if not val.empty else None

            if pd.isna(val) or val is None:
                row_data.append(Paragraph("N/A", normal_style))
            elif isinstance(val, float):
                if col_name == f'LAST GAME PITCHER STRIKEOUTS INNING {inning_number}' or col_name == f'LAST GAME OPPONENT STRIKEOUTS BATTING INNING {inning_number}':
                    row_data.append(Paragraph(f"{int(val)}", normal_style))
                else:
                    row_data.append(Paragraph(f"{val:.2f}", normal_style))
            else:
                row_data.append(Paragraph(str(val), normal_style))
        table_data_list.append(row_data)

    available_width = landscape(letter)[0] - 1*inch
    num_cols = len(metric_df.columns)
    base_col_width = available_width / num_cols
    
    col_widths = []
    for col_name in metric_df.columns:
        if 'Game' in col_name: col_widths.append(base_col_width * 1.2)
        elif 'Pitcher' in col_name: col_widths.append(base_col_width * 1.3)
        elif 'Opponent' in col_name: col_widths.append(base_col_width * 0.8)
        elif 'CONFIDENCE' in col_name or 'CONF' in col_name: col_widths.append(base_col_width * 1.1)
        elif 'BET' in col_name: col_widths.append(base_col_width * 1.0)
        elif 'NRFI %' in col_name: col_widths.append(base_col_width * 1.0)
        elif 'NRHI %' in col_name: col_widths.append(base_col_width * 1.0)
        elif '# TOTAL STARTS' in col_name: col_widths.append(base_col_width * 0.7)
        elif 'LAST GAME' in col_name: col_widths.append(base_col_width * 1.0)
        elif 'VENUE NRFI %' in col_name: col_widths.append(base_col_width * 1.0)
        elif 'TODAY PITCHER' in col_name: col_widths.append(base_col_width * 1.0)
        elif 'VENUE PITCH' in col_name: col_widths.append(base_col_width * 1.0)
        elif 'VENUE BAT' in col_name: col_widths.append(base_col_width * 1.0)
        else: col_widths.append(base_col_width * 0.9)
    
    total_requested_width = sum(col_widths)
    col_widths = [(w / total_requested_width) * available_width for w in col_widths]

    table_style_cmds = [
        ('BACKGROUND', (0,0), (-1,0), colors.grey),
        ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0,0), (-1,0), 4),
        ('BACKGROUND', (0,1), (-1,-1), colors.beige),
        ('GRID', (0,0), (-1,-1), 0.5, colors.black),
        ('LEFTPADDING', (0,0), (-1,-1), 2),
        ('RIGHTPADDING', (0,0), (-1,-1), 2),
    ]

    for highlight_rule in metric_cfg.get('highlight_cols', []):
        if highlight_rule.get('type') == 'bet_recommendation_k' and highlight_rule['col'] in metric_df.columns:
            apply_bet_recommendation_k_highlight(metric_df, highlight_rule['col'], table_style_cmds)
        elif highlight_rule.get('type') == 'bet_recommendation' and highlight_rule['col'] in metric_df.columns:
            apply_bet_recommendation_highlight(metric_df, highlight_rule['col'], table_style_cmds)
        elif highlight_rule.get('type') == 'today_runs_highlight' and highlight_rule['col'] in metric_df.columns:
            apply_today_runs_highlight(metric_df, highlight_rule['col'], table_style_cmds)
        elif highlight_rule.get('type') == 'nrfi_highlight' and highlight_rule['col'] in metric_df.columns:
            apply_nrfi_highlight(metric_df, highlight_rule['col'], table_style_cmds)
        elif highlight_rule.get('type') == 'zero_value_highlight' and highlight_rule['col'] in metric_df.columns:
            apply_zero_value_highlight(metric_df, highlight_rule['col'], table_style_cmds)
        elif highlight_rule.get('type') == 'positive_value_highlight' and highlight_rule['col'] in metric_df.columns:
            apply_positive_value_highlight(metric_df, highlight_rule['col'], table_style_cmds)
        elif highlight_rule.get('type') == 'percentage_range_highlight' and highlight_rule['col'] in metric_df.columns:
            apply_percentage_range_highlight(metric_df, highlight_rule['col'], table_style_cmds)
        elif 'col_conf' in highlight_rule and highlight_rule['col_conf'] in metric_df.columns:
            apply_confidence_highlight(metric_df, highlight_rule['col_conf'], table_style_cmds, highlight_rule.get('is_inverse', False))
        elif 'col' in highlight_rule and highlight_rule['col'] in metric_df.columns:
            apply_top_bottom_highlight(metric_df, report_df, highlight_rule['col'], table_style_cmds, ascending=highlight_rule['ascending'])

    table = Table(table_data_list, colWidths=col_widths)
    table.setStyle(TableStyle(table_style_cmds))
    story.append(table)
    story.append(Spacer(1, 0.15*inch))

    # --- Recommendations for this metric ---
    if recommendations:
        story.append(Paragraph(f"<b>{metric_cfg['name']} Recommendations:</b>", h3_style))
        for rec_type, rec_list in recommendations.items():
            if rec_list and isinstance(rec_list, list) and rec_list and isinstance(rec_list[0], dict):
                story.append(Paragraph(f"<b>{rec_type}:</b>", normal_style))
                rec_header = [Paragraph(str(col).replace(f' INNING {inning_number}', ''), table_header_style) for col in rec_list[0].keys()]
                rec_data_list = [rec_header]
                for rec in rec_list:
                    row_data = []
                    for k in rec_list[0].keys():
                        val = rec.get(k, "N/A")
                        if isinstance(val, float):
                            row_data.append(Paragraph(f"{val:.2f}", normal_style))
                        else:
                            row_data.append(Paragraph(str(val), normal_style))
                    rec_data_list.append(row_data)
                
                rec_table = Table(rec_data_list)
                rec_table.setStyle(TableStyle([
                    ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
                    ('TEXTCOLOR', (0,0), (-1,0), colors.black),
                    ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                    ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                    ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
                ]))
                story.append(rec_table)
                story.append(Spacer(1, 0.05*inch))
            else:
                story.append(Paragraph(f"<b>{rec_type}:</b> No recommendations available.", normal_style))
                story.append(Spacer(1, 0.05*inch))
        story.append(Spacer(1, 0.1*inch))

    # --- Parlays for this metric ---
    if parlays:
        story.append(Paragraph(f"<b>{metric_cfg['name']} Parlays:</b>", h3_style))
        found_parlays_for_metric = False
        for parlay_type, parlay_list in parlays.items():
            if parlay_list and isinstance(parlay_list, list): # No need to filter by metric_cfg['name'].lower() as parlays are already specific
                found_parlays_for_metric = True
                story.append(Paragraph(f"<b>{parlay_type}:</b>", normal_style))
                parlay_data_list = [[Paragraph('Games', table_header_style), Paragraph('Score', table_header_style)]]
                for parlay in parlay_list:
                    parlay_data_list.append([Paragraph(parlay['games'], normal_style), Paragraph(f"{parlay.get('score', 0):.2f}", normal_style)])
                
                parlay_table = Table(parlay_data_list, colWidths=[available_width*0.8, available_width*0.18])
                parlay_table.setStyle(TableStyle([
                    ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
                    ('TEXTCOLOR', (0,0), (-1,0), colors.black),
                    ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                    ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                    ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
                ]))
                story.append(parlay_table)
                story.append(Spacer(1, 0.05*inch))
        if not found_parlays_for_metric:
            story.append(Paragraph("No relevant parlays available for this metric.", normal_style))
            story.append(Spacer(1, 0.05*inch))
    story.append(Spacer(1, 0.2*inch))

    try:
        doc.build(story)
        buffer.seek(0) # Rewind the buffer to the beginning
        return buffer
    except Exception as e:
        logging.error(f"Error generating individual PDF for Inning {inning_number} {metric_cfg['name']} report: {e}", exc_info=True)
        print(f"Error generating individual PDF for Inning {inning_number} {metric_cfg['name']} report. Check logs for details.")
        return None


def generate_consolidated_inning_pdf_in_memory(report_df, all_metrics_config, report_date, all_recommendations, all_parlays, inning_number: int):
    """
    Generates a single PDF report for a specific inning, containing all metric recommendations and parlays,
    returning the PDF as a BytesIO object.
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(letter), topMargin=0.5*inch, bottomMargin=0.5*inch, leftMargin=0.5*inch, rightMargin=0.5*inch)
    styles = getSampleStyleSheet()
    
    normal_style = ParagraphStyle(name='NormalSmall', parent=styles['Normal'], fontSize=7, fontName='Helvetica', leading=9,alignment=1)
    title_style = ParagraphStyle(name='TitleSmall', parent=styles['h1'], fontSize=12, fontName='Helvetica-Bold', alignment=1)
    h2_style = ParagraphStyle(name='H2Small', parent=styles['h2'], fontSize=10, fontName='Helvetica-Bold', spaceBefore=6, spaceAfter=2,alignment=1)
    h3_style = ParagraphStyle(name='H3Small', parent=styles['h3'], fontSize=9, fontName='Helvetica-Bold', spaceBefore=4, spaceAfter=1, alignment=0) # Left-aligned for sub-sections
    table_header_style = ParagraphStyle(name='TableHeaderSmall', parent=normal_style, fontName='Helvetica-Bold', fontSize=6, alignment=1)


    story = []

    filename_prefix = "probable_pitchers" if report_date == datetime.now().strftime('%Y-%m-%d') else "historical"
    title_text = f"Consolidated Inning {inning_number} Report ({filename_prefix.replace('_', ' ').title()} - {report_date})"
    story.append(Paragraph(title_text, title_style))
    story.append(Spacer(1, 0.1*inch))

    # --- NEW: Summary Table of Top Recommendations ---
    all_top_recommendations = []

    # Mapping for confidence scores for consistent ranking
    # Combine all relevant maps into one for easier lookup
    full_confidence_map = {**confidence_map, **nrfi_yrfi_map, **over_under_map}

    for metric_cfg in all_metrics_config:
        metric_name = metric_cfg['name']
        overall_conf_col = metric_cfg.get('overall_conf_col')

        if overall_conf_col and overall_conf_col in report_df.columns:
            # Sort by the overall confidence column for this metric
            # Use a lambda function with .map() to convert confidence strings to numeric scores for sorting
            sorted_metric_df = report_df.sort_values(
                by=overall_conf_col,
                key=lambda x: x.map(full_confidence_map),
                ascending=False # Highest confidence first
            ).head(3) # Get top 3 for this metric

            for _, row in sorted_metric_df.iterrows():
                confidence_str = row[overall_conf_col]
                numeric_score = full_confidence_map.get(confidence_str, 0)
                
                # Determine the 'Bet Recommendation' based on the metric type
                bet_rec = "N/A"
                if metric_name == 'Strikeouts':
                    bet_rec = row['PITCHER K BET']
                elif metric_name == 'Runs':
                    bet_rec = row['PITCHER RUNS BET']
                else: # For Hits, Walks, Singles, Doubles, Triples, Homers, Total Bases
                    # Check if 'High (Under)' or 'High (Over)' for these metrics
                    if 'Under' in confidence_str:
                        bet_rec = f"Under {metric_name} Bet"
                    elif 'Over' in confidence_str:
                        bet_rec = f"Over {metric_name} Bet"
                    else:
                        bet_rec = "Neutral"

                all_top_recommendations.append({
                    'Metric': metric_name,
                    'Game': row['Game'],
                    'Pitcher': row['Pitcher'],
                    'Opponent': row['Opponent'],
                    'Confidence': confidence_str,
                    'Bet Recommendation': bet_rec,
                    'Score': numeric_score # Keep numeric score for final sorting
                })

    # Sort all collected recommendations by their numeric score
    all_top_recommendations_sorted = sorted(all_top_recommendations, key=lambda x: x['Score'], reverse=True)

    if all_top_recommendations_sorted:
        story.append(Paragraph("<b>Top Recommendations Across All Metrics (Overall Ranking):</b>", h2_style))
        story.append(Spacer(1, 0.1*inch))

        summary_table_headers = [
            Paragraph('Rank', table_header_style),
            Paragraph('Metric', table_header_style),
            Paragraph('Game', table_header_style),
            Paragraph('Pitcher', table_header_style),
            Paragraph('Opponent', table_header_style),
            Paragraph('Confidence', table_header_style),
            Paragraph('Bet Rec.', table_header_style)
        ]
        summary_table_data = [summary_table_headers]

        for i, rec in enumerate(all_top_recommendations_sorted):
            summary_table_data.append([
                Paragraph(str(i + 1), normal_style),
                Paragraph(rec['Metric'], normal_style),
                Paragraph(rec['Game'], normal_style),
                Paragraph(rec['Pitcher'], normal_style),
                Paragraph(rec['Opponent'], normal_style),
                Paragraph(rec['Confidence'], normal_style),
                Paragraph(rec['Bet Recommendation'], normal_style)
            ])
        
        # Extract plain text headers for index lookup
        plain_text_headers = [p.text for p in summary_table_headers]

        # Calculate column widths for the summary table
        summary_available_width = landscape(letter)[0] - 1*inch
        summary_col_widths = [
            summary_available_width * 0.05, # Rank
            summary_available_width * 0.12, # Metric
            summary_available_width * 0.20, # Game
            summary_available_width * 0.18, # Pitcher
            summary_available_width * 0.18, # Opponent
            summary_available_width * 0.12, # Confidence
            summary_available_width * 0.15, # Bet Rec.
        ]

        summary_table = Table(summary_table_data, colWidths=summary_col_widths)
        summary_table_style = [
            ('BACKGROUND', (0,0), (-1,0), colors.darkgrey),
            ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0,0), (-1,0), 4),
            ('BACKGROUND', (0,1), (-1,-1), colors.lightgrey),
            ('GRID', (0,0), (-1,-1), 0.5, colors.black),
            ('LEFTPADDING', (0,0), (-1,-1), 2),
            ('RIGHTPADDING', (0,0), (-1,-1), 2),
        ]
        # Apply highlighting to the Confidence and Bet Recommendation columns in the summary table
        conf_col_idx = plain_text_headers.index('Confidence')
        bet_rec_col_idx = plain_text_headers.index('Bet Rec.')

        for i, rec in enumerate(all_top_recommendations_sorted):
            row_index = i + 1 # +1 for header row
            confidence_str = rec['Confidence']
            bet_rec_str = rec['Bet Recommendation']

            # Confidence highlighting
            if 'High' in confidence_str:
                summary_table_style.append(('BACKGROUND', (conf_col_idx, row_index), (conf_col_idx, row_index), colors.lightgreen))
            elif 'Moderate' in confidence_str:
                summary_table_style.append(('BACKGROUND', (conf_col_idx, row_index), (conf_col_idx, row_index), colors.lightyellow))
            elif 'Low' in confidence_str:
                summary_table_style.append(('BACKGROUND', (conf_col_idx, row_index), (conf_col_idx, row_index), colors.salmon))
            
            # Bet Recommendation highlighting
            if 'UNDER' in bet_rec_str.upper() or 'HIGH OVER' in bet_rec_str.upper(): # High Over for K's is good
                summary_table_style.append(('BACKGROUND', (bet_rec_col_idx, row_index), (bet_rec_col_idx, row_index), colors.lightgreen))
            elif 'OVER' in bet_rec_str.upper():
                summary_table_style.append(('BACKGROUND', (bet_rec_col_idx, row_index), (bet_rec_col_idx, row_index), colors.salmon))
            else: # Neutral
                summary_table_style.append(('BACKGROUND', (bet_rec_col_idx, row_index), (bet_rec_col_idx, row_index), colors.lightyellow))


        summary_table.setStyle(TableStyle(summary_table_style))
        story.append(summary_table)
        story.append(Spacer(1, 0.2*inch))
    else:
        story.append(Paragraph("No top recommendations available for summary.", normal_style))
        story.append(Spacer(1, 0.2*inch))
    # --- END NEW: Summary Table of Top Recommendations ---

    # Iterate through each metric configuration to generate its section
    for metric_cfg in all_metrics_config:
        story.append(PageBreak()) # Start a new page for each metric for clarity
        story.append(Paragraph(f"--- {metric_cfg['name']} Analysis ---", h2_style))
        story.append(Spacer(1, 0.1*inch))

        display_cols = ['Game', 'Pitcher', 'Opponent'] + metric_cfg['pitcher_cols'] + metric_cfg['opponent_cols']
        if metric_cfg.get('overall_conf_col') and metric_cfg['overall_conf_col'] in report_df.columns:
            display_cols.append(metric_cfg['overall_conf_col'])
        if metric_cfg.get('moved_pitcher_today_col') and metric_cfg['moved_pitcher_today_col'] in report_df.columns:
            display_cols.append(metric_cfg['moved_pitcher_today_col'])
        
        # Specific column exclusions for consolidated report to keep it clean
        columns_to_explicitly_exclude = [
            'PITCH WALKS ALLOWED/GM',
            'PITCH WALKS ALLOWED RATE %',
            'OPP WALKS BATTING RATE %',
            f'TODAY OPPONENT WALKS BATTING INNING {inning_number}'
        ]

        if metric_cfg['name'] not in ['Runs', 'Strikeouts']:
            display_cols = [col for col in display_cols if col not in columns_to_explicitly_exclude]
        
        if metric_cfg['name'] == 'Strikeouts':
            display_cols = [col for col in display_cols if col != f'TODAY OPPONENT STRIKEOUTS INNING {inning_number}']

        missing_cols = [col for col in display_cols if col not in report_df.columns]
        if missing_cols:
            print(f"ERROR: The following display columns are missing from report_df for {metric_cfg['name']}: {missing_cols}. Adding as N/A.")
            for col in missing_cols:
                report_df[col] = "N/A"

        metric_df = report_df[display_cols].copy()

        numerical_columns = []
        PITCHER_METRICS_INNING = get_pitcher_metrics_for_inning(inning_number)
        BATTING_METRICS_INNING = get_batting_metrics_for_inning(inning_number)

        for metric in PITCHER_METRICS_INNING:
            if metric.get('report_key_avg') and metric['report_key_avg'] in metric_df.columns: numerical_columns.append(metric['report_key_avg'])
            if metric.get('report_key_rate') and metric['report_key_rate'] in metric_df.columns: numerical_columns.append(metric['report_key_rate'])
            if metric.get('report_key_per_game_pct') and metric['report_key_per_game_pct'] in metric_df.columns: numerical_columns.append(metric['report_key_per_game_pct'])
            if metric.get('last_game_key') and metric['last_game_key'] in metric_df.columns: numerical_columns.append(metric['last_game_key'])

        for metric in BATTING_METRICS_INNING:
            if metric.get('report_key_avg') and metric['report_key_avg'] in metric_df.columns: numerical_columns.append(metric['report_key_avg'])
            if metric.get('report_key_rate') and metric['report_key_rate'] in metric_df.columns: numerical_columns.append(metric['report_key_rate'])
            if metric.get('report_key_per_game_pct') and metric['report_key_per_game_pct'] in metric_df.columns: numerical_columns.append(metric['report_key_per_game_pct'])
            if metric.get('last_game_key') and metric['last_game_key'] in metric_df.columns: numerical_columns.append(metric['last_game_key'])
        
        for col_suffix in [
            f'PITCH NRFI % INNING {inning_number}',
            f'# TOTAL STARTS INNING {inning_number}',
            f'PITCHER VENUE NRFI % INNING {inning_number}',
            f'OPPONENT VENUE NRFI % INNING {inning_number}',
            f'BAT NRFI % INNING {inning_number}',
            'VENUE PITCH HIT AVG',
            'VENUE BAT HIT AVG',
            'VENUE PITCH WALKS ALLOWED/GM',
            'VENUE BAT WALKS BATTING/GM',
            'VENUE PITCH SINGLES ALLOWED/GM',
            'VENUE BAT SINGLES BATTING/GM',
            'VENUE PITCH DOUBLES ALLOWED/GM',
            'VENUE BAT DOUBLES BATTING/GM',
            'VENUE PITCH TRIPLES ALLOWED/GM',
            'VENUE BAT TRIPLES BATTING/GM',
            'VENUE PITCH HOMERS ALLOWED/GM',
            'VENUE BAT HOMERS BATTING/GM',
            'VENUE PITCH TOTAL BASES ALLOWED/GM',
            'VENUE BAT TOTAL BASES BATTING/GM',
            f'PITCHER VENUE NRHI % INNING {inning_number}',
            f'BATTER VENUE NRHI % INNING {inning_number}',
            'VENUE PITCH K RATE %',
            'VENUE OPP K RATE %',
            'PITCH K OVERALL RATE %',
            'OPP K OVERALL RATE %',
        ]:
            if col_suffix in metric_df.columns: numerical_columns.append(col_suffix)
        
        if metric_cfg.get('moved_pitcher_today_col') and metric_cfg['moved_pitcher_today_col'] in metric_df.columns:
            numerical_columns.append(metric_cfg['moved_pitcher_today_col'])

        numerical_columns = list(set(numerical_columns))

        for col in metric_df.columns:
            if col in numerical_columns:
                try:
                    if col == f'LAST GAME PITCHER STRIKEOUTS INNING {inning_number}' or col == f'LAST GAME OPPONENT STRIKEOUTS BATTING INNING {inning_number}':
                        metric_df[col] = pd.to_numeric(metric_df[col], errors='coerce').fillna(0).astype(int)
                    else:
                        metric_df[col] = pd.to_numeric(metric_df[col], errors='coerce').round(2)
                except Exception as e:
                    print(f"Error converting column {col} to numeric for PDF: {e}")
                    metric_df[col] = metric_df[col].astype(str)
            else:
                metric_df[col] = metric_df[col].astype(str)

        header_row = [Paragraph(col.replace(f' INNING {inning_number}', ''), table_header_style) for col in metric_df.columns.tolist()]
        table_data_list = [header_row]
        for index, row in metric_df.iterrows():
            row_data = []
            for col_name in metric_df.columns:
                val = row[col_name]
                if isinstance(val, pd.Series):
                    val = val.iloc[0] if not val.empty else None

                if pd.isna(val) or val is None:
                    row_data.append(Paragraph("N/A", normal_style))
                elif isinstance(val, float):
                    if col_name == f'LAST GAME PITCHER STRIKEOUTS INNING {inning_number}' or col_name == f'LAST GAME OPPONENT STRIKEOUTS BATTING INNING {inning_number}':
                        row_data.append(Paragraph(f"{int(val)}", normal_style))
                    else:
                        row_data.append(Paragraph(f"{val:.2f}", normal_style))
                else:
                    row_data.append(Paragraph(str(val), normal_style))
            table_data_list.append(row_data)

        available_width = landscape(letter)[0] - 1*inch
        num_cols = len(metric_df.columns)
        base_col_width = available_width / num_cols
        
        col_widths = []
        for col_name in metric_df.columns:
            if 'Game' in col_name: col_widths.append(base_col_width * 1.2)
            elif 'Pitcher' in col_name: col_widths.append(base_col_width * 1.3)
            elif 'Opponent' in col_name: col_widths.append(base_col_width * 0.8)
            elif 'CONFIDENCE' in col_name or 'CONF' in col_name: col_widths.append(base_col_width * 1.1)
            elif 'BET' in col_name: col_widths.append(base_col_width * 1.0)
            elif 'NRFI %' in col_name: col_widths.append(base_col_width * 1.0)
            elif 'NRHI %' in col_name: col_widths.append(base_col_width * 1.0)
            elif '# TOTAL STARTS' in col_name: col_widths.append(base_col_width * 0.7)
            elif 'LAST GAME' in col_name: col_widths.append(base_col_width * 1.0)
            elif 'VENUE NRFI %' in col_name: col_widths.append(base_col_width * 1.0)
            elif 'TODAY PITCHER' in col_name: col_widths.append(base_col_width * 1.0)
            elif 'VENUE PITCH' in col_name: col_widths.append(base_col_width * 1.0)
            elif 'VENUE BAT' in col_name: col_widths.append(base_col_width * 1.0)
            else: col_widths.append(base_col_width * 0.9)
        
        total_requested_width = sum(col_widths)
        col_widths = [(w / total_requested_width) * available_width for w in col_widths]

        table_style_cmds = [
            ('BACKGROUND', (0,0), (-1,0), colors.grey),
            ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0,0), (-1,0), 4),
            ('BACKGROUND', (0,1), (-1,-1), colors.beige),
            ('GRID', (0,0), (-1,-1), 0.5, colors.black),
            ('LEFTPADDING', (0,0), (-1,-1), 2),
            ('RIGHTPADDING', (0,0), (-1,-1), 2),
        ]

        for highlight_rule in metric_cfg.get('highlight_cols', []):
            if highlight_rule.get('type') == 'bet_recommendation_k' and highlight_rule['col'] in metric_df.columns:
                apply_bet_recommendation_k_highlight(metric_df, highlight_rule['col'], table_style_cmds)
            elif highlight_rule.get('type') == 'bet_recommendation' and highlight_rule['col'] in metric_df.columns:
                apply_bet_recommendation_highlight(metric_df, highlight_rule['col'], table_style_cmds)
            elif highlight_rule.get('type') == 'today_runs_highlight' and highlight_rule['col'] in metric_df.columns:
                apply_today_runs_highlight(metric_df, highlight_rule['col'], table_style_cmds)
            elif highlight_rule.get('type') == 'nrfi_highlight' and highlight_rule['col'] in metric_df.columns:
                apply_nrfi_highlight(metric_df, highlight_rule['col'], table_style_cmds)
            elif highlight_rule.get('type') == 'zero_value_highlight' and highlight_rule['col'] in metric_df.columns:
                apply_zero_value_highlight(metric_df, highlight_rule['col'], table_style_cmds)
            elif highlight_rule.get('type') == 'positive_value_highlight' and highlight_rule['col'] in metric_df.columns:
                apply_positive_value_highlight(metric_df, highlight_rule['col'], table_style_cmds)
            elif highlight_rule.get('type') == 'percentage_range_highlight' and highlight_rule['col'] in metric_df.columns:
                apply_percentage_range_highlight(metric_df, highlight_rule['col'], table_style_cmds)
            elif 'col_conf' in highlight_rule and highlight_rule['col_conf'] in metric_df.columns:
                apply_confidence_highlight(metric_df, highlight_rule['col_conf'], table_style_cmds, highlight_rule.get('is_inverse', False))
            elif 'col' in highlight_rule and highlight_rule['col'] in metric_df.columns:
                apply_top_bottom_highlight(metric_df, report_df, highlight_rule['col'], table_style_cmds, ascending=highlight_rule['ascending'])

        table = Table(table_data_list, colWidths=col_widths)
        table.setStyle(TableStyle(table_style_cmds))
        story.append(table)
        story.append(Spacer(1, 0.15*inch))

        # --- Recommendations for this metric ---
        current_recs = {}
        if metric_cfg['name'].lower() == 'strikeouts':
            current_recs = all_recommendations['strikeout_recs_gen']
        elif metric_cfg['name'].lower() == 'runs':
            current_recs = all_recommendations['runs_recs_gen']
        else: # For Hits, Walks, Singles, Doubles, Triples, Homers, Total Bases
            # Filter other_metrics_recs_gen and other_metrics_parlays_gen
            # to only include recommendations/parlays relevant to the current metric
            filtered_recs = {}
            for rec_type, rec_list in all_recommendations['other_metrics_recs_gen'].items(): # Use all_recommendations['other_metrics_recs_gen']
                if metric_cfg['name'].lower() in rec_type.lower():
                    filtered_recs[rec_type] = rec_list
            current_recs = filtered_recs

        if current_recs:
            story.append(Paragraph(f"<b>{metric_cfg['name']} Recommendations:</b>", h3_style))
            for rec_type, rec_list in current_recs.items():
                # Filter recommendations to only show those relevant to the current metric
                # This is a heuristic based on `rec_type` string
                if rec_list and isinstance(rec_list, list) and rec_list and isinstance(rec_list[0], dict):
                    story.append(Paragraph(f"<b>{rec_type}:</b>", normal_style))
                    rec_header = [Paragraph(str(col).replace(f' INNING {inning_number}', ''), table_header_style) for col in rec_list[0].keys()]
                    rec_data_list = [rec_header]
                    for rec in rec_list:
                        row_data = []
                        for k in rec_list[0].keys():
                            val = rec.get(k, "N/A")
                            if isinstance(val, float):
                                row_data.append(Paragraph(f"{val:.2f}", normal_style))
                            else:
                                row_data.append(Paragraph(str(val), normal_style))
                        rec_data_list.append(row_data)
                    
                    rec_table = Table(rec_data_list)
                    rec_table.setStyle(TableStyle([
                        ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
                        ('TEXTCOLOR', (0,0), (-1,0), colors.black),
                        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
                    ]))
                    story.append(rec_table)
                    story.append(Spacer(1, 0.05*inch))
                else:
                    story.append(Paragraph(f"<b>{rec_type}:</b> No recommendations available.", normal_style))
                    story.append(Spacer(1, 0.05*inch))
            story.append(Spacer(1, 0.1*inch))

        # --- Parlays for this metric ---
        current_parlays = {}
        if metric_cfg['name'].lower() == 'strikeouts':
            current_parlays = all_parlays['strikeout_parlays_gen']
        elif metric_cfg['name'].lower() == 'runs':
            current_parlays = all_parlays['runs_parlays_gen']
        else:
            filtered_parlays = {}
            for parlay_type, parlay_list in all_parlays['other_metrics_parlays_gen'].items(): # Use all_parlays['other_metrics_parlays_gen']
                if metric_cfg['name'].lower() in parlay_type.lower():
                    filtered_parlays[parlay_type] = parlay_list
            current_parlays = filtered_parlays

        if current_parlays:
            story.append(Paragraph(f"<b>{metric_cfg['name']} Parlays:</b>", h3_style))
            found_parlays_for_metric = False
            for parlay_type, parlay_list in current_parlays.items():
                if parlay_list and isinstance(parlay_list, list):
                    found_parlays_for_metric = True
                    story.append(Paragraph(f"<b>{parlay_type}:</b>", normal_style))
                    parlay_data_list = [[Paragraph('Games', table_header_style), Paragraph('Score', table_header_style)]]
                    for parlay in parlay_list:
                        parlay_data_list.append([Paragraph(parlay['games'], normal_style), Paragraph(f"{parlay.get('score', 0):.2f}", normal_style)])
                    
                    parlay_table = Table(parlay_data_list, colWidths=[available_width*0.8, available_width*0.18])
                    parlay_table.setStyle(TableStyle([
                        ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
                        ('TEXTCOLOR', (0,0), (-1,0), colors.black),
                        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
                    ]))
                    story.append(parlay_table)
                    story.append(Spacer(1, 0.05*inch))
            if not found_parlays_for_metric:
                story.append(Paragraph("No relevant parlays available for this metric.", normal_style))
                story.append(Spacer(1, 0.05*inch))
        story.append(Spacer(1, 0.2*inch)) # Add some space before the next metric or end of report

    try:
        doc.build(story)
        buffer.seek(0) # Rewind the buffer to the beginning
        return buffer
    except Exception as e:
        logging.error(f"Error generating consolidated PDF for Inning {inning_number} report: {e}", exc_info=True)
        print(f"Error generating consolidated PDF for Inning {inning_number} report. Check logs for details.")
        return None


def calculate_confidence_level(value, thresholds, comparison_type='greater'):
    """
    Determines confidence level based on a value and thresholds.
    `comparison_type` can be 'greater' (higher value is better) or 'less' (lower value is better).
    """
    if comparison_type == 'greater':
        if value >= thresholds['high']:
            return "High"
        elif value >= thresholds['moderate']:
            return "Moderate"
        else:
            return "Low"
    elif comparison_type == 'less':
        if value <= thresholds['high']:
            return "High"
        elif value <= thresholds['moderate']:
            return "Moderate"
        else:
            return "Low"
    return "Low"


def get_over_under_recommendation(predicted_value, historical_average, metric_type):
    """
    Generates an 'Over', 'Under', or 'Neutral' recommendation based on predicted vs. historical average.
    `metric_type` can be 'positive' (higher is better/more) or 'negative' (lower is better/less).
    """
    if historical_average == 0:
        if predicted_value > 0:
            return "Over"
        else:
            return "Neutral"
    
    percentage_diff = (predicted_value - historical_average) / historical_average
    
    if metric_type == 'positive':
        if percentage_diff >= OVER_UNDER_THRESHOLD_PCT:
            return "Over"
        elif percentage_diff <= -OVER_UNDER_THRESHOLD_PCT:
            return "Under"
    elif metric_type == 'negative':
        if percentage_diff >= OVER_UNDER_THRESHOLD_PCT:
            return "Over"
        elif percentage_diff <= -OVER_UNDER_THRESHOLD_PCT:
            return "Under"
    
    return "Neutral"

def initialize_directories():
    """Initializes necessary directories for caching and reports."""
    os.makedirs(BASE_CACHE_DIR, exist_ok=True)
    os.makedirs(FULL_GAME_CACHE_DIR, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)
    os.makedirs('logs', exist_ok=True)

def run_full_data_pipeline(report_date_input, inning_num_process):
    """
    Runs the data fetching and consolidation pipeline for a given date and inning.
    This function will be called by the Streamlit app.
    """
    end_date_for_fetch = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d') \
        if report_date_input.lower() == 'today' else report_date_input

    print(f"Fetching historical data up to: {end_date_for_fetch}")

    inning_specific_raw_data_dir = os.path.join(BASE_CACHE_DIR, f'inning_{inning_num_process}')

    fetch_and_process_inning_data(inning_num_process, START_2025_SEASON, end_date_for_fetch, inning_specific_raw_data_dir)
    consolidate_daily_data(inning_specific_raw_data_dir, inning_num_process)

    print(f"Data pipeline completed for Inning {inning_num_process} on {report_date_input}.")
