import streamlit as st
import pandas as pd
from datetime import datetime, date
import os
import io

# Import functions from the core logic file
# Assuming mlb_inning_analytics_core.py is in the same directory
from mlb_inning_analytics_core import (
    generate_report_data_and_pdfs,
    get_report_metrics_config_for_inning,
    initialize_directories,
    run_full_data_pipeline,
    confidence_map, # Import confidence_map for consistent highlighting logic
    nrfi_yrfi_map, # Import nrfi_yrfi_map for consistent highlighting logic
    over_under_map # Import over_under_map for consistent highlighting logic
)

# Set Streamlit page configuration
st.set_page_config(layout="wide", page_title="MLB Inning Analytics Dashboard")

# Initialize directories on app start
initialize_directories()

st.title("⚾ MLB Inning Analytics Dashboard")

st.markdown("""
This dashboard provides detailed analytics for MLB innings, including pitcher and opponent metrics,
confidence levels, and bet recommendations. You can select a date, an inning, and a specific metric
to view the analysis. PDF reports are also available for download.
""")

# --- Helper functions for Streamlit highlighting (CSS styling) ---

def get_color(color_name):
    """Maps a color name to a hex code."""
    colors_map = {
        'lightgreen': '#90EE90',
        'lightyellow': '#FFFFE0',
        'salmon': '#FA8072',
        'lightblue': '#ADD8E6',
        'lightgrey': '#D3D3D3',
        'white': '#FFFFFF',
        'black': '#000000',
        'grey': '#808080',
        'whitesmoke': '#F5F5F5',
        'beige': '#F5F5DC',
        'darkgrey': '#A9A9A9'
    }
    return colors_map.get(color_name.lower(), '#FFFFFF') # Default to white if not found

def highlight_confidence_streamlit(s, is_inverse=False):
    """Applies highlighting based on confidence levels (High, Moderate, Low)."""
    styles = []
    for val in s:
        confidence = str(val)
        if 'High' in confidence:
            styles.append(f'background-color: {get_color("lightgreen") if not is_inverse else get_color("salmon")}')
        elif 'Moderate' in confidence:
            styles.append(f'background-color: {get_color("lightyellow")}')
        elif 'Low' in confidence:
            styles.append(f'background-color: {get_color("salmon") if not is_inverse else get_color("lightgreen")}')
        else:
            styles.append('')
    return styles

def highlight_bet_recommendation_streamlit(s):
    """Applies highlighting based on 'Under/Over Bet' recommendations."""
    styles = []
    for val in s:
        bet_recommendation = str(val).upper()
        if 'UNDER' in bet_recommendation:
            styles.append(f'background-color: {get_color("lightgreen")}')
        elif 'OVER' in bet_recommendation:
            styles.append(f'background-color: {get_color("salmon")}')
        else: # Neutral
            styles.append(f'background-color: {get_color("lightyellow")}')
    return styles

def highlight_bet_recommendation_k_streamlit(s):
    """Applies highlighting for Strikeout 'Bet' recommendations."""
    styles = []
    for val in s:
        bet_recommendation = str(val).upper()
        if 'OVER' in bet_recommendation or 'HIGH OVER' in bet_recommendation:
            styles.append(f'background-color: {get_color("lightgreen")}')
        elif 'UNDER' in bet_recommendation:
            styles.append(f'background-color: {get_color("salmon")}')
        else: # Neutral
            styles.append(f'background-color: {get_color("lightyellow")}')
    return styles

def highlight_nrfi_percentage_streamlit(s):
    """Applies highlighting to NRFI/NRHI % columns based on thresholds."""
    styles = []
    for val in s:
        try:
            nrfi_pct = float(val)
            if nrfi_pct > 80:
                styles.append(f'background-color: {get_color("lightgreen")}')
            elif 65 <= nrfi_pct <= 79:
                styles.append(f'background-color: {get_color("lightyellow")}') # Changed to yellow for consistency with moderate
            elif nrfi_pct < 50:
                styles.append(f'background-color: {get_color("salmon")}')
            else:
                styles.append('')
        except ValueError:
            styles.append('')
    return styles

def highlight_today_runs_streamlit(s):
    """Applies highlighting to 'TODAY PITCHER RUNS ALLOWED' or 'TODAY PITCHER HITS' columns."""
    styles = []
    for val in s:
        try:
            value = float(val)
            if value == 0:
                styles.append(f'background-color: {get_color("lightgreen")}')
            else:
                styles.append('')
        except ValueError:
            styles.append('')
    return styles

def highlight_zero_value_streamlit(s):
    """Applies highlighting to a column if its numeric value is 0."""
    styles = []
    for val in s:
        try:
            value = float(val)
            if value == 0:
                styles.append(f'background-color: {get_color("lightgreen")}')
            else:
                styles.append('')
        except ValueError:
            styles.append('')
    return styles

def highlight_positive_value_streamlit(s):
    """Applies highlighting to a column if its numeric value is greater than 0."""
    styles = []
    for val in s:
        try:
            value = float(val)
            if value > 0:
                styles.append(f'background-color: {get_color("lightgreen")}')
            else:
                styles.append('')
        except ValueError:
            styles.append('')
    return styles

def highlight_percentage_range_streamlit(s):
    """Applies highlighting based on percentage ranges: >= 80: Green, 70-79: Blue."""
    styles = []
    for val in s:
        try:
            value = float(val)
            if value >= 80:
                styles.append(f'background-color: {get_color("lightgreen")}')
            elif 70 <= value <= 79:
                styles.append(f'background-color: {get_color("lightblue")}')
            else:
                styles.append('')
        except ValueError:
            styles.append('')
    return styles

def highlight_top_bottom_streamlit(data_series, full_df_col, n=3, ascending=False):
    """
    Applies highlighting for top N and bottom N values within the *full* dataset column.
    This function needs to be applied using `df.style.apply` with `axis=0`.
    `data_series` is the column being styled (from the filtered/displayed DataFrame).
    `full_df_col` is the corresponding column from the complete, unfiltered DataFrame.
    """
    styles = [''] * len(data_series)
    
    try:
        numeric_full_col = pd.to_numeric(full_df_col, errors='coerce').dropna()
        if numeric_full_col.empty:
            return styles
    except Exception as e:
        st.error(f"Error converting column to numeric for top/bottom highlighting: {e}")
        return styles

    # Get top N values
    top_n_values = numeric_full_col.nsmallest(n) if ascending else numeric_full_col.nlargest(n)
    
    # Get bottom N values (excluding those already in top N)
    bottom_n_values = numeric_full_col.nlargest(n) if ascending else numeric_full_col.nsmallest(n)
    bottom_n_values = bottom_n_values[~bottom_n_values.isin(top_n_values)] # Exclude overlaps

    for i, val in enumerate(data_series):
        try:
            numeric_val = float(val)
            if numeric_val in top_n_values.values:
                # Assign specific colors for top 3
                if n == 3:
                    if numeric_val == top_n_values.iloc[0]: # Best
                        styles[i] = f'background-color: {get_color("lightgreen")}'
                    elif numeric_val == top_n_values.iloc[1]: # Second best
                        styles[i] = f'background-color: {get_color("lightblue")}'
                    elif numeric_val == top_n_values.iloc[2]: # Third best
                        styles[i] = f'background-color: {get_color("lightyellow")}'
                else: # Generic for top N if N is not 3
                    styles[i] = f'background-color: {get_color("lightgreen")}'
            elif numeric_val in bottom_n_values.values:
                styles[i] = f'background-color: {get_color("salmon")}'
        except ValueError:
            pass
    return styles

# --- End Helper functions for Streamlit highlighting ---


# --- Sidebar Filters ---
st.sidebar.header("Report Filters")

# Date Selector
today = date.today()
report_date = st.sidebar.date_input(
    "Select Report Date",
    value=today,
    min_value=datetime.strptime("2025-03-28", "%Y-%m-%d").date(), # Start of 2025 season
    max_value=today
)
report_date_str = report_date.strftime("%Y-%m-%d")

# Inning Selector
inning_number = st.sidebar.selectbox(
    "Select Inning",
    options=list(range(1, 10)), # Innings 1-9
    index=0 # Default to 1st inning
)

# Metric Selector
all_metrics_config = get_report_metrics_config_for_inning(inning_number)
metric_options = [cfg['name'] for cfg in all_metrics_config]
selected_metric_name = st.sidebar.selectbox(
    "Select Metric for Display",
    options=metric_options,
    index=0 # Default to 'Strikeouts'
)

# Automatic Data Pull Checkbox
auto_refresh_data = st.sidebar.checkbox("Automatically refresh data on load (may take time)", value=False)

# --- Main Content Area ---

st.header(f"Analysis for Inning {inning_number} on {report_date_str}")

# Trigger data pull if checkbox is enabled or if "Generate Report" button is clicked
if auto_refresh_data or st.button("Generate Report"):
    st.info(f"Fetching and processing data for Inning {inning_number} on {report_date_str}. This may take a few minutes...")

    with st.spinner("Running data pipeline..."):
        run_full_data_pipeline(report_date_str, inning_number)
    
    with st.spinner("Generating report and recommendations..."):
        (report_df, strikeout_recs, runs_recs, strikeout_parlays, runs_parlays,
         other_metrics_recs, other_metrics_parlays, pdf_buffers) = \
            generate_report_data_and_pdfs(report_date_str, inning_number)

    if report_df.empty:
        st.warning("No data available to generate the report for the selected date and inning. Please try a different date or check the console for errors.")
    else:
        st.success("Report generated successfully!")

        # Store results in session state to persist across reruns
        st.session_state['report_df'] = report_df
        st.session_state['strikeout_recs'] = strikeout_recs
        st.session_state['runs_recs'] = runs_recs
        st.session_state['strikeout_parlays'] = strikeout_parlays
        st.session_state['runs_parlays'] = runs_parlays
        st.session_state['other_metrics_recs'] = other_metrics_recs
        st.session_state['other_metrics_parlays'] = other_metrics_parlays
        st.session_state['pdf_buffers'] = pdf_buffers
        st.session_state['selected_metric_name'] = selected_metric_name # Store for display logic
        st.session_state['inning_number'] = inning_number # Store inning number
        st.session_state['report_date_str'] = report_date_str # Store report date string

# --- Display Results (after generation) ---
if 'report_df' in st.session_state and not st.session_state['report_df'].empty:
    report_df = st.session_state['report_df']
    strikeout_recs = st.session_state['strikeout_recs']
    runs_recs = st.session_state['runs_recs']
    strikeout_parlays = st.session_state['strikeout_parlays']
    runs_parlays = st.session_state['runs_parlays']
    other_metrics_recs = st.session_state['other_metrics_recs']
    other_metrics_parlays = st.session_state['other_metrics_parlays']
    pdf_buffers = st.session_state['pdf_buffers']
    selected_metric_name = st.session_state['selected_metric_name']
    inning_number = st.session_state['inning_number'] # Retrieve from session state
    report_date_str = st.session_state['report_date_str'] # Retrieve from session state


    st.subheader(f"Raw Data Table for {selected_metric_name}")

    # Find the config for the selected metric
    selected_metric_cfg = next((cfg for cfg in all_metrics_config if cfg['name'] == selected_metric_name), None)

    if selected_metric_cfg:
        display_cols = ['Game', 'Pitcher', 'Opponent'] + selected_metric_cfg['pitcher_cols'] + selected_metric_cfg['opponent_cols']
        if selected_metric_cfg.get('overall_conf_col') and selected_metric_cfg['overall_conf_col'] in report_df.columns:
            display_cols.append(selected_metric_cfg['overall_conf_col'])
        if selected_metric_cfg.get('moved_pitcher_today_col') and selected_metric_cfg['moved_pitcher_today_col'] in report_df.columns:
            display_cols.append(selected_metric_cfg['moved_pitcher_today_col'])
        
        # Filter out columns that might not be relevant for specific metrics in the consolidated view
        columns_to_explicitly_exclude = [
            'PITCH WALKS ALLOWED/GM',
            'PITCH WALKS ALLOWED RATE %',
            'OPP WALKS BATTING RATE %',
            f'TODAY OPPONENT WALKS BATTING INNING {inning_number}'
        ]
        if selected_metric_cfg['name'] not in ['Runs', 'Strikeouts']:
            display_cols = [col for col in display_cols if col not in columns_to_explicitly_exclude]
        if selected_metric_cfg['name'] == 'Strikeouts':
            display_cols = [col for col in display_cols if col != f'TODAY OPPONENT STRIKEOUTS INNING {inning_number}']

        # Ensure all display columns exist in the DataFrame, add as 'N/A' if missing
        for col in display_cols:
            if col not in report_df.columns:
                report_df[col] = "N/A"

        # IMPORTANT: Remove duplicates from display_cols to prevent ValueError
        display_cols = list(pd.unique(display_cols))

        # Create a copy for styling to avoid SettingWithCopyWarning
        df_to_style = report_df[display_cols].copy()

        # Convert relevant columns to numeric, coercing errors, before applying formatting
        # This handles 'N/A' values by turning them into NaN, which can then be formatted
        numeric_cols_for_formatting = []
        for col in df_to_style.columns: # Iterate over columns in df_to_style
            if 'LAST GAME' not in col: # Do not convert 'LAST GAME' columns to float for formatting
                original_dtype = df_to_style[col].dtype
                df_to_style[col] = pd.to_numeric(df_to_style[col], errors='coerce')
                if pd.api.types.is_numeric_dtype(df_to_style[col]):
                    numeric_cols_for_formatting.append(col)
                else:
                    df_to_style[col] = df_to_style[col].astype(original_dtype) # Revert if not numeric

        # Apply formatting to numeric columns
        styled_df = df_to_style.style.format("{:.2f}", subset=numeric_cols_for_formatting)

        # Apply highlighting rules based on metric_cfg
        for highlight_rule in selected_metric_cfg.get('highlight_cols', []):
            col_name = highlight_rule.get('col')
            col_conf = highlight_rule.get('col_conf')
            highlight_type = highlight_rule.get('type')

            if col_conf and col_conf in df_to_style.columns:
                styled_df = styled_df.apply(highlight_confidence_streamlit, subset=[col_conf], is_inverse=highlight_rule.get('is_inverse', False))
            elif col_name and col_name in df_to_style.columns:
                if highlight_type == 'bet_recommendation_k':
                    styled_df = styled_df.apply(highlight_bet_recommendation_k_streamlit, subset=[col_name])
                elif highlight_type == 'bet_recommendation':
                    styled_df = styled_df.apply(highlight_bet_recommendation_streamlit, subset=[col_name])
                elif highlight_type == 'today_runs_highlight':
                    styled_df = styled_df.apply(highlight_today_runs_streamlit, subset=[col_name])
                elif highlight_type == 'nrfi_highlight':
                    styled_df = styled_df.apply(highlight_nrfi_percentage_streamlit, subset=[col_name])
                elif highlight_type == 'zero_value_highlight':
                    styled_df = styled_df.apply(highlight_zero_value_streamlit, subset=[col_name])
                elif highlight_type == 'positive_value_highlight':
                    styled_df = styled_df.apply(highlight_positive_value_streamlit, subset=[col_name])
                elif highlight_type == 'percentage_range_highlight':
                    styled_df = styled_df.apply(highlight_percentage_range_streamlit, subset=[col_name])
                elif highlight_type is None and 'ascending' in highlight_rule: # This is for top/bottom highlighting
                    # Pass the full_df column to the styling function
                    styled_df = styled_df.apply(
                        lambda s: highlight_top_bottom_streamlit(s, report_df[col_name], ascending=highlight_rule['ascending']),
                        subset=[col_name]
                    )
        st.dataframe(styled_df, use_container_width=True)
    else:
        st.warning("Could not find configuration for the selected metric.")

    st.subheader(f"{selected_metric_name} Recommendations")

    current_recs_to_display = {}
    if selected_metric_name.lower() == 'strikeouts':
        current_recs_to_display = strikeout_recs
    elif selected_metric_name.lower() == 'runs':
        current_recs_to_display = runs_recs
    else:
        for rec_type, rec_list in other_metrics_recs.items():
            if selected_metric_name.lower() in rec_type.lower():
                current_recs_to_display[rec_type] = rec_list

    if current_recs_to_display:
        for rec_type, rec_list in current_recs_to_display.items():
            st.markdown(f"**{rec_type}:**")
            if rec_list:
                rec_df = pd.DataFrame(rec_list)
                # Remove inning suffix from column names for display
                rec_df.columns = [col.replace(f' INNING {inning_number}', '') for col in rec_df.columns]
                
                # Apply highlighting to recommendation tables
                rec_styled_df = rec_df.style.format("{:.2f}", subset=[col for col in rec_df.columns if rec_df[col].dtype in ['float64', 'int64']])
                
                # Apply specific highlighting for recommendation tables
                if selected_metric_name.lower() == 'strikeouts':
                    if 'Overall K CONFIDENCE' in rec_df.columns:
                        rec_styled_df = rec_styled_df.apply(highlight_confidence_streamlit, subset=['Overall K CONFIDENCE'])
                    if 'PITCHER K BET' in rec_df.columns:
                        rec_styled_df = rec_styled_df.apply(highlight_bet_recommendation_k_streamlit, subset=['PITCHER K BET'])
                    if 'OPPONENT K BET' in rec_df.columns:
                        rec_styled_df = rec_styled_df.apply(highlight_bet_recommendation_k_streamlit, subset=['OPPONENT K BET'])
                elif selected_metric_name.lower() == 'runs':
                    if 'Overall CONFIDENCE FOR NRFI AND YRFI' in rec_df.columns:
                        rec_styled_df = rec_styled_df.apply(highlight_confidence_streamlit, subset=['Overall CONFIDENCE FOR NRFI AND YRFI'])
                    if 'PITCHER RUNS BET' in rec_df.columns:
                        rec_styled_df = rec_styled_df.apply(highlight_bet_recommendation_streamlit, subset=['PITCHER RUNS BET'])
                    if 'OPPONENT RUNS BET' in rec_df.columns:
                        rec_styled_df = rec_styled_df.apply(highlight_bet_recommendation_streamlit, subset=['OPPONENT RUNS BET'])
                else: # For other metrics (Hits, Walks, etc.)
                    overall_conf_col_name = next((cfg['overall_conf_col'] for cfg in all_metrics_config if cfg['name'] == selected_metric_name), None)
                    if overall_conf_col_name and overall_conf_col_name in rec_df.columns:
                        rec_styled_df = rec_styled_df.apply(highlight_confidence_streamlit, subset=[overall_conf_col_name])
                    
                    bet_col_name = next((cfg['bet_col'] for cfg in all_metrics_config if cfg['name'] == selected_metric_name), None)
                    if bet_col_name and bet_col_name.replace(f' INNING {inning_number}', '') in rec_df.columns:
                        rec_styled_df = rec_styled_df.apply(highlight_bet_recommendation_streamlit, subset=[bet_col_name.replace(f' INNING {inning_number}', '')])
                    
                    opponent_bet_col_name = next((cfg['opponent_bet_col'] for cfg in all_metrics_config if cfg['name'] == selected_metric_name), None)
                    if opponent_bet_col_name and opponent_bet_col_name.replace(f' INNING {inning_number}', '') in rec_df.columns:
                        rec_styled_df = rec_styled_df.apply(highlight_bet_recommendation_streamlit, subset=[opponent_bet_col_name.replace(f' INNING {inning_number}', '')])


                st.dataframe(rec_styled_df, use_container_width=True)
            else:
                st.info("No recommendations available for this category.")
    else:
        st.info("No recommendations available for the selected metric.")

    st.subheader(f"{selected_metric_name} Parlays")

    current_parlays_to_display = {}
    if selected_metric_name.lower() == 'strikeouts':
        current_parlays_to_display = strikeout_parlays
    elif selected_metric_name.lower() == 'runs':
        current_parlays_to_display = runs_parlays
    else:
        for parlay_type, parlay_list in other_metrics_parlays.items():
            if selected_metric_name.lower() in parlay_type.lower():
                current_parlays_to_display[parlay_type] = parlay_list

    if current_parlays_to_display:
        for parlay_type, parlay_list in current_parlays_to_display.items():
            st.markdown(f"**{parlay_type}:**")
            if parlay_list:
                parlay_df = pd.DataFrame(parlay_list)
                # Apply highlighting to parlay scores (e.g., higher score is better)
                parlay_styled_df = parlay_df.style.format("{:.2f}", subset=['score']).background_gradient(cmap='Greens', subset=['score'])
                st.dataframe(parlay_styled_df, use_container_width=True)
            else:
                st.info("No parlays available for this category.")
    else:
        st.info("No parlays available for the selected metric.")

    st.subheader("Download Reports")
    if pdf_buffers:
        for pdf_info in pdf_buffers:
            st.download_button(
                label=f"Download {pdf_info['name']}",
                data=pdf_info['buffer'],
                file_name=pdf_info['name'],
                mime="application/pdf"
            )
    else:
        st.info("No PDF reports generated yet.")
