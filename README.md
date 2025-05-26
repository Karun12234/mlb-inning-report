
# MLB Inning Report (Streamlit App)

This app analyzes MLB pitcher and batter data for the 1st inning and visualizes predictions and reports.

## Features
- Generates PDF reports by inning
- Uses real-time CSV data (e.g., strikeouts, runs, hits)
- Highlighted confidence scoring
- Ready to deploy on [Streamlit Cloud](https://streamlit.io/cloud)

## How to Run Locally

```bash
pip install -r requirements.txt
streamlit run app/mlb_dashboard_app.py
```

## Directory Structure

```
mlb-inning-report/
├── app/
│   ├── mlb_inning_analytics_core.py
│   └── mlb_dashboard_app.py
├── cache/
├── requirements.txt
└── README.md
```
