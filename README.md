# GA4 Multi-Ecosystem Report Automation

A Python script that connects to the Google Analytics 4 API and automatically generates a full monthly analytics report across multiple website ecosystems — outputting a self-contained HTML dashboard and an Excel file, ready to share.

## The problem it solves
Manually exporting Looker Studio reports across 8 separate website ecosystems every month was taking hours. This script pulls everything automatically in seconds.

## What it generates
- **HTML report** — visual dashboard with charts, KPI cards, and tables
- **Excel file** — 4 sheets: Ecosystem Summary, Channels, Source/Medium, Top Pages
- Month-over-month comparison built in
- Ecosystem filtering by hostname (maps directly to GA4 audience structure)

## Stack
Python · GA4 Data API · Pandas · Matplotlib · Jinja2 · OpenPyXL · Google Cloud

## Setup

### 1. Install dependencies
pip3 install google-analytics-data google-auth pandas openpyxl matplotlib jinja2 python-dateutil

### 2. Google Cloud setup
- Enable the **Google Analytics Data API** in Google Cloud Console
- Create a **Service Account** → download the JSON key
- In GA4: Admin → Property Access Management → add service account email as **Viewer**

### 3. Configure the script
Fill in the CONFIG block at the top of `ga4_monthly_report.py`:
```python
CONFIG = {
    "property_id": "YOUR_GA4_PROPERTY_ID",
    "credentials_path": "service_account.json",
    "output_dir": ".",
    "report_month": None,  # auto-uses last month, or set "2026-02"
}
```

### 4. Run
```bash
python3 ga4_monthly_report.py
```

## Note
Never commit your `service_account.json` to GitHub. Add it to `.gitignore`.
