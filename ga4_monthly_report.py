import os
import json
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, Dimension, Metric, DateRange, FilterExpression,
    Filter, FilterExpressionList, OrderBy
)
from google.oauth2 import service_account
from jinja2 import Template
import base64
from io import BytesIO


CONFIG = {
    "property_id": "261762506",        
    "credentials_path": "service_account.json",   
    "output_dir": ".",                             
    "report_month": None                     
}


ECOSYSTEMS = {
    "E: Patient Engagement":   "patientengagement.synapseconnect.org",
    "E: Precision Medicine":   "precisionmedicine.synapseconnect.org",
    "E: Digital Health":       "digitalhealth.synapseconnect.org",
    "E: Collective Change":    "collectivechange.synapseconnect.org",
    "E: Women's Health":       "womenshealth.synapseconnect.org",
    "E: Sustainable Health":   "sustainablehealth.synapseconnect.org",
    "E: MedTech":              "medtech.synapseconnect.org",
    "E: Ukrainian Health":     "ukrainian-health.synapseconnect.org",
}

ECOSYSTEM_COLORS = [
    "#0077B6", "#00B4D8", "#48CAE4", "#90E0EF",
    "#ADE8F4", "#CAF0F8", "#023E8A", "#0096C7"
]


def get_report_dates():
    if CONFIG["report_month"]:
        first = datetime.strptime(CONFIG["report_month"], "%Y-%m").date().replace(day=1)
    else:
        today = date.today()
        first = (today.replace(day=1) - relativedelta(months=1))
    last = (first + relativedelta(months=1)) - relativedelta(days=1)
    prev_first = first - relativedelta(months=1)
    prev_last = first - relativedelta(days=1)
    return first, last, prev_first, prev_last


def get_client():
    creds = service_account.Credentials.from_service_account_file(
        CONFIG["credentials_path"],
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    return BetaAnalyticsDataClient(credentials=creds)

def run_report(client, dimensions, metrics, date_ranges, dimension_filter=None, limit=50):
    req = RunReportRequest(
        property=f"properties/{CONFIG['property_id']}",
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=date_ranges,
        dimension_filter=dimension_filter,
        limit=limit,
    )
    response = client.run_report(req)
    rows = []
    for row in response.rows:
        r = {dimensions[i]: row.dimension_values[i].value for i in range(len(dimensions))}
        r.update({metrics[i]: row.metric_values[i].value for i in range(len(metrics))})
        rows.append(r)
    return pd.DataFrame(rows)

def hostname_filter(hostname):
    return FilterExpression(
        filter=Filter(
            field_name="hostName",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.CONTAINS,
                value=hostname
            )
        )
    )


def fetch_overview(client, start, end, prev_start, prev_end):
    """Active users, new users, sessions, engagement rate for current + previous month."""
    current_range = DateRange(start_date=str(start), end_date=str(end))
    prev_range = DateRange(start_date=str(prev_start), end_date=str(prev_end))
    df = run_report(client, ["date"], 
                    ["activeUsers", "newUsers", "sessions", "engagementRate", "averageSessionDuration"],
                    [current_range])
    df_prev = run_report(client, ["date"],
                         ["activeUsers", "newUsers", "sessions", "engagementRate"],
                         [prev_range])
    return df, df_prev

def fetch_by_ecosystem(client, start, end, prev_start, prev_end):
    """Summary metrics per ecosystem."""
    results = []
    prev_results = []
    current_range = DateRange(start_date=str(start), end_date=str(end))
    prev_range = DateRange(start_date=str(prev_start), end_date=str(prev_end))

    for name, hostname in ECOSYSTEMS.items():
        filt = hostname_filter(hostname)
        df = run_report(client, ["hostName"],
                        ["activeUsers", "newUsers", "sessions", "engagementRate", "averageSessionDuration"],
                        [current_range], dimension_filter=filt, limit=1)
        if not df.empty:
            row = df.iloc[0].to_dict()
            row["ecosystem"] = name
            results.append(row)
        else:
            results.append({"ecosystem": name, "activeUsers": "0", "newUsers": "0",
                            "sessions": "0", "engagementRate": "0", "averageSessionDuration": "0"})

        df_p = run_report(client, ["hostName"],
                          ["activeUsers", "sessions"],
                          [prev_range], dimension_filter=filt, limit=1)
        if not df_p.empty:
            row_p = df_p.iloc[0].to_dict()
            row_p["ecosystem"] = name
            prev_results.append(row_p)
        else:
            prev_results.append({"ecosystem": name, "activeUsers": "0", "sessions": "0"})

    df_all = pd.DataFrame(results)
    df_prev_all = pd.DataFrame(prev_results)
    for col in ["activeUsers", "newUsers", "sessions", "averageSessionDuration"]:
        df_all[col] = pd.to_numeric(df_all[col], errors="coerce").fillna(0).astype(int)
    df_all["engagementRate"] = pd.to_numeric(df_all["engagementRate"], errors="coerce").fillna(0)
    df_all["engagementRate_pct"] = (df_all["engagementRate"] * 100).round(1)
    return df_all, df_prev_all

def fetch_channels(client, start, end):
    """Sessions by channel group × ecosystem."""
    results = []
    for name, hostname in ECOSYSTEMS.items():
        filt = hostname_filter(hostname)
        df = run_report(client, ["sessionDefaultChannelGroup"],
                        ["sessions", "activeUsers", "engagedSessions"],
                        [DateRange(start_date=str(start), end_date=str(end))],
                        dimension_filter=filt, limit=20)
        if not df.empty:
            df["ecosystem"] = name
            results.append(df)
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

def fetch_source_medium(client, start, end):
    """Source/medium breakdown across all ecosystems."""
    df = run_report(client, ["sessionSourceMedium"],
                    ["activeUsers", "sessions", "engagedSessions", "engagementRate"],
                    [DateRange(start_date=str(start), end_date=str(end))],
                    limit=30)
    if not df.empty:
        for col in ["activeUsers", "sessions", "engagedSessions"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        df["engagementRate"] = (pd.to_numeric(df["engagementRate"], errors="coerce").fillna(0) * 100).round(1)
        df = df.sort_values("sessions", ascending=False)
    return df

def fetch_top_pages(client, start, end):
    """Top pages per ecosystem."""
    results = []
    for name, hostname in ECOSYSTEMS.items():
        filt = hostname_filter(hostname)
        df = run_report(client, ["pageTitle"],
                        ["screenPageViews", "activeUsers"],
                        [DateRange(start_date=str(start), end_date=str(end))],
                        dimension_filter=filt, limit=5)
        if not df.empty:
            df["ecosystem"] = name
            results.append(df)
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()


def fig_to_base64(fig):
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

def chart_ecosystem_bar(df_eco):
    fig, ax = plt.subplots(figsize=(9, 4))
    names = [e.replace("E: ", "") for e in df_eco["ecosystem"]]
    sessions = df_eco["sessions"].values
    bars = ax.barh(names, sessions, color=ECOSYSTEM_COLORS[:len(names)])
    ax.set_xlabel("Sessions", fontsize=10)
    ax.set_title("Sessions by Ecosystem", fontsize=12, fontweight="bold")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    for bar, val in zip(bars, sessions):
        ax.text(bar.get_width() + max(sessions) * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    return fig_to_base64(fig)

def chart_channel_breakdown(df_channels):
    pivot = df_channels.groupby("sessionDefaultChannelGroup")["sessions"].sum()
    pivot = pivot.sort_values(ascending=True).tail(8)
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.barh(pivot.index, pivot.values, color="#0077B6")
    ax.set_xlabel("Sessions")
    ax.set_title("Sessions by Channel Group", fontsize=12, fontweight="bold")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    return fig_to_base64(fig)

def chart_overview_trend(df_daily):
    df_daily = df_daily.copy()
    df_daily["date"] = pd.to_datetime(df_daily["date"], format="%Y%m%d")
    df_daily = df_daily.sort_values("date")
    df_daily["activeUsers"] = pd.to_numeric(df_daily["activeUsers"], errors="coerce").fillna(0)
    fig, ax = plt.subplots(figsize=(10, 3))
    ax.plot(df_daily["date"], df_daily["activeUsers"], color="#0077B6", linewidth=2)
    ax.fill_between(df_daily["date"], df_daily["activeUsers"], alpha=0.1, color="#0077B6")
    ax.set_title("Daily Active Users", fontsize=12, fontweight="bold")
    ax.set_ylabel("Active Users")
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%b %d"))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    return fig_to_base64(fig)


def mom_delta(current, previous):
    try:
        c, p = float(current), float(previous)
        if p == 0:
            return "—"
        delta = ((c - p) / p) * 100
        arrow = "▲" if delta >= 0 else "▼"
        color = "green" if delta >= 0 else "red"
        return f'<span style="color:{color}">{arrow} {abs(delta):.1f}%</span>'
    except:
        return "—"


def export_excel(df_eco, df_channels, df_source, df_pages, month_label, output_dir):
    path = os.path.join(output_dir, f"ga4_report_{month_label}.xlsx")
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        # Sheet 1: Ecosystem summary
        eco_export = df_eco[["ecosystem", "activeUsers", "newUsers", "sessions", "engagementRate_pct", "averageSessionDuration"]].copy()
        eco_export.columns = ["Ecosystem", "Active Users", "New Users", "Sessions", "Engagement Rate %", "Avg Session Duration (s)"]
        eco_export.to_excel(writer, sheet_name="Ecosystem Summary", index=False)

        # Sheet 2: Channels
        if not df_channels.empty:
            ch_export = df_channels.groupby(["ecosystem", "sessionDefaultChannelGroup"])["sessions"].sum().reset_index()
            ch_export.columns = ["Ecosystem", "Channel", "Sessions"]
            ch_export.to_excel(writer, sheet_name="Channels by Ecosystem", index=False)

        # Sheet 3: Source Medium
        if not df_source.empty:
            sm_export = df_source[["sessionSourceMedium", "activeUsers", "sessions", "engagedSessions", "engagementRate"]].copy()
            sm_export.columns = ["Source / Medium", "Active Users", "Sessions", "Engaged Sessions", "Engagement Rate %"]
            sm_export.to_excel(writer, sheet_name="Source Medium", index=False)

        # Sheet 4: Top Pages
        if not df_pages.empty:
            pg_export = df_pages[["ecosystem", "pageTitle", "screenPageViews", "activeUsers"]].copy()
            pg_export.columns = ["Ecosystem", "Page Title", "Page Views", "Active Users"]
            pg_export.to_excel(writer, sheet_name="Top Pages", index=False)

    print(f"  ✓ Excel saved: {path}")
    return path


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>GA4 Monthly Report — {{ month_label }}</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; 
         margin: 0; background: #f4f6f9; color: #1a1a2e; }
  .container { max-width: 1100px; margin: auto; padding: 32px 24px; }
  h1 { font-size: 28px; color: #0077B6; margin-bottom: 4px; }
  .subtitle { color: #666; font-size: 14px; margin-bottom: 32px; }
  
  /* KPI cards */
  .kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 32px; }
  .kpi-card { background: white; border-radius: 12px; padding: 20px; 
               box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
  .kpi-label { font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; }
  .kpi-value { font-size: 32px; font-weight: 700; color: #0077B6; margin: 6px 0 4px; }
  .kpi-delta { font-size: 13px; }

  /* Sections */
  .section { background: white; border-radius: 12px; padding: 24px; 
              box-shadow: 0 2px 8px rgba(0,0,0,0.06); margin-bottom: 24px; }
  .section h2 { font-size: 18px; margin: 0 0 20px; color: #1a1a2e; border-bottom: 2px solid #e8f4f8; padding-bottom: 10px; }
  
  /* Charts side by side */
  .charts-row { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 24px; }
  .chart-box { background: white; border-radius: 12px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
  img.chart { width: 100%; border-radius: 6px; }

  /* Ecosystem table */
  table { width: 100%; border-collapse: collapse; font-size: 14px; }
  th { background: #f0f8ff; color: #0077B6; font-weight: 600; padding: 10px 12px; text-align: left; }
  td { padding: 9px 12px; border-bottom: 1px solid #f0f0f0; }
  tr:hover td { background: #f9fcff; }
  .eco-name { font-weight: 600; color: #0077B6; }
  
  /* Source medium table */
  .sm-table th { background: #f0f0f0; color: #333; }
  
  /* Top pages per ecosystem */
  .pages-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  .pages-box { background: #f9fcff; border-radius: 8px; padding: 16px; }
  .pages-box h3 { font-size: 13px; color: #0077B6; margin: 0 0 10px; }
  .pages-box ol { margin: 0; padding-left: 18px; }
  .pages-box li { font-size: 13px; margin-bottom: 5px; color: #333; }
  .pages-box .views { color: #888; font-size: 12px; }
  
  footer { text-align: center; color: #aaa; font-size: 12px; margin-top: 32px; padding-bottom: 32px; }
</style>
</head>
<body>
<div class="container">
  <h1>📊 GA4 Monthly Report</h1>
  <p class="subtitle">Synapse Connect — All Ecosystems &nbsp;|&nbsp; {{ month_label }} &nbsp;|&nbsp; Generated {{ generated_at }}</p>

  <!-- KPI Summary -->
  <div class="kpi-grid">
    <div class="kpi-card">
      <div class="kpi-label">Active Users</div>
      <div class="kpi-value">{{ totals.activeUsers }}</div>
      <div class="kpi-delta">{{ totals.activeUsers_delta }} vs last month</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">New Users</div>
      <div class="kpi-value">{{ totals.newUsers }}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Sessions</div>
      <div class="kpi-value">{{ totals.sessions }}</div>
      <div class="kpi-delta">{{ totals.sessions_delta }} vs last month</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Avg Engagement Rate</div>
      <div class="kpi-value">{{ totals.engagementRate }}%</div>
    </div>
  </div>

  <!-- Trend + Ecosystem bar -->
  <div class="charts-row">
    <div class="chart-box">
      <img class="chart" src="data:image/png;base64,{{ chart_trend }}" alt="Daily Active Users">
    </div>
    <div class="chart-box">
      <img class="chart" src="data:image/png;base64,{{ chart_ecosystem }}" alt="Sessions by Ecosystem">
    </div>
  </div>

  <!-- Ecosystem Summary Table -->
  <div class="section">
    <h2>Ecosystem Breakdown</h2>
    <table>
      <thead>
        <tr>
          <th>Ecosystem</th>
          <th>Active Users</th>
          <th>New Users</th>
          <th>Sessions</th>
          <th>Engagement Rate</th>
          <th>Avg Session (s)</th>
        </tr>
      </thead>
      <tbody>
        {% for row in ecosystem_rows %}
        <tr>
          <td class="eco-name">{{ row.ecosystem }}</td>
          <td>{{ "{:,}".format(row.activeUsers) }}</td>
          <td>{{ "{:,}".format(row.newUsers) }}</td>
          <td>{{ "{:,}".format(row.sessions) }}</td>
          <td>{{ row.engagementRate_pct }}%</td>
          <td>{{ row.averageSessionDuration }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>

  <!-- Channel Breakdown -->
  <div class="charts-row">
    <div class="chart-box">
      <img class="chart" src="data:image/png;base64,{{ chart_channels }}" alt="Sessions by Channel">
    </div>
    <div class="section" style="margin:0">
      <h2>Source / Medium</h2>
      <table class="sm-table">
        <thead>
          <tr>
            <th>Source / Medium</th>
            <th>Active Users</th>
            <th>Sessions</th>
            <th>Engagement Rate</th>
          </tr>
        </thead>
        <tbody>
          {% for row in source_rows %}
          <tr>
            <td>{{ row.sessionSourceMedium }}</td>
            <td>{{ "{:,}".format(row.activeUsers) }}</td>
            <td>{{ "{:,}".format(row.sessions) }}</td>
            <td>{{ row.engagementRate }}%</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </div>

  <!-- Top Pages -->
  <div class="section">
    <h2>Top Pages per Ecosystem</h2>
    <div class="pages-grid">
      {% for eco, pages in top_pages.items() %}
      <div class="pages-box">
        <h3>{{ eco }}</h3>
        <ol>
          {% for p in pages %}
          <li>{{ p.pageTitle[:55] }}{% if p.pageTitle|length > 55 %}…{% endif %}
            <span class="views"> — {{ "{:,}".format(p.screenPageViews) }} views</span>
          </li>
          {% endfor %}
        </ol>
      </div>
      {% endfor %}
    </div>
  </div>

  <footer>Generated by ga4_monthly_report.py &nbsp;|&nbsp; Data source: Google Analytics 4</footer>
</div>
</body>
</html>
"""


def main():
    print("━" * 50)
    print("  GA4 Monthly Ecosystem Report Generator")
    print("━" * 50)

    start, end, prev_start, prev_end = get_report_dates()
    month_label = start.strftime("%B %Y")
    file_label = start.strftime("%Y-%m")
    print(f"  Report period: {start} → {end}")
    print(f"  Comparison:    {prev_start} → {prev_end}")

    client = get_client()
    print("\n  Fetching data from GA4...")

    print("  → Overview trend...")
    df_daily, df_daily_prev = fetch_overview(client, start, end, prev_start, prev_end)

    print("  → Ecosystem breakdown...")
    df_eco, df_eco_prev = fetch_by_ecosystem(client, start, end, prev_start, prev_end)

    print("  → Channel groups...")
    df_channels = fetch_channels(client, start, end)

    print("  → Source / Medium...")
    df_source = fetch_source_medium(client, start, end)

    print("  → Top pages...")
    df_pages = fetch_top_pages(client, start, end)

    print("\n  Building charts...")
    chart_trend = chart_overview_trend(df_daily)
    chart_ecosystem = chart_ecosystem_bar(df_eco)
    chart_channels_img = chart_channel_breakdown(df_channels) if not df_channels.empty else ""

    # Totals
    total_active = df_eco["activeUsers"].sum()
    total_new = df_eco["newUsers"].sum()
    total_sessions = df_eco["sessions"].sum()
    avg_eng = df_eco["engagementRate_pct"].mean().round(1)

    prev_active = pd.to_numeric(df_eco_prev["activeUsers"], errors="coerce").fillna(0).sum()
    prev_sessions = pd.to_numeric(df_eco_prev["sessions"], errors="coerce").fillna(0).sum()

    totals = {
        "activeUsers": f"{total_active:,}",
        "newUsers": f"{total_new:,}",
        "sessions": f"{total_sessions:,}",
        "engagementRate": avg_eng,
        "activeUsers_delta": mom_delta(total_active, prev_active),
        "sessions_delta": mom_delta(total_sessions, prev_sessions),
    }

    # Top pages dict
    top_pages_dict = {}
    if not df_pages.empty:
        df_pages["screenPageViews"] = pd.to_numeric(df_pages["screenPageViews"], errors="coerce").fillna(0).astype(int)
        for eco in df_eco["ecosystem"]:
            subset = df_pages[df_pages["ecosystem"] == eco].head(5)
            if not subset.empty:
                top_pages_dict[eco.replace("E: ", "")] = subset.to_dict("records")

    # Source rows (top 10)
    source_rows = df_source.head(10).to_dict("records") if not df_source.empty else []

    print("  Rendering HTML report...")
    tmpl = Template(HTML_TEMPLATE)
    html = tmpl.render(
        month_label=month_label,
        generated_at=datetime.now().strftime("%d %b %Y, %H:%M"),
        totals=totals,
        chart_trend=chart_trend,
        chart_ecosystem=chart_ecosystem,
        chart_channels=chart_channels_img,
        ecosystem_rows=df_eco.to_dict("records"),
        source_rows=source_rows,
        top_pages=top_pages_dict,
    )

    html_path = os.path.join(CONFIG["output_dir"], f"ga4_report_{file_label}.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  ✓ HTML saved: {html_path}")

    print("  Exporting Excel...")
    export_excel(df_eco, df_channels, df_source, df_pages, file_label, CONFIG["output_dir"])

    print("\n━" * 50)
    print(f"  ✅ Done! Files saved to: {os.path.abspath(CONFIG['output_dir'])}")
    print("━" * 50)


if __name__ == "__main__":
    main()