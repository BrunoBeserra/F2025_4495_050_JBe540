# --------------------------------------------------------------------------------- IMPORT LIBARIES

import os
import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import plotly.io as pio
from databricks import sql
from dotenv import load_dotenv

# --------------------------------------------------------------------------------- LOAD .env VARAIBLES

# Load environment variables
load_dotenv()

DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# Safety check for ennvironment error
required_vars = {
    "DATABRICKS_SERVER_HOSTNAME": DATABRICKS_SERVER_HOSTNAME,
    "DATABRICKS_HTTP_PATH": DATABRICKS_HTTP_PATH,
    "DATABRICKS_TOKEN": DATABRICKS_TOKEN,
}
missing = [name for name, value in required_vars.items() if not value]
if missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

# --------------------------------------------------------------------------------- GET DATA FROM DATABRICKS

def load_hr_data() -> pd.DataFrame:
    """Connect to Databricks, run the HR query, and return a cleaned DataFrame."""
    query = """
        SELECT *
        FROM (
            SELECT
                f.employee_id,
                f.full_name,
                d.name  AS Department,
                j.name  AS Job_Title,
                f.hire_date AS Hire_Date,
                l.name  AS Location,
                f.performance_rating AS Performance_Rating,
                f.experience_years AS Experience_Years,
                s.name  AS Status,
                w.name  AS Work_Mode,
                f.annual_salary AS Salary_INR,
                jl.name AS Job_Level,
                ROW_NUMBER() OVER (PARTITION BY f.employee_id ORDER BY f.hire_date DESC) AS rn
            FROM workspace.applied_research_gold.fact_table_gold_hr_data AS f
                LEFT JOIN workspace.applied_research_gold.dim_department_gold AS d ON f.department_id = d.id
                LEFT JOIN workspace.applied_research_gold.dim_job_title_gold  AS j ON f.job_title_id  = j.id
                LEFT JOIN workspace.applied_research_gold.dim_location_gold   AS l ON f.location_id   = l.id
                LEFT JOIN workspace.applied_research_gold.dim_status_gold     AS s ON f.status_id     = s.id
                LEFT JOIN workspace.applied_research_gold.dim_work_mode_gold  AS w ON f.work_mode_id  = w.id
                LEFT JOIN workspace.applied_research_gold.dim_job_level_gold  AS jl ON f.job_level_id = jl.id
        ) t
        WHERE rn = 1
    """

    with sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        df = pd.read_sql(query, conn)

    return df

# Load dataframe
df = load_hr_data()


# --------------------------------------------------------------------------------- DATA CLEANING AND FEATURE ENGINEERIN

df["Status"] = df["Status"].fillna("Active")
df["Department"] = df["Department"].fillna("Unknown")
df["Job_Level"] = df["Job_Level"].fillna("Unknown")
df["Job_Title"] = df["Job_Title"].fillna("Unknown")

df["Performance_Rating"] = pd.to_numeric(df["Performance_Rating"], errors="coerce")
df["Experience_Years"] = pd.to_numeric(df["Experience_Years"], errors="coerce")

df["Salary_INR"] = pd.to_numeric(df["Salary_INR"], errors="coerce")
INR_TO_USD = 1 / 83
df["Salary_USD"] = df["Salary_INR"] * INR_TO_USD


df["Hire_Date"] = pd.to_datetime(df["Hire_Date"], errors="coerce")

departments = sorted(df["Department"].dropna().unique())


# --------------------------------------------------------------------------------- GLOBAL PLOTLY THEME

px.defaults.template = "plotly_white"
px.defaults.color_continuous_scale = "Viridis"
px.defaults.color_discrete_sequence = ["#2E86C1", "#28B463", "#AF7AC5", "#F5B041", "#E74C3C"]

pio.templates.default = "plotly_white"
pio.templates["plotly_white"].layout.font.family = "Segoe UI"
pio.templates["plotly_white"].layout.font.size = 13
pio.templates["plotly_white"].layout.title.font.size = 20
pio.templates["plotly_white"].layout.title.font.family = "Segoe UI"



# --------------------------------------------------------------------------------- DASH APP LAYOUT

app = Dash(
    __name__,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
app.title = "HR Analytics Dashboard"

app.layout = html.Div(
    [
        html.H2(
            "HR Analytics Dashboard",
            style={"textAlign": "center", "marginTop": "20px"},
        ),

        # KPI summary header
        html.Div(
            [
                html.Div(
                    [
                        html.Div("👥", style={"fontSize": "28px"}),
                        html.H4(
                            "Total Employees",
                            style={"color": "#666", "marginBottom": "0"},
                        ),
                        html.H3(id="kpi-total-employees", style={"color": "#2E86C1"}),
                    ],
                    style={"width": "30%", "textAlign": "center"},
                ),
                html.Div(
                    [
                        html.Div("💰", style={"fontSize": "28px"}),
                        html.H4(
                            "Average Salary ($)",
                            style={"color": "#666", "marginBottom": "0"},
                        ),
                        html.H3(id="kpi-average-salary", style={"color": "#28B463"}),
                    ],
                    style={"width": "30%", "textAlign": "center"},
                ),
                html.Div(
                    [
                        html.Div("⏳", style={"fontSize": "28px"}),
                        html.H4(
                            "Average Experience (Years)",
                            style={"color": "#666", "marginBottom": "0"},
                        ),
                        html.H3(
                            id="kpi-average-experience", style={"color": "#AF7AC5"}
                        ),
                    ],
                    style={"width": "30%", "textAlign": "center"},
                ),
            ],
            style={
                "display": "flex",
                "justifyContent": "space-around",
                "alignItems": "center",
                "margin": "20px auto",
                "backgroundColor": "#fdfdfd",
                "padding": "20px",
                "borderRadius": "12px",
                "boxShadow": "0 2px 8px rgba(0,0,0,0.1)",
                "width": "95%",
            },
        ),

        # Dashboard tabs
        dcc.Tabs(
            [
                # Tab 1 — Turnover and retention
                dcc.Tab(
                    label="Employee Turnover & Retention",
                    children=[
                        html.Div(
                            [
                                html.Label(
                                    "Select Department:",
                                    style={"fontWeight": "bold"},
                                ),
                                dcc.Dropdown(
                                    id="dept-dropdown",
                                    options=[
                                        {"label": dept, "value": dept}
                                        for dept in departments
                                    ],
                                    placeholder="All Departments",
                                    clearable=True,
                                    style={"width": "50%", "marginBottom": "10px"},
                                ),
                                html.Div(
                                    id="kpi-text",
                                    style={
                                        "textAlign": "center",
                                        "fontSize": "18px",
                                        "marginBottom": "20px",
                                        "color": "#444",
                                    },
                                ),
                                dcc.Graph(id="turnover-chart"),
                            ],
                            style={
                                "backgroundColor": "white",
                                "padding": "20px",
                                "margin": "20px",
                                "borderRadius": "10px",
                                "boxShadow": "0 2px 6px rgba(0,0,0,0.1)",
                            },
                        )
                    ],
                ),

                # Tab 2 — Salary and compensation
                dcc.Tab(
                    label="Salary and Compensation Trends",
                    children=[
                        html.Div(
                            [
                                html.Label(
                                    "Select Department:",
                                    style={"fontWeight": "bold"},
                                ),
                                dcc.Dropdown(
                                    id="salary-dept-dropdown",
                                    options=[
                                        {"label": dept, "value": dept}
                                        for dept in departments
                                    ],
                                    placeholder="All Departments",
                                    clearable=True,
                                    style={"width": "50%", "marginBottom": "10px"},
                                ),
                                dcc.Graph(id="salary-chart"),
                            ],
                            style={
                                "backgroundColor": "white",
                                "padding": "20px",
                                "margin": "20px",
                                "borderRadius": "10px",
                                "boxShadow": "0 2px 6px rgba(0,0,0,0.1)",
                            },
                        )
                    ],
                ),

                # Tab 3 — Performance vs experience
                dcc.Tab(
                    label="Performance and Experience Relationship",
                    children=[
                        html.Div(
                            [
                                html.Label(
                                    "Select Department:",
                                    style={"fontWeight": "bold"},
                                ),
                                dcc.Dropdown(
                                    id="exp-perf-dropdown",
                                    options=[
                                        {"label": dept, "value": dept}
                                        for dept in departments
                                    ],
                                    placeholder="All Departments",
                                    clearable=True,
                                    style={"width": "50%", "marginBottom": "10px"},
                                ),
                                dcc.Checklist(
                                    id="trendline-toggle",
                                    options=[
                                        {"label": "Show Trendline", "value": "show"}
                                    ],
                                    value=[],
                                    inline=True,
                                    style={
                                        "fontSize": "14px",
                                        "marginBottom": "15px",
                                    },
                                ),
                                dcc.Graph(id="exp-perf-chart"),
                            ],
                            style={
                                "backgroundColor": "white",
                                "padding": "20px",
                                "margin": "20px",
                                "borderRadius": "10px",
                                "boxShadow": "0 2px 6px rgba(0,0,0,0.1)",
                            },
                        )
                    ],
                ),

                # Tab 4 — Workforce demographics
                dcc.Tab(
                    label="Workforce Demographics and Headcount",
                    children=[
                        html.Div(
                            [
                                html.Label(
                                    "Select Work Mode:",
                                    style={"fontWeight": "bold"},
                                ),
                                dcc.Dropdown(
                                    id="workmode-dropdown",
                                    options=[
                                        {
                                            "label": wm,
                                            "value": wm,
                                        }
                                        for wm in sorted(
                                            df["Work_Mode"].dropna().unique()
                                        )
                                    ],
                                    placeholder="All Work Modes",
                                    clearable=True,
                                    style={"width": "50%", "marginBottom": "10px"},
                                ),
                                html.Div(
                                    [
                                        dcc.Graph(
                                            id="headcount-chart",
                                            style={
                                                "width": "60%",
                                                "display": "inline-block",
                                            },
                                        ),
                                        dcc.Graph(
                                            id="workmode-pie",
                                            style={
                                                "width": "38%",
                                                "display": "inline-block",
                                                "float": "right",
                                            },
                                        ),
                                    ]
                                ),
                            ],
                            style={
                                "backgroundColor": "white",
                                "padding": "20px",
                                "margin": "20px",
                                "borderRadius": "10px",
                                "boxShadow": "0 2px 6px rgba(0,0,0,0.1)",
                            },
                        )
                    ],
                ),

                # Tab 5 — Promotion and career progression
                dcc.Tab(
                    label="Promotion and Career Progression",
                    children=[
                        html.Div(
                            [
                                html.Label(
                                    "Select Department:",
                                    style={"fontWeight": "bold"},
                                ),
                                dcc.Dropdown(
                                    id="promotion-dept-dropdown",
                                    options=[
                                        {"label": dept, "value": dept}
                                        for dept in departments
                                    ],
                                    placeholder="All Departments",
                                    clearable=True,
                                    style={"width": "50%", "marginBottom": "10px"},
                                ),
                                html.Div(
                                    [
                                        dcc.Graph(
                                            id="promotion-chart",
                                            style={
                                                "width": "55%",
                                                "display": "inline-block",
                                            },
                                        ),
                                        dcc.Graph(
                                            id="career-path-chart",
                                            style={
                                                "width": "43%",
                                                "display": "inline-block",
                                                "float": "right",
                                            },
                                        ),
                                    ]
                                ),
                            ],
                            style={
                                "backgroundColor": "white",
                                "padding": "20px",
                                "margin": "20px",
                                "borderRadius": "10px",
                                "boxShadow": "0 2px 6px rgba(0,0,0,0.1)",
                            },
                        )
                    ],
                ),
            ]
        ),
    ],
    style={
        "backgroundColor": "#f5f6fa",
        "paddingBottom": "40px",
        "fontFamily": "Segoe UI",
    },
)


# --------------------------------------------------------------------------------- DASH CALLBACKS

# Summary KPI callback
@app.callback(
    [
        Output("kpi-total-employees", "children"),
        Output("kpi-average-salary", "children"),
        Output("kpi-average-experience", "children"),
    ],
    Input("dept-dropdown", "value"),
)
def update_summary_kpis(selected_dept):
    df_temp = df
    if selected_dept:
        df_temp = df_temp[df_temp["Department"] == selected_dept]

    total_employees = len(df_temp)
    avg_salary = df_temp["Salary_USD"].mean()
    avg_experience = df_temp["Experience_Years"].mean()

    return (
        f"{total_employees:,}",
        f"${avg_salary:,.0f}",
        f"{avg_experience:.1f}",
    )


# Callback 1 — Turnover and Retention
@app.callback(
    [Output("turnover-chart", "figure"), Output("kpi-text", "children")],
    Input("dept-dropdown", "value"),
)
def update_turnover_chart(selected_dept):
    df_temp = df
    if selected_dept:
        df_temp = df_temp[df_temp["Department"] == selected_dept]
        group_field = "Job_Level"
        title = f"Turnover Rate by Job Level – {selected_dept}"
    else:
        group_field = "Department"
        title = "Turnover Rate by Department (Overall)"

    if len(df_temp) == 0:
        return px.bar(title=title), "No data available."

    turnover_data = (
        df_temp.groupby(group_field)
        .agg(
            Total=("Status", "size"),
            Resigned=(
                "Status",
                lambda s: (s.str.lower() == "resigned").sum(),
            ),
        )
        .assign(Turnover_Rate=lambda x: x["Resigned"] / x["Total"] * 100)
        .reset_index()[[group_field, "Turnover_Rate"]]
    )

    total_turnover = (
        (df_temp["Status"].str.lower() == "resigned").sum()
        / len(df_temp)
        * 100
    )
    kpi_text = f"Overall Turnover Rate: {total_turnover:.1f}%"

    fig = px.bar(
        turnover_data,
        x=group_field,
        y="Turnover_Rate",
        text=turnover_data["Turnover_Rate"].map("{:.1f}%".format),
        color="Turnover_Rate",
        title=title,
    )
    fig.update_layout(title_x=0.5, margin=dict(l=60, r=40, t=80, b=60))
    return fig, kpi_text


# Callback 2 — Salary and Compensation Trends
@app.callback(
    Output("salary-chart", "figure"),
    Input("salary-dept-dropdown", "value"),
)
def update_salary_chart(selected_dept):
    df_temp = df
    if selected_dept:
        df_temp = df_temp[df_temp["Department"] == selected_dept]
        df_salary = (
            df_temp.groupby(df_temp["Hire_Date"].dt.to_period("Y"))["Salary_USD"]
            .mean()
            .reset_index()
        )
        df_salary["Hire_Date"] = df_salary["Hire_Date"].astype(str)

        fig = px.line(
            df_salary,
            x="Hire_Date",
            y="Salary_USD",
            markers=True,
            title=f"Average Salary Over Time – {selected_dept}",
        )
    else:
        df_salary = (
            df.groupby("Department")["Salary_USD"]
            .mean()
            .reset_index()
            .sort_values("Salary_USD", ascending=False)
        )
        fig = px.bar(
            df_salary,
            x="Department",
            y="Salary_USD",
            text=df_salary["Salary_USD"].map("${:,.0f}".format),
            color="Salary_USD",
            title="Average Salary by Department",
        )

    fig.update_layout(title_x=0.5, margin=dict(l=60, r=40, t=80, b=60))
    return fig


# Callback 3 — Performance and Experience Relationship
@app.callback(
    Output("exp-perf-chart", "figure"),
    [
        Input("exp-perf-dropdown", "value"),
        Input("trendline-toggle", "value"),
    ],
)
def update_exp_perf_chart(selected_dept, trendline_toggle):
    df_temp = df
    if selected_dept:
        df_temp = df_temp[df_temp["Department"] == selected_dept]
        title = f"Experience vs Performance – {selected_dept}"
    else:
        title = "Experience vs Performance (All Departments)"

    grouped = (
        df_temp.groupby("Job_Title")
        .agg(
            Experience_Years=("Experience_Years", "mean"),
            Performance_Rating=("Performance_Rating", "mean"),
            Salary_USD=("Salary_USD", "mean"),
            Job_Level=("Job_Level", "first"),
            Department=("Department", "first"),
        )
        .reset_index()
    )

    trendline_opt = "ols" if "show" in (trendline_toggle or []) else None

    fig = px.scatter(
        grouped,
        x="Experience_Years",
        y="Performance_Rating",
        color="Job_Level" if selected_dept else "Department",
        hover_name="Job_Title",
        size="Salary_USD",
        trendline=trendline_opt,
        title=title,
    )
    fig.update_layout(
        title_x=0.5,
        margin=dict(l=60, r=40, t=80, b=60),
        xaxis_title="Years of Experience",
        yaxis_title="Performance Rating (1–5)",
    )
    return fig


# Callback 4 — Workforce Demographics and Headcount
@app.callback(
    [
        Output("headcount-chart", "figure"),
        Output("workmode-pie", "figure"),
    ],
    Input("workmode-dropdown", "value"),
)
def update_workforce_charts(selected_workmode):
    df_temp = df
    if selected_workmode:
        df_temp = df_temp[df_temp["Work_Mode"] == selected_workmode]
        title_suffix = f" – {selected_workmode}"
    else:
        title_suffix = ""

    if len(df_temp) == 0:
        return px.bar(title=f"Headcount by Department{title_suffix}"), px.pie(
            title=f"Work Mode Distribution{title_suffix}"
        )

    dept_counts = (
        df_temp.groupby("Department")
        .size()
        .reset_index(name="Headcount")
    )
    workmode_counts = (
        df_temp.groupby("Work_Mode")
        .size()
        .reset_index(name="Count")
    )

    fig_bar = px.bar(
        dept_counts,
        x="Department",
        y="Headcount",
        text="Headcount",
        color="Headcount",
        title=f"Headcount by Department{title_suffix}",
    )
    fig_bar.update_layout(title_x=0.5, margin=dict(l=60, r=40, t=80, b=60))

    fig_pie = px.pie(
        workmode_counts,
        names="Work_Mode",
        values="Count",
        hole=0.4,
        title=f"Work Mode Distribution{title_suffix}",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_pie.update_layout(title_x=0.5, margin=dict(l=20, r=20, t=80, b=60))

    return fig_bar, fig_pie


# Callback 5 — Promotion and Career Progression
@app.callback(
    [
        Output("promotion-chart", "figure"),
        Output("career-path-chart", "figure"),
    ],
    Input("promotion-dept-dropdown", "value"),
)
def update_promotion_charts(selected_dept):
    df_temp = df
    if selected_dept:
        df_temp = df_temp[df_temp["Department"] == selected_dept]
        title_suffix = f" – {selected_dept}"
    else:
        title_suffix = ""

    if len(df_temp) == 0:
        return px.bar(
            title=f"Employee Distribution by Job Level{title_suffix}"
        ), px.line(
            title=f"Average Experience by Job Level{title_suffix}"
        )

    promotion_counts = (
        df_temp.groupby("Job_Level")
        .size()
        .reset_index(name="Employee_Count")
    )

    career_path = (
        df_temp.groupby("Job_Level")["Experience_Years"]
        .mean()
        .reset_index()
        .sort_values("Experience_Years")
    )

    fig_bar = px.bar(
        promotion_counts,
        x="Job_Level",
        y="Employee_Count",
        text="Employee_Count",
        color="Employee_Count",
        title=f"Employee Distribution by Job Level{title_suffix}",
    )
    fig_bar.update_layout(title_x=0.5, margin=dict(l=60, r=40, t=80, b=60))

    fig_line = px.line(
        career_path,
        x="Job_Level",
        y="Experience_Years",
        markers=True,
        title=f"Average Experience by Job Level{title_suffix}",
        color_discrete_sequence=["#2E86C1"],
    )
    fig_line.update_layout(title_x=0.5, margin=dict(l=60, r=40, t=80, b=60))

    return fig_bar, fig_line


# --------------------------------------------------------------------------------- RUN SERVER

if __name__ == "__main__":
    app.run(debug=True)