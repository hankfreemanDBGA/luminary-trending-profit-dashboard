"""
Luminary Insurance Weekly Profit Trending Dashboard
Streamlit Cloud deployment version with Snowflake connection via secrets
"""

import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta
from snowflake.snowpark import Session

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE CONFIGURATION & STYLING
# ═══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Luminary Profit Dashboard",
    page_icon="📊",
    layout="wide"
)

# Custom CSS for styling
st.markdown("""
<style>
    .main .block-container {
        padding: 1.5rem 2rem;
        max-width: 1400px;
    }
    
    .dashboard-title {
        font-size: 2.25rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 0.25rem;
    }
    
    .dashboard-subtitle {
        color: #6b7280;
        font-size: 1rem;
        margin-bottom: 1.5rem;
    }
    
    .section-header {
        font-size: 1.1rem;
        font-weight: 600;
        color: #374151;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #3b82f6;
        margin-bottom: 1rem;
        margin-top: 1.5rem;
    }
    
    .profit-equation {
        background: #f8fafc;
        padding: 1.25rem;
        border-radius: 8px;
        border-left: 4px solid #8b5cf6;
        font-family: monospace;
        font-size: 0.85rem;
        line-height: 1.7;
        color: #1e293b;
    }
    
    .metric-card {
        background: #ffffff;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS & CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

DEFAULT_COMMISSION_MULTIPLIER = 1.38
DEFAULT_REALIZATION_RATE = 0.60
DEFAULT_TENURED_DAILY_SALARY = 150
DEFAULT_TRAINING_DAILY_SALARY = 300
DEFAULT_OVERHEAD = 25000
DEFAULT_NON_SALES_SALARY = 14500


# ═══════════════════════════════════════════════════════════════════════════════
# SNOWFLAKE CONNECTION
# ═══════════════════════════════════════════════════════════════════════════════

def get_session():
    """Create or retrieve Snowflake session from session state."""
    if "snowpark_session" not in st.session_state:
        connection_params = {
            "account": st.secrets["snowflake"]["account"],
            "user": st.secrets["snowflake"]["user"],
            "password": st.secrets["snowflake"]["password"],
            "warehouse": st.secrets["snowflake"]["warehouse"],
            "database": st.secrets["snowflake"]["database"],
            "schema": st.secrets["snowflake"]["schema"],
            "role": st.secrets["snowflake"]["role"]
        }
        st.session_state.snowpark_session = Session.builder.configs(connection_params).create()
    return st.session_state.snowpark_session


# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600)
def fetch_weekly_metrics():
    """
    Fetch and compute all weekly metrics from Snowflake.
    Returns a DataFrame with weekly aggregated data.
    """
    
    session = get_session()
    
    query = """
    WITH weekly_roster AS (
        SELECT 
            DATE_TRUNC('WEEK', ORG_CAPTURE_DATE) AS week_start,
            COUNT(DISTINCT ICD_ID) AS total_agents,
            COUNT(DISTINCT CASE WHEN STATUS = 'A - Active' THEN ICD_ID END) AS active_agents,
            COUNT(DISTINCT CASE WHEN STATUS = 'A - Active' AND AGENT_TYPE != 'Training' THEN ICD_ID END) AS tenured_agents,
            COUNT(DISTINCT CASE WHEN STATUS = 'A - Active' AND AGENT_TYPE = 'Training' THEN ICD_ID END) AS training_agents
        FROM DBGA_TEST_ANALYTICS.DBGA_TEST_ORG_DATA.DIM_ACTIVE_AGENT_ROSTER_FLAT_HISTORICAL
        GROUP BY DATE_TRUNC('WEEK', ORG_CAPTURE_DATE)
    ),
    
    weekly_cpc AS (
        SELECT 
            DATE_TRUNC('WEEK', DATE) AS week_start,
            AVG(CPC) AS avg_cpc
        FROM DBGA_TEST_ANALYTICS.DBGA_TEST_HANK.HISTORICAL_CPC
        GROUP BY DATE_TRUNC('WEEK', DATE)
    ),
    
    daily_active_agents AS (
        SELECT 
            ORG_CAPTURE_DATE,
            ICD_ID
        FROM DBGA_TEST_ANALYTICS.DBGA_TEST_ORG_DATA.DIM_ACTIVE_AGENT_ROSTER_FLAT_HISTORICAL
        WHERE STATUS = 'A - Active'
          AND AGENT_TYPE != 'Training'
    ),
    
    daily_call_activity AS (
        SELECT DISTINCT
            DATE(TIMESTAMP) AS call_date,
            USER_ID
        FROM RAW.DIC.CALL_STATS
        WHERE TIMESTAMP IS NOT NULL
    ),
    
    weekly_attendance AS (
        SELECT 
            DATE_TRUNC('WEEK', daa.ORG_CAPTURE_DATE) AS week_start,
            COUNT(DISTINCT daa.ORG_CAPTURE_DATE || '-' || daa.ICD_ID) AS total_agent_days,
            COUNT(DISTINCT CASE WHEN dca.USER_ID IS NOT NULL 
                  THEN daa.ORG_CAPTURE_DATE || '-' || daa.ICD_ID END) AS present_agent_days
        FROM daily_active_agents daa
        LEFT JOIN daily_call_activity dca 
            ON daa.ICD_ID = dca.USER_ID 
            AND daa.ORG_CAPTURE_DATE = dca.call_date
        GROUP BY DATE_TRUNC('WEEK', daa.ORG_CAPTURE_DATE)
    ),
    
    weekly_leads_sales AS (
        SELECT 
            DATE_TRUNC('WEEK', o.CALL_TIMESTAMP_LOCAL) AS week_start,
            COUNT(DISTINCT CASE WHEN o.BILLABLE_FLAG = 'Y' AND o.ANSWERED_FLAG = 'Y' 
                  AND o.LEAD_ID IS NOT NULL AND o.CALL_DURATION > 15
                  THEN o.LEAD_ID END) AS total_leads,
            COUNT(DISTINCT CASE WHEN o.BILLABLE_FLAG = 'Y' AND o.ANSWERED_FLAG = 'Y' 
                  AND o.SALE_MADE_FLAG = 'Y' AND o.LEAD_ID IS NOT NULL AND o.CALL_DURATION > 15
                  THEN o.LEAD_ID END) AS total_sales,
            AVG(CASE WHEN o.SALE_MADE_FLAG = 'Y' AND o.ANNUAL_PREMIUM > 0 
                AND o.LEAD_ID IS NOT NULL AND o.CALL_DURATION > 15
                THEN o.ANNUAL_PREMIUM END) AS avg_annual_premium,
            COUNT(DISTINCT o.USER_ID) AS agents_with_leads
        FROM DBGA_TEST_ANALYTICS.DBGA_TEST_BERMUDA.OLYMPUS o
        INNER JOIN DBGA_TEST_ANALYTICS.DBGA_TEST_ORG_DATA.DIM_ACTIVE_AGENT_ROSTER_FLAT_HISTORICAL r
            ON o.USER_ID = r.ICD_ID
            AND DATE_TRUNC('WEEK', o.CALL_TIMESTAMP_LOCAL) = DATE_TRUNC('WEEK', r.ORG_CAPTURE_DATE)
            AND r.STATUS = 'A - Active'
        WHERE o.CALL_TIMESTAMP_LOCAL IS NOT NULL
        GROUP BY DATE_TRUNC('WEEK', o.CALL_TIMESTAMP_LOCAL)
    ),
    
    weekly_days AS (
        SELECT 
            DATE_TRUNC('WEEK', ORG_CAPTURE_DATE) AS week_start,
            COUNT(DISTINCT ORG_CAPTURE_DATE) AS business_days
        FROM DBGA_TEST_ANALYTICS.DBGA_TEST_ORG_DATA.DIM_ACTIVE_AGENT_ROSTER_FLAT_HISTORICAL
        GROUP BY DATE_TRUNC('WEEK', ORG_CAPTURE_DATE)
    )
    
    SELECT 
        wr.week_start,
        wr.active_agents AS num_agents,
        wr.tenured_agents,
        wr.training_agents,
        COALESCE(wa.present_agent_days * 1.0 / NULLIF(wa.total_agent_days, 0), 0) AS attendance_rate,
        COALESCE(wls.total_leads * 1.0 / NULLIF(wa.present_agent_days, 0), 0) AS leads_per_agent,
        COALESCE(wls.total_leads, 0) AS total_leads,
        COALESCE(wls.total_sales, 0) AS total_sales,
        COALESCE(wls.total_sales * 1.0 / NULLIF(wls.total_leads, 0), 0) AS close_rate,
        COALESCE(wls.avg_annual_premium, 0) AS avg_annual_premium,
        COALESCE(wd.business_days, 5) AS business_days,
        COALESCE(wcpc.avg_cpc, 70) AS cost_per_call
    FROM weekly_roster wr
    LEFT JOIN weekly_attendance wa ON wr.week_start = wa.week_start
    LEFT JOIN weekly_leads_sales wls ON wr.week_start = wls.week_start
    LEFT JOIN weekly_days wd ON wr.week_start = wd.week_start
    LEFT JOIN weekly_cpc wcpc ON wr.week_start = wcpc.week_start
    WHERE wr.week_start >= DATEADD('month', -12, CURRENT_DATE())
      AND wr.week_start < DATE_TRUNC('WEEK', CURRENT_DATE())  -- Exclude current incomplete week
    ORDER BY wr.week_start ASC
    """
    
    try:
        df = session.sql(query).to_pandas()
        # Normalize column names to uppercase
        df.columns = [c.upper() for c in df.columns]
        return df
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# PROFIT CALCULATION
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_weekly_profit(row, params):
    """Calculate weekly profit based on the provided formula."""
    
    num_agents = row['NUM_AGENTS']
    attendance = row['ATTENDANCE_RATE']
    effective_agents = num_agents * attendance
    
    leads_per_agent = row['LEADS_PER_AGENT']
    close_rate = row['CLOSE_RATE']
    avg_premium = row['AVG_ANNUAL_PREMIUM']
    tenured_agents = row['TENURED_AGENTS']
    training_agents = row['TRAINING_AGENTS']
    total_leads = row['TOTAL_LEADS']
    total_sales = row['TOTAL_SALES']
    business_days = row['BUSINESS_DAYS']
    cost_per_call = row['COST_PER_CALL']
    
    commission_mult = params['commission_multiplier']
    realization = params['realization_rate']
    tenured_salary = params['tenured_daily_salary']
    training_salary = params['training_daily_salary']
    overhead = params['overhead']
    non_sales_salary = params['non_sales_salary']
    
    # REVENUE
    # Use total_leads directly (already filtered to active agents)
    # Revenue = total_leads * close_rate * avg_premium * commission * realization
    weekly_revenue = (
        total_leads * 
        close_rate * 
        avg_premium * 
        commission_mult * 
        realization
    )
    
    # Total annual premium sold this week
    total_premium_sold = total_sales * avg_premium
    
    # COSTS
    # Sales Salary
    tenured_salary_cost = tenured_agents * tenured_salary * business_days
    training_salary_cost = training_agents * training_salary * business_days
    sales_salary_cost = tenured_salary_cost + training_salary_cost
    
    # Sales Commissions (20% for tenured, 5% for training on total premium sold)
    # Approximate split based on agent ratio
    if num_agents > 0:
        tenured_ratio = tenured_agents / num_agents
        training_ratio = training_agents / num_agents
    else:
        tenured_ratio = 0
        training_ratio = 0
    
    tenured_commission = total_premium_sold * tenured_ratio * 0.20
    training_commission = total_premium_sold * training_ratio * 0.05
    sales_commission_cost = tenured_commission + training_commission
    
    # Non-Sales Salary
    non_sales_salary_cost = non_sales_salary * business_days
    
    # Marketing (lead cost)
    marketing_cost = total_leads * cost_per_call
    
    # Overhead
    overhead_cost = overhead * business_days
    
    total_costs = sales_salary_cost + sales_commission_cost + non_sales_salary_cost + marketing_cost + overhead_cost
    
    # PROFIT
    profit = weekly_revenue - total_costs
    daily_profit = profit / business_days if business_days > 0 else 0
    
    return {
        'WEEKLY_REVENUE': weekly_revenue,
        'SALES_SALARY_COST': sales_salary_cost,
        'SALES_COMMISSION_COST': sales_commission_cost,
        'NON_SALES_SALARY_COST': non_sales_salary_cost,
        'MARKETING_COST': marketing_cost,
        'OVERHEAD_COST': overhead_cost,
        'TOTAL_COSTS': total_costs,
        'WEEKLY_PROFIT': profit,
        'DAILY_PROFIT': daily_profit,
        'PROFIT_PER_AGENT': profit / num_agents if num_agents > 0 else 0
    }


def apply_profit_calculations(df, params):
    """Apply profit calculations to all rows in the dataframe."""
    results = df.apply(lambda row: calculate_weekly_profit(row, params), axis=1)
    results_df = pd.DataFrame(results.tolist())
    return pd.concat([df.reset_index(drop=True), results_df], axis=1)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN APPLICATION
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    # Header
    st.markdown('<div class="dashboard-title">📊 Luminary Profit Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="dashboard-subtitle">Weekly Agent Performance & Profitability Analysis</div>', unsafe_allow_html=True)
    
    # Sidebar - Parameters
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        st.subheader("💰 Revenue Parameters")
        
        commission_mult = st.number_input(
            "Commission Multiplier (κ)",
            min_value=1.0, max_value=3.0,
            value=DEFAULT_COMMISSION_MULTIPLIER,
            step=0.01,
            format="%.2f"
        )
        
        realization_rate = st.slider(
            "Realization Rate (π)",
            min_value=0, max_value=100,
            value=int(DEFAULT_REALIZATION_RATE * 100),
            step=1,
            format="%d%%"
        ) / 100.0
        
        st.divider()
        st.subheader("💵 Cost Parameters")
        
        tenured_salary = st.number_input(
            "Tenured Daily Salary ($)",
            min_value=0, max_value=1000,
            value=DEFAULT_TENURED_DAILY_SALARY,
            step=10
        )
        
        training_salary = st.number_input(
            "Training Daily Salary ($)",
            min_value=0, max_value=1000,
            value=DEFAULT_TRAINING_DAILY_SALARY,
            step=10
        )
        
        overhead = st.number_input(
            "Overhead ($/day)",
            min_value=0, max_value=100000,
            value=DEFAULT_OVERHEAD,
            step=1000
        )
        
        non_sales_salary = st.number_input(
            "Non-Sales Salary ($/day)",
            min_value=0, max_value=50000,
            value=DEFAULT_NON_SALES_SALARY,
            step=500
        )
    
    params = {
        'commission_multiplier': commission_mult,
        'realization_rate': realization_rate,
        'tenured_daily_salary': tenured_salary,
        'training_daily_salary': training_salary,
        'overhead': overhead,
        'non_sales_salary': non_sales_salary
    }
    
    # Load data from Snowflake
    with st.spinner("Loading data from Snowflake..."):
        df = fetch_weekly_metrics()
        if df is None or df.empty:
            st.error("No data returned from Snowflake. Please check your data sources.")
            st.stop()
    
    # Apply profit calculations
    df = apply_profit_calculations(df, params)
    
    # Ensure WEEK_START is datetime
    df['WEEK_START'] = pd.to_datetime(df['WEEK_START'])
    
    # Drop earliest week (may be incomplete)
    if len(df) > 1:
        df = df.iloc[1:].reset_index(drop=True)
    
    # Get latest week for KPIs
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else latest
    
    # ═══════════════════════════════════════════════════════════════════════════
    # KPI CARDS
    # ═══════════════════════════════════════════════════════════════════════════
    
    st.markdown('<div class="section-header">📈 Key Performance Indicators (Latest Week)</div>', unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        delta = latest['WEEKLY_PROFIT'] - prev['WEEKLY_PROFIT']
        st.metric(
            label="Weekly Profit",
            value=f"${latest['WEEKLY_PROFIT']:,.0f}",
            delta=f"${delta:,.0f}"
        )
    
    with col2:
        delta = latest['NUM_AGENTS'] - prev['NUM_AGENTS']
        st.metric(
            label="Active Agents",
            value=f"{latest['NUM_AGENTS']:.0f}",
            delta=f"{delta:+.0f}"
        )
    
    with col3:
        delta = (latest['ATTENDANCE_RATE'] - prev['ATTENDANCE_RATE']) * 100
        st.metric(
            label="Attendance Rate",
            value=f"{latest['ATTENDANCE_RATE']:.2%}",
            delta=f"{delta:+.2f}%"
        )
    
    with col4:
        delta = (latest['CLOSE_RATE'] - prev['CLOSE_RATE']) * 100
        st.metric(
            label="Close Rate",
            value=f"{latest['CLOSE_RATE']:.2%}",
            delta=f"{delta:+.2f}%"
        )
    
    with col5:
        delta = latest['AVG_ANNUAL_PREMIUM'] - prev['AVG_ANNUAL_PREMIUM']
        st.metric(
            label="Avg Premium",
            value=f"${latest['AVG_ANNUAL_PREMIUM']:,.0f}",
            delta=f"${delta:+,.0f}"
        )
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PROFIT TRENDING
    # ═══════════════════════════════════════════════════════════════════════════
    
    st.markdown('<div class="section-header">💹 Profit Trending</div>', unsafe_allow_html=True)
    
    # Prepare chart data
    chart_df = df[['WEEK_START', 'WEEKLY_PROFIT']].copy()
    chart_df['WEEKLY_PROFIT'] = chart_df['WEEKLY_PROFIT'].round(0)
    
    st.subheader("Weekly Profit")
    profit_chart = alt.Chart(chart_df).mark_bar(size=20).encode(
        x=alt.X('WEEK_START:T', title='Week'),
        y=alt.Y('WEEKLY_PROFIT:Q', title='Profit ($)', axis=alt.Axis(format=',.0f')),
        color=alt.condition(
            alt.datum.WEEKLY_PROFIT > 0,
            alt.value('#22c55e'),
            alt.value('#ef4444')
        )
    ).properties(height=300)
    st.altair_chart(profit_chart, use_container_width=True)
    
    # Revenue vs Costs
    st.subheader("Revenue vs Total Costs")
    rev_cost_df = df[['WEEK_START', 'WEEKLY_REVENUE', 'TOTAL_COSTS']].copy()
    rev_cost_df['WEEKLY_REVENUE'] = rev_cost_df['WEEKLY_REVENUE'].round(0)
    rev_cost_df['TOTAL_COSTS'] = rev_cost_df['TOTAL_COSTS'].round(0)
    rev_cost_df = rev_cost_df.set_index('WEEK_START')
    rev_cost_df.columns = ['Revenue', 'Costs']
    st.line_chart(rev_cost_df, use_container_width=True, color=['#22c55e', '#ef4444'])
    
    # Cost Breakdown
    st.subheader("Cost Breakdown Over Time")
    cost_df = df[['WEEK_START', 'SALES_SALARY_COST', 'SALES_COMMISSION_COST', 'NON_SALES_SALARY_COST', 'MARKETING_COST', 'OVERHEAD_COST']].copy()
    cost_df['SALES_SALARY_COST'] = cost_df['SALES_SALARY_COST'].round(0)
    cost_df['SALES_COMMISSION_COST'] = cost_df['SALES_COMMISSION_COST'].round(0)
    cost_df['NON_SALES_SALARY_COST'] = cost_df['NON_SALES_SALARY_COST'].round(0)
    cost_df['MARKETING_COST'] = cost_df['MARKETING_COST'].round(0)
    cost_df['OVERHEAD_COST'] = cost_df['OVERHEAD_COST'].round(0)
    cost_melted = cost_df.melt(id_vars=['WEEK_START'], 
                               value_vars=['SALES_SALARY_COST', 'SALES_COMMISSION_COST', 'NON_SALES_SALARY_COST', 'MARKETING_COST', 'OVERHEAD_COST'],
                               var_name='Cost Type', value_name='Amount')
    cost_melted['Cost Type'] = cost_melted['Cost Type'].map({
        'SALES_SALARY_COST': 'Sales Salary',
        'SALES_COMMISSION_COST': 'Sales Commissions',
        'NON_SALES_SALARY_COST': 'Non-Sales Salary',
        'MARKETING_COST': 'Marketing',
        'OVERHEAD_COST': 'Overhead'
    })
    cost_chart = alt.Chart(cost_melted).mark_bar(size=20).encode(
        x=alt.X('WEEK_START:T', title='Week'),
        y=alt.Y('Amount:Q', title='Cost ($)', axis=alt.Axis(format=',.0f')),
        color=alt.Color('Cost Type:N', scale=alt.Scale(
            domain=['Sales Salary', 'Sales Commissions', 'Non-Sales Salary', 'Marketing', 'Overhead'],
            range=['#ef4444', '#f97316', '#eab308', '#22c55e', '#6b7280']
        )),
        order=alt.Order('Cost Type:N')
    ).properties(height=300)
    st.altair_chart(cost_chart, use_container_width=True)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # INDIVIDUAL METRICS
    # ═══════════════════════════════════════════════════════════════════════════
    
    st.markdown('<div class="section-header">📊 Metric Time Series</div>', unsafe_allow_html=True)
    
    # Row 1
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Number of Agents")
        agents_df = df[['WEEK_START', 'NUM_AGENTS']].copy()
        agents_df['NUM_AGENTS'] = agents_df['NUM_AGENTS'].round(0)
        agents_df = agents_df.set_index('WEEK_START')
        st.line_chart(agents_df, use_container_width=True, color='#3b82f6')
    
    with col2:
        st.subheader("Attendance Rate")
        att_df = df[['WEEK_START', 'ATTENDANCE_RATE']].set_index('WEEK_START')
        st.line_chart(att_df, use_container_width=True, color='#8b5cf6')
    
    # Row 2
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Leads per Agent")
        leads_df = df[['WEEK_START', 'LEADS_PER_AGENT']].copy()
        leads_df['LEADS_PER_AGENT'] = leads_df['LEADS_PER_AGENT'].round(2)
        leads_df = leads_df.set_index('WEEK_START')
        st.line_chart(leads_df, use_container_width=True, color='#f59e0b')
    
    with col2:
        st.subheader("Close Rate")
        close_df = df[['WEEK_START', 'CLOSE_RATE']].set_index('WEEK_START')
        st.line_chart(close_df, use_container_width=True, color='#22c55e')
    
    # Row 3
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Avg Annual Premium")
        prem_df = df[['WEEK_START', 'AVG_ANNUAL_PREMIUM']].copy()
        prem_df['AVG_ANNUAL_PREMIUM'] = prem_df['AVG_ANNUAL_PREMIUM'].round(0)
        prem_df = prem_df.set_index('WEEK_START')
        st.line_chart(prem_df, use_container_width=True, color='#ef4444')
    
    with col2:
        st.subheader("Cost Per Call")
        cpc_df = df[['WEEK_START', 'COST_PER_CALL']].copy()
        cpc_df['COST_PER_CALL'] = cpc_df['COST_PER_CALL'].round(0)
        cpc_df = cpc_df.set_index('WEEK_START')
        st.line_chart(cpc_df, use_container_width=True, color='#f59e0b')
    
    # Row 4
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Tenured vs Training Agents")
        tenure_df = df[['WEEK_START', 'TENURED_AGENTS', 'TRAINING_AGENTS']].copy()
        tenure_melted = tenure_df.melt(id_vars=['WEEK_START'],
                                       value_vars=['TENURED_AGENTS', 'TRAINING_AGENTS'],
                                       var_name='Agent Type', value_name='Count')
        tenure_melted['Agent Type'] = tenure_melted['Agent Type'].map({
            'TENURED_AGENTS': 'Tenured',
            'TRAINING_AGENTS': 'Training'
        })
        tenure_chart = alt.Chart(tenure_melted).mark_bar(size=15).encode(
            x=alt.X('WEEK_START:T', title='Week'),
            y=alt.Y('Count:Q', title='Agents', axis=alt.Axis(format=',.0f')),
            color=alt.Color('Agent Type:N', scale=alt.Scale(
                domain=['Tenured', 'Training'],
                range=['#3b82f6', '#93c5fd']
            )),
            order=alt.Order('Agent Type:N')
        ).properties(height=250)
        st.altair_chart(tenure_chart, use_container_width=True)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PROFIT EQUATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    st.markdown('<div class="section-header">📐 Profit Calculation Formula</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="profit-equation">
        <strong>REVENUE CALCULATION</strong><br><br>
        Revenue = N × λ × α × β × κ × π<br><br>
        Where:<br>
        &nbsp;&nbsp;N = Number of agents × Attendance rate<br>
        &nbsp;&nbsp;λ = Leads per agent<br>
        &nbsp;&nbsp;α = Close rate<br>
        &nbsp;&nbsp;β = Average annual premium<br>
        &nbsp;&nbsp;κ = Commission multiplier<br>
        &nbsp;&nbsp;π = Realization rate
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="profit-equation">
        <strong>COST CALCULATION</strong><br><br>
        Costs = Sales Salary + Sales Commissions + Non-Sales Salary + Marketing + Overhead<br><br>
        Where:<br>
        &nbsp;&nbsp;Sales Salary = (Tenured × ${tenured_salary:,}) + (Training × ${training_salary:,})<br>
        &nbsp;&nbsp;Sales Commissions = (Tenured Premium × 20%) + (Training Premium × 5%)<br>
        &nbsp;&nbsp;Non-Sales Salary = ${non_sales_salary:,}/day<br>
        &nbsp;&nbsp;Marketing = Total Leads × CPC (from data)<br>
        &nbsp;&nbsp;Overhead = ${overhead:,}/day<br><br>
        <strong>Profit = Revenue - Costs</strong>
        </div>
        """, unsafe_allow_html=True)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # DATA TABLE
    # ═══════════════════════════════════════════════════════════════════════════
    
    with st.expander("📋 View Raw Data"):
        display_cols = [
            'WEEK_START', 'NUM_AGENTS', 'TENURED_AGENTS', 'TRAINING_AGENTS',
            'ATTENDANCE_RATE', 'LEADS_PER_AGENT', 'CLOSE_RATE', 'AVG_ANNUAL_PREMIUM',
            'WEEKLY_REVENUE', 'SALES_SALARY_COST', 'SALES_COMMISSION_COST', 
            'NON_SALES_SALARY_COST', 'MARKETING_COST', 'OVERHEAD_COST',
            'TOTAL_COSTS', 'WEEKLY_PROFIT'
        ]
        
        display_df = df[display_cols].copy()
        display_df.columns = [
            'Week', 'Agents', 'Tenured', 'Training', 'Attendance', 
            'Leads/Agent', 'Close Rate', 'Avg Premium',
            'Revenue', 'Sales Salary', 'Sales Commission',
            'Non-Sales Salary', 'Marketing', 'Overhead',
            'Total Costs', 'Profit'
        ]
        
        # Format for display
        format_df = display_df.copy()
        format_df['Attendance'] = format_df['Attendance'].apply(lambda x: f"{x:.2%}")
        format_df['Close Rate'] = format_df['Close Rate'].apply(lambda x: f"{x:.2%}")
        format_df['Leads/Agent'] = format_df['Leads/Agent'].apply(lambda x: f"{x:.2f}")
        format_df['Avg Premium'] = format_df['Avg Premium'].apply(lambda x: f"${x:,.0f}")
        format_df['Revenue'] = format_df['Revenue'].apply(lambda x: f"${x:,.0f}")
        format_df['Sales Salary'] = format_df['Sales Salary'].apply(lambda x: f"${x:,.0f}")
        format_df['Sales Commission'] = format_df['Sales Commission'].apply(lambda x: f"${x:,.0f}")
        format_df['Non-Sales Salary'] = format_df['Non-Sales Salary'].apply(lambda x: f"${x:,.0f}")
        format_df['Marketing'] = format_df['Marketing'].apply(lambda x: f"${x:,.0f}")
        format_df['Overhead'] = format_df['Overhead'].apply(lambda x: f"${x:,.0f}")
        format_df['Total Costs'] = format_df['Total Costs'].apply(lambda x: f"${x:,.0f}")
        format_df['Profit'] = format_df['Profit'].apply(lambda x: f"${x:,.0f}")
        
        st.dataframe(format_df, use_container_width=True, hide_index=True)
        
        # Download button
        csv = display_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name="profit_trending_data.csv",
            mime="text/csv"
        )


if __name__ == "__main__":
    main()
