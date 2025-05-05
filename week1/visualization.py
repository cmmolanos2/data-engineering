import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from emoji import emojize

# Custom mapping: CSV nationality codes → ISO 2-letter codes → flags
NATIONALITY_TO_FLAG = {
    "AUS": "🇦🇺",  # Australia
    "GBR": "🇬🇧",  # Great Britain
    "NED": "🇳🇱",  # Netherlands
    "MON": "🇲🇨",  # Monaco
    "THA": "🇹🇭",  # Thailand
    "ITA": "🇮🇹",  # Italy
    "CAN": "🇨🇦",  # Canada
    "FRA": "🇫🇷",  # France
    "GER": "🇩🇪",  # Germany
    "JPN": "🇯🇵",  # Japan
    "ESP": "🇪🇸",  # Spain
    "NZL": "🇳🇿",  # New Zealand
    "BRA": "🇧🇷",  # Brazil
}



# Read CSV files
engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5433/postgres")
driver_data = pd.read_sql("select * from drivers_clasification", engine)
team_data = pd.read_sql("select * from teams_clasification", engine)

driver_data["nationality"] = driver_data["nationality"].map(NATIONALITY_TO_FLAG).fillna("❓")  # "❓" for unknown
driver_data["driver"] = driver_data["nationality"] + '  ' + driver_data["driver"]
del driver_data["nationality"]

# Function to apply color styling and clean columns
def style_and_clean(df):
    if 'team' in df.columns:
        # Create HTML-styled team names
        df['Team'] = df.apply(lambda x: f"<span style='color:#{x['team_colour']}'>{x['team']}</span>", axis=1)
        # Remove original columns
        return df.drop(['team', 'team_colour'], axis=1)
    return df

# Process both datasets
styled_drivers = style_and_clean(driver_data)
styled_teams = style_and_clean(team_data)
styled_teams['Points'] = styled_teams['points']
del styled_teams['points']


# Configure page
st.title("🏎️ F1 Championship Standings")
st.markdown("""
<style>
    div[data-testid="stMarkdown"] span { display: block; text-align: center; }
    table { width:100% !important; }
    th, td { text-align: center !important; }
</style>
""", unsafe_allow_html=True)

# Display styled tables
st.header("Driver Standings")
st.markdown(
    styled_drivers.to_html(escape=False, index=False),
    unsafe_allow_html=True
)

st.header("Team Standings")
st.markdown(
    styled_teams.to_html(escape=False, index=False, justify='center'),
    unsafe_allow_html=True
)