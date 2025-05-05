import logging
import os
import pandas as pd
import requests
import time
from sqlalchemy import create_engine


# ─── Logging Setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─── ETL Steps ──────────────────────────────────────────────────────────────────
def get_dataframes_from_api(
        url: str,
        params: dict = None,
        session_keys: list[int] = None
    ) -> pd.DataFrame:
    attempts = 1
    logger.info(f"Extraction for: {url}")

    while True:
        logger.info(f"Extracction attempt: {attempts}.")
        try:
            if session_keys:
                response_df = pd.DataFrame()  # Empty
                for sk in session_keys:
                    response = requests.get(
                        url,
                        params={"session_key": sk}
                    )

                    if response.status_code == 200:
                        response_dict = response.json()
                        tmp = pd.DataFrame.from_dict(data=response_dict)
                        response_df = pd.concat(
                            [response_df, tmp],
                            ignore_index=True
                        )
                    elif response.status_code == 429:
                        logger.error("Rate limit exceeded. Try in 10 minutes.")
                        break

                return response_df
            else:
                response = requests.get(url=url, params=params)

                if response.status_code == 200:
                    response_dict = response.json()
                    response_df = pd.DataFrame.from_dict(data=response_dict)
                    return response_df
                elif response.status_code == 429:
                    logger.error("Rate limit exceeded. Try in 10 minutes.")
                    break

            logger.info(f"Data extracted successfully from: {url}")
            break

        except Exception as e:
            logger.error(f"Failed to get data from api. Retry in 2 seconds.")
            attempts += 1
            time.sleep(2)


def data_extraction(db_url: str) -> tuple[pd.DataFrame]:
    try:
        # session to get races information
        params = {
            'year': 2025,
            'session_type': 'Race'
        }
        sessions_df = get_dataframes_from_api(
            url='https://api.openf1.org/v1/sessions',
            params=params
        )

        # check for new sessions
        session_keys = set(sessions_df.session_key)
        engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5433/postgres")
        postgres_df = pd.read_sql("SELECT session_key FROM races", engine)
        postgres_ids = set(postgres_df['session_key'])
        session_key_to_extract = list(session_keys - postgres_ids)

        if len(session_key_to_extract) == 0:
            raise("No new sessions.")

        # drivers information
        drivers_df = get_dataframes_from_api(
            url='https://api.openf1.org/v1/drivers',
            session_keys=session_key_to_extract
        )
        # position of driver for each session(race)
        positions_df = get_dataframes_from_api(
            url='https://api.openf1.org/v1/position',
            session_keys=session_key_to_extract
        )

        return (
            sessions_df[sessions_df['session_key'].isin(session_key_to_extract)],
            drivers_df,
            positions_df
        )

    except Exception:
        logger.error("Data extraction failed. Try in 10 minutes.")


def data_transformation(
        sessions: pd.DataFrame,
        drivers: pd.DataFrame,
        positions: pd.DataFrame
    ) -> tuple[pd.DataFrame]:

    sessions['date'] = pd.to_datetime(sessions['date_start']).dt.date
    sessions = sessions[['session_key', 'session_name', 'date', 'country_code']]

    rookies_countries = {
        'Antonelli': 'ITA',
        'Bearman': 'GBR',
        'Bortoleto': 'BRA',
        'Doohan': 'AUS',
        'Hadjar': 'FRA',
        'Lawson': 'NZL',
    }
    mask = drivers['country_code'].isna()
    drivers.loc[mask, 'country_code'] = drivers.loc[mask, 'last_name'].map(rookies_countries)
    drivers['id'] = drivers["session_key"].astype(str) + '-' + drivers['driver_number'].astype(str)
    drivers = drivers[
        ['id', 'session_key', 'full_name', 'country_code', 'driver_number',
         'team_colour', 'team_name']
    ]

    sessions_standings = (
        positions[['session_key', 'driver_number', 'position']]
        .groupby(['session_key', 'driver_number'], as_index=False)
        .last()
    )
    results = pd.merge(
        drivers,
        sessions_standings,
        on=['session_key', 'driver_number'],
        how='left'
    )

    return sessions, results

def load_to_db(races: pd.DataFrame, results: pd.DataFrame, db_url: str):
    try:
        engine = create_engine(db_url)

        races.to_sql(
            name='races',
            con=engine,
            if_exists='append', # Choose 'append', 'replace', or 'fail'
            index=False,        # Don't write pandas index as a column
        )

        results.to_sql(
            name='results',
            con=engine,
            if_exists='append', # Choose 'append', 'replace', or 'fail'
            index=False,        # Don't write pandas index as a column
        )

        logger.info("Records added to database")

    except Exception as e:
        logger.error(f"Loading to database failed. Error:\n {e}")

def main():
    USER = os.getenv('POSTGRES_USER')
    PASSWORD = os.getenv('POSTGRES_PASSWORD')
    HOST = os.getenv('POSTGRES_HOST')
    PORT = os.getenv('POSTGRES_PORT')
    DB = os.getenv('POSTGRES_DB')
    db_url = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"

    sessions, drivers, positions = data_extraction(db_url)

    races, results = data_transformation(
        sessions=sessions,
        drivers=drivers,
        positions=positions
    )

    load_to_db(races, results, db_url)


if __name__ == "__main__":
    main()