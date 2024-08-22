import sqlalchemy
import streamlit as st
from pandas import DataFrame
from pygwalker.api.streamlit import StreamlitRenderer
import pandas as pd
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(
    level=logging.ERROR,
    filename="app.log",
    filemode="a",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def connect_to_database() -> 'sqlalchemy.engine.Engine':
    """
    Connects to the SQL Server database using SQLAlchemy.

    Returns:
        sqlalchemy.Engine: The database engine.
    """
    print("Connecting to database")
    try:
        engine = create_engine(
            'mssql+pyodbc://localhost/kp_p2p?driver=SQL+Server&trusted_connection=yes')
        print("Connected to database")
        return engine
    except SQLAlchemyError as e:
        print("Error connecting to database:", e)
        st.error(f"Error connecting to database: {e}")
        raise


def import_data_from_db(db_name: str, engine: 'sqlalchemy.engine.Engine', chunksize: int = 10000) -> pd.DataFrame:
    """
    Imports cleaned data from the database using SQLAlchemy.

    Args:
        db_name (str): The name of the database table.
        engine (sqlalchemy.Engine): The database engine.
        chunksize (int): The number of rows to fetch per chunk.

    Returns:
        pandas.DataFrame: The cleaned data.
    """
    print("Importing data from database")
    try:
        query = 'SELECT * FROM [dbo].[KP_Master_Raw]'
        chunks = pd.read_sql_query(query, engine, chunksize=chunksize)
        df = pd.concat(chunk for chunk in chunks)
        if not df.empty:
            print("Data imported successfully")
        else:
            print("No data returned from SQL query")
            raise ValueError("No data returned from SQL query")
        return df
    except SQLAlchemyError as e:
        print("Error executing SQL query:", e)
        st.error(f"Error executing SQL query: {e}")
        raise


@st.cache_data
def load_clean_data(db_name: str) -> DataFrame | None:
    """
    Loads cleaned data from the database.

    Args:
        db_name (str): The name of the database table.

    Returns:
        pandas.DataFrame: The cleaned data.
    """
    print("Loading clean data")
    try:
        engine = connect_to_database()
        clean_data = import_data_from_db(db_name, engine)
        print("Clean data loaded successfully")
        return clean_data
    except SQLAlchemyError as e:
        print("Error connecting to database:", e)
        st.error(f"Error connecting to database: {e}")
        logging.error(f"Error connecting to database: {e}")
    except Exception as e:
        print("Error loading clean data:", e)
        st.error(f"Error loading clean data: {e}")
        return None


def main():
    """
    The main function.
    """
    print("Starting main function")
    st.title("Data Explorer", anchor=None, help=None)

    db_name = "kp_p2p"

    with st.spinner("Loading clean data..."):
        clean_data = load_clean_data(db_name)
    if clean_data is not None:
        with st.spinner("Loading data explorer..."):
            print("Loading data explorer")
            StreamlitRenderer(clean_data).explorer()
    else:
        st.error("Error loading clean data")


if __name__ == "__main__":
    print("Running main")
    main()
