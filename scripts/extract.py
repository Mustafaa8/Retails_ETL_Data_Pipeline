from sqlalchemy import create_engine
import pandas as pd

FILEPATH = "/home/mostafa/data/airflow-docker/data/sales_data.csv"
DBURL = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"

def db_connection(db_url):
    """
    Creates the connection between the Pipeline and the Database

    Args:
    - db_url : link that connect to the database
    """
    try:
        engine = create_engine(db_url)
        with engine.connect():
            print("Successfully connected to the database")
        return engine
    except Exception as e:
        print("Cannot connect to the database",e)
        raise

def read_data(filepath):
    """
    Reading data from CSV file into dataframe to be transformed or ingested

    Args:
    - filepath : the path of the wanted to be red file
    """
    try:
        print("Reading Data from file...")
        dataframe = pd.read_csv(filepath)
        col_names = {"Store ID":"store_id",
                 "Product ID":"product_id",
                 "Inventory Level":"inventory_level",
                 "Weather Condition":"weather_condition",
                 "Competitor Pricing":"competitor_pricing",
                 "Units Sold":"units_sold",
                "Units Ordered":"units_ordered"}
        dataframe = dataframe.rename(columns=col_names)
        dataframe.columns = dataframe.columns.str.lower()
        return dataframe
    except Exception as e:
        print("Couldn't read or edit data",e)
        raise

def load_data_into_db(dataframe,engine):
    """
    Ingesting the data optained from file(dataframe) into a postgres database using sqlalchemy engine
    Args:
    - dataframe : the table of the data
    - engine : sqlalchmey engine the connects to it
    """
    try:
        
        print("Ingesting Data into the database...")
        dataframe.to_sql("raw_data",con=engine,if_exists="replace",index=False,chunksize=1000)
        print("Data successfully ingested into raw_data table.")
    except Exception as e:
        print("Couldn't ingest the data",e)
        raise

if __name__ == "__main__":
    pg_engine = db_connection(DBURL)
    df = read_data(FILEPATH)
    load_data_into_db(df,pg_engine)
    