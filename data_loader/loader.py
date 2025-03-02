
import os
import logging
import json
import pandas as pd # TODO: **maybe** don't use pandas since it's pretty heavyweight? look into more options
from dotenv import load_dotenv
from sqlalchemy import create_engine

# TODO: Move logging set up to separate module
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class DataLoader:
    """DataLoader is a singleton responsible for transforming and loading data. """
    _instance = None
    
    def __new__(cls):
        """
        Creates DataLoader object.
        
        Ensures an instance does not already exist.
        """
        if cls._instance is None:
            cls._instance = super(DataLoader, cls).__new__(cls)
            cls._instance.init_db()
        return cls._instance
    
    def __init__(self):
        """Initialize only if it hasn't been initialized (i.e. no engine) before."""
        if not hasattr(self, 'engine'):
            self.init_db()

    def get_db_url(self):
        """
        Constructs database connection URL based on environment variables.
        
        Performs strict validation on the env settings; each value must be explicitly set.
        """
        required_vars = ["DB_TYPE", "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
        missing_vars = [var for var in required_vars if os.getenv(var) is None]

        if missing_vars:
            raise ValueError(f"❌ Missing required environment variables: {', '.join(missing_vars)}")

        db_type = os.getenv("DB_TYPE")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")

        if db_type == "mysql":
            return f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        elif db_type == "postgresql":
            return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        else:
            raise ValueError(f"❌ Unsupported database type: {db_type}")
    
    def load_mappings(self, mapping_file: str):
        """Loads user-defined mappings from a JSON file."""
        try:
            with open(mapping_file, "r") as f:
                mappings = json.load(f)
            return mappings
        except Exception as e:
            logger.error(f"❌ Failed to load mappings: {e}")
            raise e
    
    def process_csv(self, dataset_config: dict):
        """
        Reads a CSV file, applies mappings, and writes it to the target database table.
        
        Args:
            dataset_config (dict): A dictionary with keys:
                - "csv_path": Path to the CSV file.
                - "table_name": Name of the target database table.
                - "column_mappings": Mapping of CSV columns to DB columns.
        """
        csv_path = dataset_config["csv_path"]
        table_name = dataset_config["table_name"]
        column_mappings = dataset_config["column_mappings"]
        
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"✅ Loaded {len(df)} rows from {csv_path}")
        except Exception as e:
            logger.error(f"❌ Failed to read CSV {csv_path}: {e}")
            raise e

        df.rename(columns=column_mappings, inplace=True)
        df = df[list(column_mappings.values())]  # Keep only mapped columns

        try:
            with self.engine.connect() as conn:
                df.to_sql(table_name, conn, if_exists="append", index=False)
            logger.info(f"✅ Successfully inserted {len(df)} rows into {table_name}")
        except Exception as e:
            logger.error(f"❌ Failed to insert data into {table_name}: {e}")
            raise e

    def init_db(self):
        """Initialize connection to target database."""
        db_url = self.get_db_url()
        self.engine = create_engine(db_url) # TODO: Enable pooling
        logger.info('Target DB initialized!')

_data_loader = None

def get_data_loader():
    """Lazy initialization of DataLoader."""
    global _data_loader
    if _data_loader is None:
        _data_loader = DataLoader()
    return _data_loader