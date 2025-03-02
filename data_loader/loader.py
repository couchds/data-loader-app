
import os
import logging
import json
import re

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
    
    def parse_map_expression(self, expression):
        """
        Parses a MAP() transformation string and returns a dictionary of value mappings and a default.

        Example:
            "MAP(('Yes'-> True), ('No' -> False), False)"
        
        Returns:
            ({'Yes': True, 'No': False}, False)
            
        TODO: Refactor
        """
        pattern = r"MAP\((.*?)\)$"  # Match full MAP() content
        match = re.match(pattern, expression.strip())

        if not match:
            raise ValueError(f"❌ Invalid MAP() syntax: {expression}")

        content = match.group(1).strip()

        # Extract key-value pairs (e.g., ('Yes' -> True))
        pairs = re.findall(r"\('(.+?)'\s*->\s*(.+?)\)", content)
        mappings = {}

        for key, value in pairs:
            try:
                value = eval(value)  # Convert 'True', 'False', or numbers
            except:
                pass
            mappings[key] = value

        # Extract default value (last item after key-value pairs)
        remaining = re.sub(r"\('(.+?)'\s*->\s*(.+?)\),?\s*", "", content)
        default_value = remaining.strip().rstrip(",")

        try:
            default_value = eval(default_value)  # Convert default to correct type
        except:
            pass

        return mappings, default_value

    def process_mappings(self, mappings_file, data_dir):
        """
        Reads dependencies, performs JOINs, applies transformations, and inserts data into the database.

        Args:
            mappings_file (str): Path to the mappings JSON file.
            data_dir (str): Directory where dependency files are stored.
        
        TODO: Refactor, support large dataframes (maybe use other library for this...)
        """
        # Load mapping configuration
        try:
            with open(mappings_file, "r") as f:
                mapping_config = json.load(f)
        except Exception as e:
            logger.error(f"❌ Failed to load mappings from {mappings_file}: {e}")
            return

        table_name = mapping_config["table_name"]
        column_mappings = mapping_config["column_mappings"]
        transformations = mapping_config.get("transformations", [])

        # Load dependencies into DataFrames
        dataframes = {}
        for dep in mapping_config["dependencies"]:
            filename = dep["filename"]
            file_path = os.path.join(data_dir, filename)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path, sep="\t")
                dataframes[filename] = df
                logger.info(f"✅ Loaded {len(df)} rows from {file_path}")
            else:
                logger.error(f"❌ Missing dependency file: {file_path}")
                return

        # Perform JOINs if specified
        if "joins" in mapping_config:
            for join in mapping_config["joins"]:
                left_df = dataframes.get(join["left"])
                right_df = dataframes.get(join["right"])

                if left_df is None or right_df is None:
                    logger.error(f"❌ Could not find required DataFrames for JOIN: {join}")
                    return

                join_type = join["type"].lower()  # INNER, LEFT, RIGHT, OUTER
                left_on = join["left_on"]
                right_on = join["right_on"]

                if "include" in join:
                    right_df = right_df[[right_on] + join["include"].split(", ")]

                logger.info(f"🔄 Performing {join_type.upper()} JOIN on {left_on} = {right_on}")

                merged_df = pd.merge(left_df, right_df, left_on=left_on, right_on=right_on, how=join_type)
                dataframes[join["left"]] = merged_df

        # Apply transformations
        final_df = list(dataframes.values())[0]  # Use the first DataFrame after JOINs
        for transformation in transformations:
            for column, expr in transformation.items():
                if expr.startswith("MAP("):
                    mappings, default_value = self.parse_map_expression(expr)
                    if column in final_df.columns:
                        final_df[column] = final_df[column].map(mappings).fillna(default_value)
                        logger.info(f"✅ Applied MAP() transformation on column: {column}")

        final_df = final_df.rename(columns=column_mappings)
        final_df = final_df[list(column_mappings.values())]  # Keep only mapped columns

        try:
            with self.engine.connect() as conn:
                final_df.to_sql(table_name, conn, if_exists="append", index=False)
            logger.info(f"✅ Successfully inserted {len(final_df)} rows into {table_name}")
        except Exception as e:
            logger.error(f"❌ Failed to insert data into {table_name}: {e}")
   
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