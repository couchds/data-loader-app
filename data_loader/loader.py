
import os
import logging
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
        """When creating the object, ensure an instance does not already exist."""
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

    def init_db(self):
        """Initialize connection to target database."""
        db_url = self.get_db_url()
        self.engine = create_engine(db_url) # TODO: Enable pooling
        logger.info('Target DB initialized!')

data_loader = DataLoader()