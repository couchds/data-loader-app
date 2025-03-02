
from sqlalchemy import create_engine
from utils import get_db_url

class DataLoader:
    """DataLoader is a singleton responsible for transforming and loading data. """
    _instance = None
    
    def __new__(cls):
        """When creating the object, ensure an instance does not already exist."""
        if cls._instance is None:
            cls._instance = super(DataLoader, cls).__new__(cls)
            cls._instance.init_db()
        return cls._instance

    def init_db(self):
        """Initialize connection to target database."""
        db_url = get_db_url()  # Get the database connection string
        self.engine = create_engine(db_url)
        print('Target DB initialized!')

data_loader = DataLoader()