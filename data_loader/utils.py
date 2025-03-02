import os
from dotenv import load_dotenv

load_dotenv()

def get_db_url():
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