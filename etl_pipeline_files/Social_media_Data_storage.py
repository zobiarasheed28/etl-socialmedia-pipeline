import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from pathlib import Path

def main():
    try:
        # Correct base path to project directory
        BASE_DIR = Path(__file__).resolve().parent.parent
        ENV_PATH = BASE_DIR / ".env"
        CLEANED_PATH = BASE_DIR / "etl_pipeline_files" / "cleaned_social_media_data.csv"

        # Load environment variables
        if not ENV_PATH.exists():
            raise FileNotFoundError(f".env file not found at: {ENV_PATH}")
        load_dotenv(dotenv_path=ENV_PATH)

        DB_USER = os.getenv('DB_USER')
        DB_PASSWORD = os.getenv('DB_PASSWORD')
        DB_HOST = os.getenv('DB_HOST')
        DB_PORT = int(os.getenv('DB_PORT', 5432))  # Ensure it's an int
        DB_NAME = os.getenv('DB_NAME')

        # Debug logging
        print(f"Connecting to DB: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

        # Create SQLAlchemy engine using URL
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        engine = create_engine(url)

        # Load cleaned dataset
        df = pd.read_csv(CLEANED_PATH)
        print("Dataset Loaded. Data types:\n", df.dtypes)

        # Push to PostgreSQL
        df.to_sql('social_media_data', con=engine, if_exists='replace', index=False)
        print("✅ Data pushed successfully to PostgreSQL!")

    except Exception as e:
        print(f"\n❌ Data storage failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
