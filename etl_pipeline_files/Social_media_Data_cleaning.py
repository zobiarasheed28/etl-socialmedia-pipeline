import pandas as pd
import numpy as np
from pathlib import Path
from dotenv import load_dotenv


# Always load from project root: /Users/zobiarasheed/Desktop/BI final/.env
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)


def main():
    try:
        # Define file paths
        BASE_DIR = Path("/Users/zobiarasheed/Desktop/BI final/etl_pipeline_files")
        RAW_PATH = BASE_DIR / "Social Media Engagement Dataset.csv"
        CLEANED_PATH = BASE_DIR / "cleaned_social_media_data.csv"

        # Load dataset
        df = pd.read_csv(RAW_PATH)
        print("Original data shape:", df.shape)

        # Convert date column
        date_cols = [col for col in df.columns if 'date' in col.lower()]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            print(f"Invalid dates in '{col}':", df[col].isna().sum())

        # Drop duplicates
        df = df.drop_duplicates()
        print("Shape after removing duplicates:", df.shape)

        # Standardize text/object columns
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].str.strip().str.upper()

        # Replace ambiguous values
        df.replace(['UNKNOWN', 'NOT REPORTED', 'OTHER', 'N/A', 'NA', 'NONE', 'NOT APPLICABLE'], np.nan, inplace=True)

        # Convert engagement metrics to numeric
        engagement_cols = ['likes', 'shares', 'comments', 'impressions', 'reach', 'followers']
        for col in engagement_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Reset index and create record_id
        df = df.reset_index(drop=True)
        df['record_id'] = ['rid' + str(i).zfill(6) for i in range(1, len(df) + 1)]
        df = df[['record_id'] + [col for col in df.columns if col != 'record_id']]

        # Save cleaned data
        df.to_csv(CLEANED_PATH, index=False)
        print("Cleaned dataset saved successfully.")
        print(df.dtypes)

    except Exception as e:
        print("ETL pipeline failed:", str(e))
        raise

if __name__ == "__main__":
    main()
