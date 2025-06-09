import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from joblib import load
from pathlib import Path


# Always load from project root: /Users/zobiarasheed/Desktop/BI final/.env
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)

def predict_engagement():
    try:
        # Set up paths
        BASE_DIR = Path("/Users/zobiarasheed/Desktop/BI final")
        ENV_PATH = BASE_DIR / ".env"
        MODEL_PATH = BASE_DIR / "ml_modeling" / "social_engagement_model.pkl"
        ENCODERS_PATH = BASE_DIR / "ml_modeling" / "label_encoders.pkl"

        # Load environment variables
        load_dotenv(ENV_PATH)
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")

        # Create database engine
        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

        # Load new data to predict
        query = """
        SELECT * FROM social_media_data
        WHERE record_id NOT IN (SELECT record_id FROM engagement_predictions);
        """
        df = pd.read_sql(query, engine)

        if df.empty:
            print("No new data to predict.")
            return

        # Drop unused columns and prepare data
        df_model = df.drop(columns=["record_id"]).fillna("UNKNOWN")

        # Load model and encoders
        label_encoders = load(ENCODERS_PATH)
        clf = load(MODEL_PATH)

        # Encode categorical columns
        for col in df_model.select_dtypes(include="object").columns:
            if col in label_encoders:
                le = label_encoders[col]
                mask = ~df_model[col].isin(le.classes_)
                df_model.loc[mask, col] = 'UNKNOWN'
                df_model[col] = le.transform(df_model[col])

        # Predict
        preds = clf.predict(df_model)
        df['high_engagement_pred'] = preds

        # Save predictions to database
        df[['record_id', 'high_engagement_pred']].to_sql(
            'engagement_predictions',
            engine,
            if_exists='append',
            index=False
        )

        print(f"✅ Saved {len(df)} predictions to engagement_predictions table.")

    except Exception as e:
        print("❌ Prediction failed:", str(e))
        raise

if __name__ == "__main__":
    predict_engagement()
