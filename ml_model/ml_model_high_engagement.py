import pandas as pd
import os
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from joblib import dump
from dotenv import load_dotenv
from pathlib import Path

# Load environment
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)

def main():
    try:
        # Set up paths
        BASE_DIR = Path("/Users/zobiarasheed/Desktop/BI final")
        DATA_PATH = BASE_DIR / "etl_pipeline_files" / "cleaned_social_media_data.csv"
        MODEL_PATH = BASE_DIR / "ml_modeling" / "social_engagement_model.pkl"
        ENCODERS_PATH = BASE_DIR / "ml_modeling" / "label_encoders.pkl"

        # Load dataset
        df = pd.read_csv(DATA_PATH)
        print("✅ Dataset loaded:", df.shape)

        # Create binary target
        df["total_engagement"] = df[["likes_count", "shares_count", "comments_count"]].sum(axis=1)
        df["high_engagement"] = df["total_engagement"].apply(lambda x: 1 if x >= 500 else 0)  # threshold adjusted to 500


        # Drop unnecessary columns
        df_model = df.drop(columns=["record_id", "total_engagement"])
        df_model = df_model.fillna("UNKNOWN")

        # Encode categorical features
        label_encoders = {}
        for col in df_model.select_dtypes(include="object").columns:
            le = LabelEncoder()
            df_model[col] = le.fit_transform(df_model[col])
            label_encoders[col] = le

        # Save encoders
        ENCODERS_PATH.parent.mkdir(parents=True, exist_ok=True)
        dump(label_encoders, ENCODERS_PATH)

        # Split dataset
        X = df_model.drop("high_engagement", axis=1)
        y = df_model["high_engagement"]

        print("\n✅ Class distribution:\n", y.value_counts())

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        # Train model
        clf = RandomForestClassifier(random_state=42, n_jobs=-1)
        clf.fit(X_train, y_train)

        # Save model
        MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
        dump(clf, MODEL_PATH)

        # Evaluate
        y_pred = clf.predict(X_test)
        print("\n📊 Confusion Matrix:\n", confusion_matrix(y_test, y_pred))
        print("\n📋 Classification Report:\n", classification_report(y_test, y_pred))
        print("✅ Model training completed successfully.")

    except Exception as e:
        print("❌ Model training failed:", str(e))
        raise

if __name__ == "__main__":
    main()
