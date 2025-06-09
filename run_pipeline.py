import os
import sys
import logging
import traceback
import subprocess
from pathlib import Path
from datetime import datetime
from send_email import send_email
from dotenv import load_dotenv

# Set base directory
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

# Load environment variables
if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)
    logging.info(f".env file loaded from {ENV_PATH}")
else:
    logging.error(f".env file not found at: {ENV_PATH}")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(BASE_DIR / "pipeline.log")
    ]
)
logger = logging.getLogger(__name__)

# Scripts to run
SCRIPTS = [
    "etl_pipeline_files/Social_media_Data_cleaning.py",
    "etl_pipeline_files/Social_media_Data_storage.py"
]

def run_script(script_path):
    try:
        full_path = BASE_DIR / script_path
        logger.info(f"🚀 Running {full_path}")

        start_time = datetime.now()
        result = subprocess.run(
            ["/usr/local/bin/python3", str(full_path)],
            capture_output=True,
            text=True,
            check=True
        )

        if result.stdout:
            logger.info(f"Output from {script_path}:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"Warnings from {script_path}:\n{result.stderr}")

        duration = datetime.now() - start_time
        logger.info(f"✅ {script_path} completed in {duration.total_seconds():.2f} seconds")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Script failed with exit code {e.returncode}")
        logger.error(f"Command: {e.cmd}")
        logger.error(f"Error output:\n{e.stderr}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error running {script_path}: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def main():
    pipeline_start = datetime.now()
    logger.info("⏳ Starting ETL Pipeline")

    success = True
    failed_scripts = []

    for script in SCRIPTS:
        if not run_script(script):
            success = False
            failed_scripts.append(script)
            break

    duration = datetime.now() - pipeline_start
    status = "SUCCESS" if success else "FAILED"
    subject = f"ETL Pipeline {status}"

    body = "\n".join([
        "ETL Pipeline Report",
        "===================",
        f"Status: {status}",
        f"Start Time: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Duration: {duration}",
        f"Failure Point: {failed_scripts[0] if failed_scripts else 'N/A'}" if not success else "All stages completed successfully.",
        f"Log Location: {BASE_DIR}/pipeline.log"
    ])

    try:
        if send_email(subject, body):
            logger.info("📧 Status email sent successfully")
        else:
            logger.warning("⚠️ Email sending failed")
    except Exception as e:
        logger.error(f"❌ Exception during email sending: {str(e)}")

    if success:
        logger.info(f"✅ Pipeline completed successfully in {duration.total_seconds():.2f} seconds")
        sys.exit(0)
    else:
        logger.error(f"❌ Pipeline failed after {duration.total_seconds():.2f} seconds")
        sys.exit(1)

if __name__ == "__main__":
    main()
