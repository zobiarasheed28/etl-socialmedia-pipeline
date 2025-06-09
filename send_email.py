import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from pathlib import Path


# Always load from project root: /Users/zobiarasheed/Desktop/BI final/.env
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def send_email(subject, body):
    """Send email notification using Gmail SMTP with app password"""
    try:
        # Load environment variables from project root
        base_dir = Path(__file__).resolve().parent.parent  # one level up
        env_path = Path("/Users/zobiarasheed/Desktop/BI final/.env")

        if not env_path.exists():
            logger.error(".env file not found at: %s", env_path)
            return False

        load_dotenv(env_path)

        # Required email credentials
        sender = os.getenv("EMAIL_SENDER")
        receiver = os.getenv("EMAIL_RECEIVER")
        password = os.getenv("EMAIL_PASSWORD")

        if not all([sender, receiver, password]):
            logger.error("Missing email credentials in .env file")
            return False

        # Create message
        msg = MIMEMultipart()
        msg["Subject"] = f"[ETL Pipeline] {subject}"
        msg["From"] = sender
        msg["To"] = receiver
        msg.attach(MIMEText(body, "plain"))

        # Send via Gmail SMTP
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, receiver, msg.as_string())

        logger.info("Notification email sent successfully")
        return True

    except smtplib.SMTPAuthenticationError:
        logger.error("Email authentication failed. Check Gmail app password.")
    except smtplib.SMTPException as e:
        logger.error("SMTP error occurred: %s", str(e))
    except Exception as e:
        logger.error("Unexpected error sending email: %s", str(e))

    return False

# Optional: test run
if __name__ == "__main__":
    send_email("Test Notification", "This is a test email from the ETL pipeline.")
