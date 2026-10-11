"""Optional SMTP email notifications for partial city/date failures."""
import logging
import os
import smtplib
from email.message import EmailMessage

logger = logging.getLogger(__name__)

def send_failure_summary(subject: str, body: str) -> bool:
    """Send a pipeline summary email when SMTP settings are configured.

    Missing SMTP configuration or delivery errors are logged and return ``False``
    so notification problems do not replace the pipeline's original result.

    Args:
        subject: Email subject line.
        body: Plain-text message body.

    Returns:
        ``True`` if the email was sent, otherwise ``False``.
    """
    host = os.getenv("WEATHER_SMTP_HOST", "").strip()
    sender = os.getenv("WEATHER_SMTP_FROM", "").strip()
    recipient = os.getenv("WEATHER_ALERT_EMAIL", "").strip()
    if not (host and sender and recipient):
        logger.warning(
            "Email not sent: configure WEATHER_SMTP_HOST, WEATHER_SMTP_FROM, "
            "and WEATHER_ALERT_EMAIL (and credentials if required). Summary: %s", body
        )
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = recipient
    message.set_content(body)
    port = int(os.getenv("WEATHER_SMTP_PORT", "587"))
    username = os.getenv("WEATHER_SMTP_USERNAME", "").strip()
    password = os.getenv("WEATHER_SMTP_PASSWORD", "")
    use_tls = os.getenv("WEATHER_SMTP_STARTTLS", "true").lower() in {"1", "true", "yes"}
    try:
        with smtplib.SMTP(host, port, timeout=20) as smtp:
            if use_tls:
                smtp.starttls()
            if username:
                smtp.login(username, password)
            smtp.send_message(message)
        logger.info("Weather pipeline failure summary email sent to %s", recipient)
        return True
    except Exception:
        logger.exception("Unable to send weather pipeline summary email")
        return False
