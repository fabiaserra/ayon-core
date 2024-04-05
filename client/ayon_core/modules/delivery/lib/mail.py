import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


DEFAULT_SMTP_SERVER = os.getenv("AX_SMTP_SERVER")
DEFAULT_PORT = 25


def send_email(
    sender_email,
    receiver_email,
    subject,
    body,
    attachment_paths=[],
    smtp_server=DEFAULT_SMTP_SERVER,
    port=DEFAULT_PORT,
):
    # Create a MIMEText object to represent the email
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject

    # Add email body
    msg.attach(MIMEText(body, "plain"))

    # Attach files
    for path in attachment_paths:
        with open(path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {path}",
            )
            msg.attach(part)

    try:
        # Connect to the server and send the email
        server = smtplib.SMTP(smtp_server, port)
        # server.starttls()  # Upgrade the connection to encrypted SSL/TLS
        # server.login(sender_email, sender_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")
