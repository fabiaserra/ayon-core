import os
import smtplib
import datetime

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

from ayon_core.lib import Logger


DEFAULT_SMTP_SERVER = os.getenv("AX_SMTP_SERVER")
DEFAULT_PORT = 25

# Default email sender to use for notifications
DEFAULT_SENDER_EMAIL = "pam@alkemy-x.com"

# Get current time and date
TODAY = datetime.date.today()
THIS_HOUR = datetime.datetime.now().hour

logger = Logger.get_logger(__name__)


def send_email(
    recipients,
    subject,
    body,
    sender_email=DEFAULT_SENDER_EMAIL,
    attachment=None,
    cc=None,
    bcc=None,
    reply_to=None,
    smtp_server=DEFAULT_SMTP_SERVER,
    port=DEFAULT_PORT,
    is_html=False,
):
    """Utility function to execute the sending of email.
    Requires at least one recipient, a subject and message. Can also be used
    to attach files, add CC, BCC and reply_to tags.
    """

    # Convert recipients to proper list
    if isinstance(recipients, str):
        recipients = recipients.split(",")

    if not isinstance(recipients, list):
        logger.error("Invalid recipients! Must be a string or list of strings")
        return

    approved_emails = []
    # for person in recipients:
    #     if re.search(".*@.*\..*", str(person)):
    #         approved_emails.append(person)
    #     else:
    #         logger.warning(f"\n<!> {person} is not a valid email\n")

    # if not approved_emails:
    #     logger.error("No address was provided, canceling email.")
    #     return
    
    approved_emails.append("farrizabalaga@alkemy-x.com")

    # Create a MIMEText object to represent the email
    if is_html:
        msg = MIMEMultipart("alternative")
    else:
        msg = MIMEMultipart()

    msg["From"] = sender_email
    msg["To"] = ",".join(approved_emails)
    msg["Subject"] = subject

    cc_recipients = []
    if cc:
        if isinstance(cc, str):
            cc_recipients = cc.split(",")
        elif isinstance(cc, list):
            cc_recipients = cc
        approved_emails += cc_recipients
        msg["cc"] = ",".join(cc_recipients)

    bcc_recipients = []
    if bcc:
        if isinstance(bcc, str):
            bcc_recipients = bcc.split(",")
        elif isinstance(bcc, list):
            bcc_recipients = bcc
        approved_emails += bcc_recipients
        msg["bcc"] = ",".join(bcc_recipients)

    if reply_to:
        msg.add_header("reply-to", reply_to)
    
    # Combine all recipients and remove duplicates
    approved_emails = sorted(list(set(approved_emails)))

    # Add email body
    if is_html:
        msg.attach(MIMEText(body, "html"))
    else:
        msg.attach(MIMEText(body, "plain"))
    
    # Add attachment
    if attachment:
        if isinstance(attachment, str):
            attachment = [attachment]
        elif not isinstance(attachment, list):
            logger.error("Attachments must be either a string or list of strings.")
            logger.error(attachment)
            return

        for path in attachment:
            if not os.path.exists(path):
                logger.error("Attached file '%s' does not exist", path)
                continue

            with open(path, "rb") as attachment:
                attach_file = MIMEBase("application", "octet-stream")
                attach_file.set_payload(attachment.read())

            encoders.encode_base64(attach_file)

            attach_file.add_header(
                "Content-Disposition",
                f"attachment; filename={os.path.basename(path)}",
            )
            msg.attach(attach_file)

    logger.info("Sending e-mail to  %s", approved_emails)
    try:
        # Connect to the server and send the email
        server = smtplib.SMTP(smtp_server, port)
        server.sendmail(sender_email, approved_emails, msg.as_string())
        server.quit()
        logger.debug("Email sent successfully!")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")


def signoff():
    if THIS_HOUR < 9:
        return "Have a great morning!"
    elif THIS_HOUR < 17:
        return "Have a great day!"
    elif THIS_HOUR < 17:
        return "Have a great rest of the afternoon!"
    else:
        return "Have a great evening!"


def greet():
    if THIS_HOUR < 12:
        return "Good Morning"
    elif THIS_HOUR < 16:
        return "Good Afternoon"
    else:
        return "Good Evening"
