import smtplib
from email.mime.text import MIMEText

def send_email(subject, body, to_email, from_email, from_password):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)  # Use Gmail SMTP over SSL
        server.login(from_email, from_password)
        server.sendmail(from_email, [to_email], msg.as_string())
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

if __name__ == "__main__":
    import sys
    # Example usage:
    subject = "Trading Dashboard Alert: Data Download Failed"
    body = "Please check your dashboard automation. There was a failure in data download."
    to_email = "brightfox270@gmail.com"  # Replace with your email
    from_email = "brightfox270@gmail.com"  # Replace with your Gmail
    from_password = "forex4win"  # Use Gmail App Password for security

    send_email(subject, body, to_email, from_email, from_password)
