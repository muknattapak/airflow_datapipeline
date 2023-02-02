import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from settings.configuration import Configuration


class EmailService:

    def send(self, title, content):
        # The mail addresses and
        config = Configuration().getConfig()
        sender_address = config.get('EMAIL', 'MAIL')
        sender_pass = config.get('EMAIL', 'MAIL_PASSWORD')

        receiver_address = config.get('EMAIL', 'MAIL_RECEIVER')
        # mail_content = '''Hello,
        # This is a simple mail. There is only text, no attachments are there The mail is sent using Python SMTP library.
        # Thank You
        # '''

        # Setup the MIME
        message = MIMEMultipart()
        message['From'] = sender_address
        message['To'] = receiver_address
        # The subject line
        message['Subject'] = title
        # The body and the attachments for the mail
        message.attach(MIMEText(content, 'plain'))
        # Create SMTP session for sending the mail
        session = smtplib.SMTP('smtp.gmail.com', 587)  # use gmail with port
        session.starttls()  # enable security
        # login with mail_id and password
        session.login(sender_address, sender_pass)
        text = message.as_string()
        session.sendmail(sender_address, receiver_address.split(','), text)
        session.quit()
        print('Mail Sent', receiver_address)

    def send_error(self, name, error):
        title = 'Analytic Engine Error : ' + name
        contant = str(error)
        self.send(title, contant)
