import smtplib
import dns.resolver
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.utils import formataddr
from email import encoders
import threading
import queue

NUM_THREADS = 50  # Fixer le nombre de threads à 100
BATCH_SIZE = 10    # Chaque batch contient 50 emails

# Removed SOCKS5 proxy setup

def read_html_file(file_path):
    with open(file_path, "r", encoding="utf-8") as html_file:
        html_content = html_file.read()
    return html_content

def read_pdf_file(file_path):
    with open(file_path, "rb") as pdf_file:
        return pdf_file.read()

def send_email_task(q, mx_server, sender_email, sender_name, subject, message, to_email, pdf_attachment):
    while True:
        batch = q.get()
        if batch is None:
            break  # Arrêter le thread quand on reçoit "None"

        try:
            server = smtplib.SMTP(mx_server)
            server.ehlo("mac.com")

            msg = MIMEMultipart()
            msg['From'] = formataddr((sender_name, sender_email))
            msg['To'] = to_email
            msg['Subject'] = subject
            msg.attach(MIMEText(message, 'html'))

            if pdf_attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(pdf_attachment)
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment', filename="DE5874557.pdf")
                msg.attach(part)

            server.sendmail(sender_email, batch, msg.as_string())
            print(f"Batch of {len(batch)} emails successfully sent.")

            server.quit()
        except Exception as e:
            print(f"Failed to send email batch: {e}")
        finally:
            q.task_done()

def prepare_and_send_batches(recipient_emails, subject, message, sender_email, sender_name, to_email, pdf_attachment):
    domain = recipient_emails[0].split('@')[1]
    mx_records = dns.resolver.resolve(domain, 'MX')
    mx_record = sorted(mx_records, key=lambda rec: rec.preference)[0]
    mx_server = str(mx_record.exchange).strip('.')

    q = queue.Queue()

    # Créer les threads pour envoyer les e-mails
    threads = []
    for _ in range(NUM_THREADS):
        thread = threading.Thread(target=send_email_task, args=(q, mx_server, sender_email, sender_name, subject, message, to_email, pdf_attachment))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    # Ajouter les emails dans la queue par batch de 50
    for i in range(0, len(recipient_emails), BATCH_SIZE):
        batch = recipient_emails[i:i + BATCH_SIZE]
        q.put(batch)

    # Attendre que tous les emails soient envoyés
    q.join()

    # Envoyer un signal d'arrêt aux threads
    for _ in range(NUM_THREADS):
        q.put(None)
    
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    sender_email = "news@dpd.de"
    sender_name = "Kundenbetreuung"
    subject = "Sie haben gewonnen!"

    message = read_html_file("message.html")
    pdf_attachment = read_pdf_file("code.pdf")

    with open("mails.txt", "r") as file:
        recipient_emails = [line.strip() for line in file.readlines()]

    to_email = ""

    # Envoyer les emails avec 100 threads fixes et batch de 50 emails
    prepare_and_send_batches(recipient_emails, subject, message, sender_email, sender_name, to_email, pdf_attachment)
