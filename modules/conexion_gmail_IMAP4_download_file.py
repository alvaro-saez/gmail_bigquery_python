import imaplib
import email
from email.header import decode_header

import os
from datetime import datetime, timedelta
import re
import json
from google.cloud import secretmanager
from google.oauth2.service_account import Credentials



# variables de configuración
config_env = open('../config/general_config_environments.json')
config_env = json.load(config_env)
env = config_env['general']['config_env']

config_file = open('../config/general_config_' + env + '.json')
config_file = json.load(config_file)
## gmail
#email_user = config_file['gmail']['email_user']
#email_pass = config_file['gmail']['email_pass']
imap4_sll_type = config_file['gmail']['imap4_sll_type']
download_path = config_file['gmail']['download_path']
project_id = config_file['bigquery']['project_id']
secret_id_pass = config_file['secretmanager']['gmail_pass']
secret_id_name = config_file['secretmanager']['gmail_account']
credentials_path = config_file['bigquery']['bq_credentials']

##variables de búsqueda de gmail
email_labels = config_file['gmail']['email_labels']
email_sender = config_file['gmail']['email_sender']
email_query_timedelta_days = config_file['gmail']['email_query_timedelta_days']
date_since_range = (datetime.now() - timedelta(days=email_query_timedelta_days)).strftime("%d-%b-%Y") #desde ayer
date_today = datetime.now().strftime("%d-%b-%Y")
email_subject = config_file['gmail']['email_subject']
email_attached_file = config_file['gmail']['email_attached_file']

#conexión al secret manager
def get_secret(project_id, secret_id, credentials_path, version_id="latest"):
    try:
        creds = Credentials.from_service_account_file(credentials_path)
        client = secretmanager.SecretManagerServiceClient(credentials = creds)
    except FileNotFoundError:
        client = secretmanager.SecretManagerServiceClient()
    secret_name_chosen = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    try:
        response = client.access_secret_version(name=secret_name_chosen)
        return response.payload.data.decode('UTF-8')
    except Exception as e:
        print('SecretManager Error: ' + e)
        return None
    
email_user = get_secret(project_id, secret_id_name, credentials_path, version_id="latest")
email_pass = get_secret(project_id, secret_id_name, credentials_path, version_id="latest")


# Conexión al servidor IMAP de Gmail y la cuenta de gmail
def gmail_imaplib_conexion(email_user, email_pass, imap4_sll_type):
    try:
        mail = imaplib.IMAP4_SSL(imap4_sll_type)
        mail.login(email_user, email_pass)
        return mail
    except Exception as e:
        print(e)

mail = gmail_imaplib_conexion(email_user, email_pass, imap4_sll_type)


def gmail_label_list(mail):
    try:
        label_list = [str(i).split("\"/\" \"")[1].split("\"\'")[0] for i in mail.list()[1]]
        return label_list
    except Exception as e:
        print(e)
label_list = gmail_label_list(mail)


# selección del buzón deseado de gmail
def gmail_label_selected(mail, email_labels):
    try:
        for i in email_labels:
            label_selected = mail.select(i)
        #return mail.select(i)
    except Exception as e:
        print(e)

gmail_label_selected(mail, email_labels)


def gmail_serach_query_creator(email_sender, date_since_range, email_subject, email_attached_file):
    gmail_query = f'(UNSEEN' 
    if email_sender and email_sender != "":
        gmail_query += f' FROM "{email_sender}"'
    if date_since_range and date_since_range != "":
        gmail_query += f' SINCE "{date_since_range}"'
    if email_subject and email_subject !="":
        gmail_query += f' SUBJECT "{email_subject}"'
    if email_attached_file and email_attached_file !="":
        gmail_query += f' TEXT "{email_attached_file}"' 
    gmail_query += ')'
    return gmail_query

gmail_query = gmail_serach_query_creator(email_sender, date_since_range, email_subject, email_attached_file)


def gmail_search_emails_status(gmail_query):
    try:
        status, messages_id = mail.search(None,gmail_query)
        return status
    except Exception as e:
        print(e)
        
def gmail_search_emails_messages_id(gmail_query):
    try:
        status, messages_id = mail.search(None,gmail_query)
        return messages_id[0].split()
    except Exception as e:
        print(e)
        
status = gmail_search_emails_status(gmail_query)
messages_id = gmail_search_emails_messages_id(gmail_query)


def email_read(messages_id, download_path):
    if messages_id == []:
        print("no messages in this query")
    try:
        for message_id in messages_id:
            # Obtiene el correo electrónico con el ID
            message_id_type, message_data = mail.fetch(message_id, "(RFC822)")
            for response_part in message_data:
                if isinstance(response_part, tuple):
                    # Parsea el mensaje utilizando la librería email
                    email_message = email.message_from_bytes(response_part[1])
        
                    # Obtiene el asunto y el remitente
                    subject, encoding = decode_header(email_message["Subject"])[0]
                    if isinstance(subject, bytes):
                        subject = subject.decode(encoding if encoding else "utf-8")
                    from_, encoding = decode_header(email_message.get("From"))[0]
                    if isinstance(from_, bytes):
                        from_ = from_.decode(encoding if encoding else "utf-8")
        
                    # Imprime el asunto y el remitente
                    print("From:", from_)
                    print("Subject:", subject)
        
                    # Si el mensaje es multipart (contiene partes diferentes, como texto y adjuntos)
                    if email_message.is_multipart():
                        for part in email_message.walk():
                            if part.get_content_type() == "text/plain":
                                body = part.get_payload(decode=True)
                                print("Body:", body.decode("utf-8"))
                            else:
                                # Guarda el archivo en la carpeta local
                                filename = part.get_filename()
                                if filename:
                                    filepath = os.path.join(download_path, filename)
                                    with open(filepath, "wb") as f:
                                        f.write(part.get_payload(decode=True))
                                    print(f"File saved in: {filepath}")
                                    print(f"Next email \n\n")
        # Cierra la conexión con el servidor IMAP
        mail.logout()
        
    except Exception as e:
        print(e)

email_read(messages_id, download_path)

