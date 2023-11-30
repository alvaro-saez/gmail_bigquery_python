from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pickle 
import base64 
import os
from datetime import datetime
from datetime import datetime, timedelta

def conexion_gmail_api(SCOPES,client_secret_file):
    creds = None
  
    # The file token.pickle contains the user access token. 
    # Check if it exists 
    if os.path.exists('token.pickle'): 
      
        # Read the token from the file and store it in the variable creds 
        with open('token.pickle', 'rb') as token: 
            creds = pickle.load(token) 
      
    # If credentials are not available or are invalid, ask the user to log in. 
    if not creds or not creds.valid: 
        if creds and creds.expired and creds.refresh_token: 
            creds.refresh(Request()) 
        else: 
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES) 
            creds = flow.run_local_server(port=0) 
      
        # Save the access token in token.pickle file for the next run 
        with open('token.pickle', 'wb') as token: 
            pickle.dump(creds, token) 
      
    # Connect to the Gmail API 
    service = build('gmail', 'v1', credentials=creds) 
    return service

service = conexion_gmail_api(SCOPES,client_secret_file)

def mark_as_read(message_id):
    try:
        # Actualizar el estado del mensaje a leído
        body = {'removeLabelIds': ['UNREAD']}
        service.users().messages().modify(userId='me', id=message_id, body=body).execute()
        print(f'Mensaje marcado como leído.')

    except Exception as e:
        print(f'Error al marcar el mensaje como leído: {e}')

def get_filtered_messages(subject_filter, unread_filter, sender_filter, start_date_filter, attachment_filter, attachment_file_type_filter):
    try:
        # Construcción de la consulta
        query = f'is:'

        if unread_filter:
            query += f' {unread_filter}'

        if subject_filter:
            query += f' subject:{subject_filter}'
        
        if sender_filter:
            query += f' from:{sender_filter}'

        if start_date_filter:
            formatted_date = datetime.strptime(start_date_filter, '%Y-%m-%d').strftime('%Y/%m/%d')
            query += f' after:{formatted_date}'

        if attachment_filter:
            query += f' has:{attachment_filter}'

        if attachment_file_type_filter:
            query += f' filename:{attachment_file_type_filter}'

        # Obtención de mensajes no leídos con los filtros aplicados
        response = service.users().messages().list(userId='me', q=query).execute()
        messages = response.get('messages', [])
        if messages == []:
            print('No hay mensajes con ese filtro')

        return messages

    except Exception as e:
        print(f'Error al obtener mensajes filtrados: {e}')
        return []

def download_attachment(message_id, attachment_name, save_path):
    try:
        # Obtención del mensaje
        message = service.users().messages().get(userId='me', id=message_id).execute()

        # Iteración sobre las partes del mensaje
        for part in message['payload']['parts']:
            if 'filename' in part and part['filename'] == attachment_name:
                # Descarga del archivo adjunto
                attachment = service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=part['body']['attachmentId']
                ).execute()

                file_data = base64.urlsafe_b64decode(attachment['data'])

                # Guardar el archivo en el sistema local
                with open(save_path, 'wb') as f:
                    f.write(file_data)

                print(f'Archivo descargado en: {save_path}')
                mark_as_read(message_id)
                return True

        print(f'Archivo adjunto no encontrado: {attachment_name}')
        return False

    except Exception as e:
        print(f'Error al descargar el archivo adjunto: {e}')
        return False



SCOPES = ["https://mail.google.com/", "https://www.googleapis.com/auth/drive.readonly", "https://www.googleapis.com/auth/gmail.modify"]
client_secret_file = './credenciales/client_secret_python_gmail.json'

# variables para filtar el email
subject_filter = 'test gmail csv'  # Reemplazar con el asunto real del correo electrónico
unread_filter = 'unread'
sender_filter = 'alvarosaezsanchez@gmail.com'  # Reemplazar con el remitente real del correo electrónico
start_date_filter = (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d") #desde ayer
save_path = './files/cars.csv'  # Reemplazar con la ruta real de destino
attachment_filter = 'attachment'
attachment_file_type_filter = 'csv'
attachment_name= 'cars.csv'

# Ejecución de funciones
filtered_messages = get_filtered_messages(subject_filter, unread_filter, sender_filter, start_date_filter,attachment_filter,attachment_file_type_filter)

for message in filtered_messages:
    message_id = message['id']
    download_attachment(message_id, attachment_name, save_path)

