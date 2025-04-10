import os
import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

class SavingOnDrive:
    """Class to handle uploading files to Google Drive with date-based folders"""
    def __init__(self, credentials_path='credentials.json'):
        self.credentials_path = credentials_path
        self.drive_service = None
        self.target_folders = [
            "1NzaP1VFqfSdzkCqfPkcpI5s_43nLDLAG",
            "1hxBqJwK5g7EXAV0JcVc0_YLtBReCkaRv"
        ]
    
    def authenticate(self):
        try:
            SCOPES = ['https://www.googleapis.com/auth/drive']
            credentials = Credentials.from_service_account_file(
                self.credentials_path, scopes=SCOPES)
            self.drive_service = build('drive', 'v3', credentials=credentials)
            return True
        except Exception as e:
            logging.error(f"Authentication error: {str(e)}")
            return False
    
    def create_date_folder(self, parent_folder_id):
        try:
            today_date = datetime.datetime.now().strftime("%Y-%m-%d")
            query = f"name='{today_date}' and mimeType='application/vnd.google-apps.folder' and '{parent_folder_id}' in parents and trashed=false"
            results = self.drive_service.files().list(
                q=query, spaces='drive', fields='files(id, name)').execute()
            existing_folders = results.get('files', [])
            if existing_folders:
                logging.info(f"Folder {today_date} already exists in parent folder {parent_folder_id}")
                return existing_folders[0]['id']
            folder_metadata = {
                'name': today_date,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_folder_id]
            }
            folder = self.drive_service.files().create(
                body=folder_metadata, fields='id').execute()
            folder_id = folder.get('id')
            logging.info(f"Created folder {today_date} with ID: {folder_id} in parent folder {parent_folder_id}")
            return folder_id
        except Exception as e:
            logging.error(f"Error creating date folder: {str(e)}")
            return None
    
    def upload_file(self, file_path, folder_id, file_name=None):
        try:
            if not self.drive_service and not self.authenticate():
                return None
            if file_name is None:
                file_name = os.path.basename(file_path)
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            name_parts = os.path.splitext(file_name)
            unique_file_name = f"{name_parts[0]}_{timestamp}{name_parts[1]}"
            file_metadata = {'name': unique_file_name, 'parents': [folder_id]}
            media = MediaFileUpload(
                file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', resumable=True)
            file = self.drive_service.files().create(
                body=file_metadata, media_body=media, fields='id').execute()
            logging.info(f"File uploaded successfully to folder {folder_id}")
            return file.get('id')
        except Exception as e:
            logging.error(f"Upload error: {str(e)}")
            return None
    
    def upload_to_multiple_folders(self, file_path, file_name=None):
        if not self.authenticate():
            logging.error("Failed to authenticate with Google Drive")
            return []
        file_ids = []
        for parent_folder_id in self.target_folders:
            date_folder_id = self.create_date_folder(parent_folder_id)
            if date_folder_id:
                file_id = self.upload_file(file_path, date_folder_id, file_name)
                if file_id:
                    file_ids.append(file_id)
            else:
                logging.error(f"Failed to create/find date folder in parent folder {parent_folder_id}")
        return file_ids


# class SavingOnDrive:
#     """Class to handle uploading files to Google Drive with date-based folders"""
    
#     def __init__(self, credentials_path='credentials.json'):
#         """
#         Initialize the DriveUploader with Google Drive API credentials.
        
#         Args:
#             credentials_path: Path to the service account credentials JSON file
#         """
#         self.credentials_path = credentials_path
#         self.drive_service = None
        
#         # The folder IDs for the two target locations
#         self.target_folders = [
#             "1NzaP1VFqfSdzkCqfPkcpI5s_43nLDLAG",  # First folder
#             "1hxBqJwK5g7EXAV0JcVc0_YLtBReCkaRv"   # Second folder
#         ]
    
#     def authenticate(self):
#         """
#         Authenticate with Google Drive API using service account credentials
        
#         Returns:
#             bool: True if authentication successful, False otherwise
#         """
#         try:
#             # Define the scopes required for Google Drive access
#             SCOPES = ['https://www.googleapis.com/auth/drive']
            
#             # Load credentials from the service account file
#             credentials = Credentials.from_service_account_file(
#                 self.credentials_path, scopes=SCOPES)
            
#             # Build the Drive API service
#             self.drive_service = build('drive', 'v3', credentials=credentials)
#             return True
            
#         except Exception as e:
#             print(f"Authentication error: {str(e)}")
#             return False
    
#     def create_date_folder(self, parent_folder_id):
#         """
#         Create a folder with today's date in the specified parent folder
        
#         Args:
#             parent_folder_id: ID of the parent folder
            
#         Returns:
#             str: Folder ID of the created date folder, None if failed
#         """
#         try:
#             # Get today's date in YYYY-MM-DD format
#             today_date = datetime.datetime.now().strftime("%Y-%m-%d")
            
#             # Check if folder already exists
#             query = f"name='{today_date}' and mimeType='application/vnd.google-apps.folder' and '{parent_folder_id}' in parents and trashed=false"
#             results = self.drive_service.files().list(
#                 q=query,
#                 spaces='drive',
#                 fields='files(id, name)'
#             ).execute()
            
#             existing_folders = results.get('files', [])
            
#             # If folder already exists, return its ID
#             if existing_folders:
#                 print(f"Folder {today_date} already exists in parent folder {parent_folder_id}")
#                 return existing_folders[0]['id']
            
#             # Create new folder
#             folder_metadata = {
#                 'name': today_date,
#                 'mimeType': 'application/vnd.google-apps.folder',
#                 'parents': [parent_folder_id]
#             }
            
#             folder = self.drive_service.files().create(
#                 body=folder_metadata,
#                 fields='id'
#             ).execute()
            
#             folder_id = folder.get('id')
#             print(f"Created folder {today_date} with ID: {folder_id} in parent folder {parent_folder_id}")
#             return folder_id
            
#         except Exception as e:
#             print(f"Error creating date folder: {str(e)}")
#             return None
    
#     def upload_file(self, file_path, folder_id, file_name=None):
#         """
#         Upload a file to a specific Google Drive folder
        
#         Args:
#             file_path: Path to the file to upload
#             folder_id: ID of the folder to upload to
#             file_name: Optional name to use for the file in Drive (default: original filename)
            
#         Returns:
#             str: File ID if upload successful, None otherwise
#         """
#         try:
#             if not self.drive_service:
#                 if not self.authenticate():
#                     return None
            
#             # Get the base file name if not provided
#             if file_name is None:
#                 file_name = os.path.basename(file_path)
            
#             # Add timestamp to ensure uniqueness and prevent overwriting
#             timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
#             name_parts = os.path.splitext(file_name)
#             unique_file_name = f"{name_parts[0]}_{timestamp}{name_parts[1]}"
            
#             # Define file metadata
#             file_metadata = {
#                 'name': unique_file_name,
#                 'parents': [folder_id]
#             }
            
#             # Create media object for the file
#             media = MediaFileUpload(
#                 file_path,
#                 mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
#                 resumable=True
#             )
            
#             # Execute the upload
#             file = self.drive_service.files().create(
#                 body=file_metadata,
#                 media_body=media,
#                 fields='id'
#             ).execute()
            
#             print(f"File uploaded successfully to folder {folder_id}")
#             return file.get('id')
            
#         except Exception as e:
#             print(f"Upload error: {str(e)}")
#             return None
    
#     def upload_to_multiple_folders(self, file_path, file_name=None):
#         """
#         Upload a file to date-based folders within multiple parent folders
        
#         Args:
#             file_path: Path to the file to upload
#             file_name: Optional name to use for the file in Drive
            
#         Returns:
#             list: List of file IDs for each successful upload
#         """
#         if not self.authenticate():
#             print("Failed to authenticate with Google Drive")
#             return []
        
#         file_ids = []
        
#         for parent_folder_id in self.target_folders:
#             # Create or get date folder
#             date_folder_id = self.create_date_folder(parent_folder_id)
            
#             if date_folder_id:
#                 # Upload file to the date folder
#                 file_id = self.upload_file(file_path, date_folder_id, file_name)
#                 if file_id:
#                     file_ids.append(file_id)
#             else:
#                 print(f"Failed to create/find date folder in parent folder {parent_folder_id}")
        
#         return file_ids
