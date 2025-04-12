import os
import datetime
import json
import logging
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from retry import retry

class SavingOnDrive:
    """Class to handle uploading files to Google Drive with date-based folders"""
    
    def __init__(self, credentials_json=None):
        """
        Initialize the DriveUploader with Google Drive API credentials.
        
        Args:
            credentials_json: JSON string containing service account credentials
        """
        self.credentials_json = credentials_json
        self.drive_service = None
        # The folder IDs for the two target locations
        self.target_folders = [
            "1NzaP1VFqfSdzkCqfPkcpI5s_43nLDLAG",  # First folder
            "1hxBqJwK5g7EXAV0JcVc0_YLtBReCkaRv"   # Second folder
        ]
    
    def authenticate(self):
        """
        Authenticate with Google Drive API using service account credentials
        
        Returns:
            bool: True if authentication successful, False otherwise
        """
        try:
            if not self.credentials_json:
                logging.error("No credentials provided. Ensure TALABAT_GCLOUD_KEY_JSON is set.")
                return False
            # Parse JSON credentials
            try:
                credentials_dict = json.loads(self.credentials_json)
                if not credentials_dict.get('type') == 'service_account':
                    logging.error("Provided credentials are not a valid service account key")
                    return False
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON in credentials: {str(e)}")
                return False
            # Define the scopes required for Google Drive access
            SCOPES = ['https://www.googleapis.com/auth/drive']
            # Load credentials from JSON dictionary
            credentials = Credentials.from_service_account_info(
                credentials_dict, scopes=SCOPES)
            # Build the Drive API service
            self.drive_service = build('drive', 'v3', credentials=credentials)
            logging.info("Successfully authenticated with Google Drive")
            return True
        except Exception as e:
            logging.error(f"Authentication error: {str(e)}")
            return False
    
    @retry(tries=3, delay=2, backoff=2, logger=logging.getLogger(__name__))
    def create_date_folder(self, parent_folder_id):
        """
        Create a folder with today's date in the specified parent folder
        
        Args:
            parent_folder_id: ID of the parent folder
            
        Returns:
            str: Folder ID of the created date folder, None if failed
        """
        try:
            if not self.drive_service:
                if not self.authenticate():
                    return None
            # Get today's date in YYYY-MM-DD format
            today_date = datetime.datetime.now().strftime("%Y-%m-%d")
            # Check if folder already exists
            query = f"name='{today_date}' and mimeType='application/vnd.google-apps.folder' and '{parent_folder_id}' in parents and trashed=false"
            results = self.drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            existing_folders = results.get('files', [])
            # If folder already exists, return its ID
            if existing_folders:
                logging.info(f"Folder {today_date} already exists in parent folder {parent_folder_id}")
                return existing_folders[0]['id']
            # Create new folder
            folder_metadata = {
                'name': today_date,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_folder_id]
            }
            folder = self.drive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            folder_id = folder.get('id')
            logging.info(f"Created folder {today_date} with ID: {folder_id} in parent folder {parent_folder_id}")
            return folder_id
        except Exception as e:
            logging.error(f"Error creating date folder: {str(e)}")
            raise
    
    @retry(tries=3, delay=2, backoff=2, logger=logging.getLogger(__name__))
    def upload_file(self, file_path, folder_id, file_name=None):
        """
        Upload a file to a specific Google Drive folder
        
        Args:
            file_path: Path to the file to upload
            folder_id: ID of the folder to upload to
            file_name: Optional name to use for the file in Drive (default: original filename)
            
        Returns:
            str: File ID if upload successful, None otherwise
        """
        try:
            if not self.drive_service:
                if not self.authenticate():
                    return None
            # Get the base file name if not provided
            if file_name is None:
                file_name = os.path.basename(file_path)
            # Add timestamp to ensure uniqueness and prevent overwriting
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            name_parts = os.path.splitext(file_name)
            unique_file_name = f"{name_parts[0]}_{timestamp}{name_parts[1]}"
            # Define file metadata
            file_metadata = {
                'name': unique_file_name,
                'parents': [folder_id]
            }
            # Create media object for the file
            media = MediaFileUpload(
                file_path,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                resumable=True
            )
            # Execute the upload
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            logging.info(f"File uploaded successfully to folder {folder_id}")
            return file.get('id')
        except Exception as e:
            logging.error(f"Upload error: {str(e)}")
            raise
    
    def upload_to_multiple_folders(self, file_path, file_name=None):
        """
        Upload a file to date-based folders within multiple parent folders
        
        Args:
            file_path: Path to the file to upload
            file_name: Optional name to use for the file in Drive
            
        Returns:
            list: List of file IDs for each successful upload
        """
        if not self.drive_service and not self.authenticate():
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
