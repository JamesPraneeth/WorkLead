import os
import logging
from typing import List, Dict, Optional
import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

class LeadTrackerClient:
    #Client for Google Sheets Lead Tracker

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    
    # Column headers (must match your sheet exactly)
    HEADERS = ["id", "name", "email", "status", "source", "trello_card_id"]
    
    def __init__(self):
        #Initialize Google Sheets client with service account credentials
        creds_path = os.getenv("GOOGLE_CREDENTIALS_PATH")
        
        if not creds_path or not os.path.exists(creds_path):
            raise FileNotFoundError(
                f"Credentials file not found at {creds_path}. "
                "Download from Google Cloud Console and place in project root."
            )
        
        try:
            creds = Credentials.from_service_account_file(creds_path, scopes=self.SCOPES)
            self.gc = gspread.authorize(creds)
            
            sheet_id = os.getenv("SPREADSHEET_ID")
            if not sheet_id:
                raise ValueError("SPREADSHEET_ID not set in .env")
            
            self.sheet = self.gc.open_by_key(sheet_id).sheet1
            logger.info(f"Connected to Google Sheet: {sheet_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize LeadTrackerClient: {str(e)}")
            raise
    
    def get_all_leads(self):
        #Fetch all leads from sheet.
        try:
            records = self.sheet.get_all_records()
            logger.info(f"Retrieved {len(records)} leads from Google Sheets")
            return records
        except gspread.exceptions.APIError as e:
            logger.error(f"Google Sheets API error: {str(e)}")
            raise
    
    def get_lead_by_id(self, lead_id):
        #Find a specific lead by ID.
        try:
            records = self.get_all_leads()
            for record in records:
                if str(record.get("id")) == str(lead_id):
                    return record
            
            logger.warning(f"Lead with id {lead_id} not found")
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving lead {lead_id}: {str(e)}")
            raise
    
    def create_lead(self, lead_data):
        #Create a new lead in the sheet.
        required_fields = ["name", "email", "status"]
        for field in required_fields:
            if field not in lead_data or not lead_data[field]:
                raise ValueError(f"Missing required field: {field}")
        
        try:
            all_leads = self.get_all_leads()
            lead_id = str(len(all_leads) + 1)
            
            row = [
                lead_id,
                lead_data["name"],
                lead_data["email"],
                lead_data["status"],
                lead_data.get("source", ""),
                "",  # trello_card_id initially empty
            ]
            
            self.sheet.append_row(row)
            logger.info(f"Created lead {lead_id}: {lead_data['name']}")
            return lead_id
            
        except Exception as e:
            logger.error(f"Error creating lead: {str(e)}")
            raise
    
    def update_lead(self, lead_id, updates):
        try:
            records = self.get_all_leads()
            
            for idx, record in enumerate(records):
                if str(record.get("id")) == str(lead_id):
                    row_num = idx + 2  # Header is row 1, data starts at row 2
                    
                    for field, value in updates.items():
                        if field in self.HEADERS:
                            col_num = self.HEADERS.index(field) + 1
                            self.sheet.update_cell(row_num, col_num, value)
                            logger.info(f"Updated lead {lead_id}: {field} = {value}")
                    
                    return True
            
            logger.warning(f"Lead {lead_id} not found for update")
            return False
            
        except Exception as e:
            logger.error(f"Error updating lead {lead_id}: {str(e)}")
            raise
    def delete_lead(self, lead_id: str) -> bool:
    #Delete the row for a given lead_id from the sheet.
        try:
            records = self.get_all_leads()
            for idx, record in enumerate(records):
                if str(record.get("id")) == str(lead_id):
                    row_num = idx + 2  # header is row 1
                    self.sheet.delete_rows(row_num)
                    logger.info(f"Deleted lead {lead_id} from Google Sheets")
                    return True
            logger.warning(f"Lead {lead_id} not found for deletion")
            return False
        except Exception as e:
            logger.error(f"Error deleting lead {lead_id}: {e}")
            return False

