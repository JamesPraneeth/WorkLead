import json
import os
import logging
from typing import Dict
from datetime import datetime
from clients.lead_tracker import LeadTrackerClient
from clients.work_tracker import WorkTrackerClient

logger = logging.getLogger(__name__)

class SyncEngine:
    #Two-way sync orchestrator

    def __init__(self):
        self.lead_client = LeadTrackerClient()
        self.work_client = WorkTrackerClient()
        self.mapping_file = os.getenv("MAPPING_FILE", "data/mapping.json")
        self.mapping = self._load_mapping()
    
    def _load_mapping(self):
        # Load mapping file or create new one
        if os.path.exists(self.mapping_file):
            try:
                with open(self.mapping_file, "r") as f:
                    data = json.load(f)
                    logger.info(f"Loaded mapping file with {len(data.get('lead_to_card', {}))} mappings")
                    return data
            except Exception as e:
                logger.error(f"Failed to load mapping file: {str(e)}")
        
        return {
            "lead_to_card": {},
            "card_to_lead": {},
            "last_sync": None,
            "sync_count": 0,
        }
    
    def _save_mapping(self):
        #Persist mapping file
        try:
            os.makedirs(os.path.dirname(self.mapping_file), exist_ok=True)
            
            self.mapping["last_sync"] = datetime.now().isoformat()
            self.mapping["sync_count"] = self.mapping.get("sync_count", 0) + 1
            
            with open(self.mapping_file, "w") as f:
                json.dump(self.mapping, f, indent=2)
            
            logger.info(f"Saved mapping file (sync #{self.mapping['sync_count']})")
            
        except Exception as e:
            logger.error(f"Failed to save mapping file: {str(e)}")
            raise
    
    def initial_sync(self):
        #One-time sync: create cards for all leads (idempotent).
        logger.info("=" * 50)
        logger.info("STARTING INITIAL SYNC (Leads -> Cards)")
        logger.info("=" * 50)
        
        try:
            leads = self.lead_client.get_all_leads()
            created_count = 0
            skipped_count = 0
            
            for lead in leads:
                lead_id = str(lead.get("id"))
                status = lead.get("status")
                
                # Skip LOST leads
                if status == "LOST":
                    logger.debug(f"Skipping LOST lead {lead_id}")
                    skipped_count += 1
                    continue
                
                # Check if mapping already exists (idempotency)
                if lead_id in self.mapping["lead_to_card"]:
                    logger.debug(f"Lead {lead_id} already has card {self.mapping['lead_to_card'][lead_id]}")
                    skipped_count += 1
                    continue
                
                # Create card
                try:
                    card_id = self.work_client.create_card(
                        title=f"Follow-up: {lead.get('name')}",
                        lead_id=lead_id,
                        description=f"Email: {lead.get('email')}\nSource: {lead.get('source', 'Unknown')}",
                    )
                    
                    # Store bidirectional mapping
                    self.mapping["lead_to_card"][lead_id] = card_id
                    self.mapping["card_to_lead"][card_id] = lead_id
                    
                    # Update lead with card ID in Google Sheets
                    self.lead_client.update_lead(lead_id, {"trello_card_id": card_id})
                    
                    created_count += 1
                    logger.info(f"Created card for lead {lead_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to create card for lead {lead_id}: {str(e)}")
            
            self._save_mapping()
            logger.info(f"Initial sync complete: {created_count} created, {skipped_count} skipped")
            
        except Exception as e:
            logger.error(f"Initial sync failed: {str(e)}")
            raise
    
    def sync_lead_to_task(self, lead_id):
        #Sync a specific lead's status to its task.
        logger.info(f"Syncing lead {lead_id} -> task")
        
        try:
            lead = self.lead_client.get_lead_by_id(lead_id)
            if not lead:
                logger.warning(f"Lead {lead_id} not found")
                return False
            
            # Try mapping.json first
            card_id = self.mapping["lead_to_card"].get(str(lead_id))
            
            # Fallback to trello_card_id from sheet
            if not card_id:
                sheet_card_id = lead.get("trello_card_id")
                if sheet_card_id:
                    card_id = sheet_card_id
                    # Repair mapping.json
                    self.mapping["lead_to_card"][str(lead_id)] = card_id
                    self.mapping["card_to_lead"][card_id] = str(lead_id)
                    self._save_mapping()
                    logger.info(f"Repaired mapping for lead {lead_id}")
                else:
                    logger.warning(f"No mapped card for lead {lead_id}")
                    return False
            
            # Update card status
            status = lead.get("status")
            self.work_client.update_card_status(card_id, status)
            
            return True
            
        except Exception as e:
            logger.error(f"Error syncing lead {lead_id}: {str(e)}")
            return False
    
    def sync_task_to_lead(self, card_id: str) -> bool:
        # Sync a specific task's status back to its lead.
        logger.info(f"Syncing task {card_id} -> lead")
        
        try:
            card = self.work_client.get_card_by_id(card_id)
            if not card:
                logger.warning(f"Card {card_id} not found")
                return False
            
            lead_id = self.mapping["card_to_lead"].get(card_id)
            if not lead_id:
                logger.warning(f"No mapped lead for card {card_id}")
                return False
            
            # Update lead status
            status = card.get("status")
            self.lead_client.update_lead(lead_id, {"status": status})
            
            return True
            
        except Exception as e:
            logger.error(f"Error syncing card {card_id}: {str(e)}")
            return False
    
    def sync_all_leads_to_tasks(self):
        # Sync ALL leads -> their tasks (bulk operation).
        logger.info("Running bulk leads -> tasks status sync...")
        
        leads = self.lead_client.get_all_leads()
        success_count = 0
        
        for lead in leads:
            lead_id = str(lead.get("id"))
            if self.sync_lead_to_task(lead_id):
                success_count += 1
        
        logger.info(f"Completed leads -> tasks sync: {success_count}/{len(leads)} successful")
    
    def sync_all_tasks_to_leads(self):
        #Sync ALL tasks -> their leads (bulk operation).
        logger.info("Running bulk tasks -> leads status sync...")
        
        cards = self.work_client.get_all_cards()
        success_count = 0
        
        for card in cards:
            card_id = card.get("id")
            if self.sync_task_to_lead(card_id):
                success_count += 1
        
        logger.info(f"Completed tasks -> leads sync: {success_count}/{len(cards)} successful")
    
    def full_sync(self):
        #Complete bidirectional sync (idempotent).
        logger.info("Starting full bidirectional sync...")
        
        try:
            # Step 1: Initial sync (creates missing cards)
            self.initial_sync()
            
            # Step 2: Sync task updates to leads
            logger.info("Checking for updated tasks...")
            self.sync_all_tasks_to_leads()

            # Step 3: Sync lead updates to tasks
            logger.info("Checking for updated leads...")
            self.sync_all_leads_to_tasks()
            logger.info("Full sync completed successfully")
            
        except Exception as e:
            logger.error(f"Full sync failed: {str(e)}")
            raise
