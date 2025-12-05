import json
import os
import logging
from typing import Dict
from datetime import datetime

from clients.lead_tracker import LeadTrackerClient
from clients.work_tracker import WorkTrackerClient

logger = logging.getLogger(__name__)


class SyncEngine:
    def __init__(self):
        # Initialize clients and load mapping file
        self.lead_client = LeadTrackerClient()
        self.work_client = WorkTrackerClient()
        self.mapping_file = os.getenv("MAPPING_FILE", "data/mapping.json")
        self.mapping = self._load_mapping()

    def _load_mapping(self) -> Dict:
        # Load mapping.json if present, else create default structure
        if os.path.exists(self.mapping_file):
            try:
                with open(self.mapping_file, "r") as f:
                    data = json.load(f)
                    logger.info(
                        f"Loaded mapping file with {len(data.get('lead_to_card', {}))} mappings"
                    )
                    return data
            except Exception as e:
                logger.error(f"Failed to load mapping file: {e}")

        return {
            "lead_to_card": {},
            "card_to_lead": {},
            "last_sync": None,
            "sync_count": 0,
        }

    def _save_mapping(self):
        # Persist mapping.json and update metadata
        try:
            os.makedirs(os.path.dirname(self.mapping_file), exist_ok=True)
            self.mapping["last_sync"] = datetime.now().isoformat()
            self.mapping["sync_count"] = self.mapping.get("sync_count", 0) + 1
            with open(self.mapping_file, "w") as f:
                json.dump(self.mapping, f, indent=2)
            logger.info(f"Saved mapping file (sync #{self.mapping['sync_count']})")
        except Exception as e:
            logger.error(f"Failed to save mapping file: {e}")
            raise

    def initial_sync(self):
        # Create cards for all eligible leads that do not yet have cards
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
                    skipped_count += 1
                    continue

                # Skip if mapping already exists
                if lead_id in self.mapping["lead_to_card"]:
                    skipped_count += 1
                    continue

                try:
                    card_id = self.work_client.create_card(
                        title=f"Follow-up: {lead.get('name')}",
                        lead_id=lead_id,
                        description=f"Email: {lead.get('email')}\nSource: {lead.get('source', '')}",
                    )

                    # Store bidirectional mapping
                    self.mapping["lead_to_card"][lead_id] = card_id
                    self.mapping["card_to_lead"][card_id] = lead_id

                    # Persist card id in sheet
                    self.lead_client.update_lead(lead_id, {"trello_card_id": card_id})

                    created_count += 1
                    logger.info(f"Created card for lead {lead_id}")
                except Exception as e:
                    logger.error(f"Failed to create card for lead {lead_id}: {e}")

            self._save_mapping()
            logger.info(f"Initial sync complete: {created_count} created, {skipped_count} skipped")
        except Exception as e:
            logger.error(f"Initial sync failed: {e}")
            raise

    def sync_lead_to_task(self, lead_id):
        # Sync a single lead's status to its Trello card, or archive card if lead vanished
        logger.info(f"Syncing lead {lead_id} -> task")
        try:
            lead = self.lead_client.get_lead_by_id(lead_id)

            # If lead is gone but mapping exists, archive the card
            if not lead:
                logger.warning(f"Lead {lead_id} not found in Sheet")
                card_id = self.mapping["lead_to_card"].get(str(lead_id))
                if card_id:
                    if self.work_client.archive_card(card_id):
                        logger.info(
                            f"Archived card {card_id} because lead {lead_id} no longer exists"
                        )
                        self.mapping["lead_to_card"].pop(str(lead_id), None)
                        self.mapping["card_to_lead"].pop(card_id, None)
                        self._save_mapping()
                    return True
                return False

            # Normal path: lead exists
            card_id = self.mapping["lead_to_card"].get(str(lead_id))

            # Fallback: use trello_card_id from sheet to repair mapping
            if not card_id:
                sheet_card_id = lead.get("trello_card_id")
                if sheet_card_id:
                    card_id = sheet_card_id
                    self.mapping["lead_to_card"][str(lead_id)] = card_id
                    self.mapping["card_to_lead"][card_id] = str(lead_id)
                    self._save_mapping()
                    logger.info(f"Repaired mapping for lead {lead_id}")
                else:
                    logger.warning(f"No mapped card for lead {lead_id}")
                    return False

            status = lead.get("status")
            self.work_client.update_card_status(card_id, status)
            return True
        except Exception as e:
            logger.error(f"Error syncing lead {lead_id}: {e}")
            return False

    def sync_task_to_lead(self, card_id):
        # Sync a single card's status to its lead, or delete lead if card vanished
        logger.info(f"Syncing task {card_id} -> lead")
        try:
            card = self.work_client.get_card_by_id(card_id)

            # If card is gone but mapping exists, delete the lead
            if not card:
                logger.warning(f"Card {card_id} not found in Trello")
                lead_id = self.mapping["card_to_lead"].get(card_id)
                if lead_id:
                    if self.lead_client.delete_lead(lead_id):
                        logger.info(
                            f"Deleted lead {lead_id} because card {card_id} no longer exists"
                        )
                        self.mapping["card_to_lead"].pop(card_id, None)
                        self.mapping["lead_to_card"].pop(str(lead_id), None)
                        self._save_mapping()
                    return True
                return False

            # Normal path: card exists
            lead_id = self.mapping["card_to_lead"].get(card_id)
            if not lead_id:
                logger.warning(f"No mapped lead for card {card_id}")
                return False

            status = card.get("status")
            self.lead_client.update_lead(lead_id, {"status": status})
            return True
        except Exception as e:
            logger.error(f"Error syncing card {card_id}: {e}")
            return False

    def sync_all_leads_to_tasks(self):
        # Bulk sync of all leads -> tasks
        logger.info("Running bulk leads -> tasks status sync...")
        leads = self.lead_client.get_all_leads()
        success_count = 0
        for lead in leads:
            lead_id = str(lead.get("id"))
            if self.sync_lead_to_task(lead_id):
                success_count += 1
        logger.info(f"Completed leads -> tasks sync: {success_count}/{len(leads)} successful")

    def sync_all_tasks_to_leads(self):
        # Bulk sync of all tasks -> leads
        logger.info("Running bulk tasks -> leads status sync...")
        cards = self.work_client.get_all_cards()
        success_count = 0
        for card in cards:
            card_id = card.get("id")
            if self.sync_task_to_lead(card_id):
                success_count += 1
        logger.info(f"Completed tasks -> leads sync: {success_count}/{len(cards)} successful")

    def sync_deleted_tasks(self):
        # Detect Trello cards deleted since last mapping and delete their leads
        logger.info("Checking for deleted tasks...")
        known_ids = set(self.mapping["card_to_lead"].keys())
        existing_cards = self.work_client.get_all_cards()
        existing_ids = {card["id"] for card in existing_cards}
        deleted_ids = known_ids - existing_ids
        logger.info(f"Found {len(deleted_ids)} deleted cards")

        for card_id in deleted_ids:
            lead_id = self.mapping["card_to_lead"].get(card_id)
            if not lead_id:
                continue
            if self.lead_client.delete_lead(lead_id):
                logger.info(f"Deleted lead {lead_id} because card {card_id} was deleted")
                self.mapping["card_to_lead"].pop(card_id, None)
                self.mapping["lead_to_card"].pop(str(lead_id), None)

        if deleted_ids:
            self._save_mapping()

    def sync_deleted_leads(self):
        # Detect leads deleted from sheet and archive their Trello cards
        logger.info("Checking for deleted leads...")
        known_lead_ids = set(self.mapping["lead_to_card"].keys())
        current_leads = self.lead_client.get_all_leads()
        existing_lead_ids = {str(lead.get("id")) for lead in current_leads}
        deleted_lead_ids = known_lead_ids - existing_lead_ids
        logger.info(f"Found {len(deleted_lead_ids)} deleted leads")

        for lead_id in deleted_lead_ids:
            card_id = self.mapping["lead_to_card"].get(lead_id)
            if not card_id:
                continue
            if self.work_client.archive_card(card_id):
                logger.info(f"Archived card {card_id} because lead {lead_id} was deleted")
                self.mapping["lead_to_card"].pop(lead_id, None)
                self.mapping["card_to_lead"].pop(card_id, None)

        if deleted_lead_ids:
            self._save_mapping()

    def full_sync(self):
        # Complete bidirectional sync with deletion handling
        logger.info("Starting full bidirectional sync...")
        try:
            # Create any missing cards
            self.initial_sync()
            # Handle deletions both ways
            self.sync_deleted_tasks()
            self.sync_deleted_leads()
            # Trello -> Sheets
            self.sync_all_tasks_to_leads()
            # Sheets -> Trello
            self.sync_all_leads_to_tasks()
            logger.info("Full sync completed successfully")
        except Exception as e:
            logger.error(f"Full sync failed: {e}")
            raise
