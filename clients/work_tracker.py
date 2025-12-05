import os
import logging
import re
from typing import List, Dict, Optional

from trello import TrelloClient
from trello.exceptions import ResourceUnavailable

logger = logging.getLogger(__name__)


class WorkTrackerClient:
    # Lead status -> Trello list name
    STATUS_MAPPING = {
        "NEW": "TODO",
        "CONTACTED": "IN_PROGRESS",
        "QUALIFIED": "DONE",
        "LOST": "LOST",
    }

    # Trello list name -> lead status
    REVERSE_STATUS_MAPPING = {
        "TODO": "NEW",
        "IN_PROGRESS": "CONTACTED",
        "DONE": "QUALIFIED",
        "LOST": "LOST",
    }

    def __init__(self):
        api_key = os.getenv("TRELLO_API_KEY")
        token = os.getenv("TRELLO_TOKEN")
        board_id = os.getenv("TRELLO_BOARD_ID")

        if not all([api_key, token, board_id]):
            raise ValueError("Missing Trello credentials")

        try:
            # Initialize Trello client
            self.client = TrelloClient(api_key=api_key, token=token)
            self.board = self.client.get_board(board_id)
            # Cache lists by name
            self.lists = {l.name: l for l in self.board.list_lists()}
            logger.info(f"Connected to Trello board: {board_id}")
            logger.info(f"Available lists: {list(self.lists.keys())}")
        except Exception as e:
            logger.error(f"Failed to initialize WorkTrackerClient: {e}")
            raise

    def get_all_cards(self) -> List[Dict]:
        # Return all cards from all lists as simple dicts
        try:
            all_cards = []
            for list_name, trello_list in self.lists.items():
                cards = trello_list.list_cards()
                status = self.REVERSE_STATUS_MAPPING.get(list_name, "UNKNOWN")
                for card in cards:
                    card_dict = {
                        "id": card.id,
                        "title": card.name,
                        "status": status,
                        "list_name": list_name,
                        "lead_id": self._extract_lead_id_from_description(card.desc),
                    }
                    all_cards.append(card_dict)
            logger.info(f"Retrieved {len(all_cards)} cards from Trello")
            return all_cards
        except Exception as e:
            logger.error(f"Error retrieving cards: {e}")
            raise

    def get_card_by_id(self, card_id: str) -> Optional[Dict]:
        # Get a single card by card_id
        try:
            card = self.board.get_card(card_id)
            list_name = None
            for list_obj in self.board.list_lists():
                if card.list_id == list_obj.id:
                    list_name = list_obj.name
                    break
            status = self.REVERSE_STATUS_MAPPING.get(list_name, "UNKNOWN")
            return {
                "id": card.id,
                "title": card.name,
                "status": status,
                "list_name": list_name,
                "lead_id": self._extract_lead_id_from_description(card.desc),
            }
        except ResourceUnavailable:
            logger.warning(f"Card {card_id} not found in Trello")
            return None
        except Exception as e:
            logger.error(f"Error retrieving card {card_id}: {e}")
            raise

    def create_card(self, title: str, lead_id: str, description: str = "") -> str:
        # Create a new card in the TODO list
        try:
            todo_list = self.lists.get("TODO")
            if not todo_list:
                raise ValueError("TODO list not found in Trello board")

            # Store lead_id at top of description for later parsing
            full_desc = f"Lead ID: {lead_id}\n{description}".strip()
            card = todo_list.add_card(name=title, desc=full_desc)
            logger.info(f"Created Trello card {card.id} for lead {lead_id}")
            return card.id
        except Exception as e:
            logger.error(f"Error creating card: {e}")
            raise

    def update_card_status(self, card_id: str, new_status: str) -> bool:
        # Move card to appropriate list based on new_status
        try:
            card = self.board.get_card(card_id)
            target_list_name = self.STATUS_MAPPING.get(new_status)
            if target_list_name is None:
                logger.info(f"Status {new_status} has no target list")
                return False

            target_list = self.lists.get(target_list_name)
            if not target_list:
                raise ValueError(f"Target list '{target_list_name}' not found")

            card.change_list(target_list.id)
            logger.info(f"Moved card {card_id} to {target_list_name} (status: {new_status})")
            return True
        except ResourceUnavailable:
            logger.warning(f"Card {card_id} not found")
            return False
        except Exception as e:
            logger.error(f"Error updating card {card_id}: {e}")
            raise

    def archive_card(self, card_id: str) -> bool:
        # Archive (close) a card instead of deleting
        try:
            card = self.board.get_card(card_id)
            card.set_closed(True)
            logger.info(f"Archived Trello card {card_id}")
            return True
        except ResourceUnavailable:
            logger.warning(f"Card {card_id} not found for archive")
            return False
        except Exception as e:
            logger.error(f"Error archiving card {card_id}: {e}")
            return False

    @staticmethod
    def _extract_lead_id_from_description(desc: str) -> Optional[str]:
        # Parse "Lead ID: <id>" pattern from description
        if not desc:
            return None
        match = re.search(r"Lead ID:\s*(\d+)", desc)
        return match.group(1) if match else None
