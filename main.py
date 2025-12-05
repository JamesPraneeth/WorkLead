import os
import sys
from dotenv import load_dotenv
from core.logger import setup_logger
from core.sync_logic import SyncEngine

logger = setup_logger(__name__)

def validate_env():
    required_env_vars = [
        "GOOGLE_CREDENTIALS_PATH",
        "SPREADSHEET_ID",
        "TRELLO_API_KEY",
        "TRELLO_TOKEN",
        "TRELLO_BOARD_ID",
    ]
    missing = [v for v in required_env_vars if not os.getenv(v)]
    if missing:
        logger.error(f"Missing environment variables: {', '.join(missing)}")
        sys.exit(1)

def print_menu():
    print("\n==============================")
    print("WELCOME TO WORKLEAD SYNC TOOL")
    print("==============================")
    print("Choose an option:")
    print("1. Initial sync (Leads -> Cards)")
    print("2. Full bidirectional sync")
    print("3. Bulk sync: ALL leads -> tasks (Sheets -> Trello)")
    print("4. Bulk sync: ALL tasks -> leads (Trello -> Sheets)")
    print("5. Sync ONE lead -> task (by lead ID)")
    print("6. Sync ONE task -> lead (by lead ID or card ID)")
    print("Q. Quit")
    print("==============================")

def main():
    load_dotenv()
    validate_env()

    try:
        sync = SyncEngine()
    except Exception as e:
        logger.error(f"Failed to initialize sync engine: {e}", exc_info=True)
        sys.exit(1)

    while True:
        print_menu()
        choice = input("Enter your choice: ").strip()

        if choice.lower() == "q":
            print("\nTHANK YOU FOR USING WORKLEAD SYNC TOOL.")
            break

        try:
            if choice == "1":
                logger.info("User selected: Initial sync (Leads -> Cards)")
                sync.initial_sync()

            elif choice == "2":
                logger.info("User selected: Full bidirectional sync")
                sync.full_sync()

            elif choice == "3":
                logger.info("User selected: Bulk leads -> tasks sync")
                sync.sync_all_leads_to_tasks()

            elif choice == "4":
                logger.info("User selected: Bulk tasks -> leads sync")
                sync.sync_all_tasks_to_leads()

            elif choice == "5":
                lead_id = input("Enter lead ID to sync (lead -> task): ").strip()
                if not lead_id:
                    print("Lead ID cannot be empty.")
                else:
                    logger.info(f"User selected: Sync lead {lead_id} -> task")
                    sync.sync_lead_to_task(lead_id)

            elif choice == "6":
                id_input = input(
                    "Enter lead ID (number) or Trello card ID to sync (task -> lead): "
                ).strip()
                if not id_input:
                    print("ID cannot be empty.")
                else:
                    # If numeric, treat as lead ID and resolve card_id via trello_card_id
                    if id_input.isdigit():
                        lead_id = id_input
                        lead = sync.lead_client.get_lead_by_id(lead_id)
                        if not lead or not lead.get("trello_card_id"):
                            logger.error(f"No trello_card_id found for lead {lead_id}")
                            print(f"No trello_card_id found for lead {lead_id}")
                        else:
                            card_id = lead["trello_card_id"]
                            logger.info(
                                f"Found lead {lead_id} linked to card {card_id} (task -> lead)"
                            )
                            sync.sync_task_to_lead(card_id)
                    else:
                        card_id = id_input
                        logger.info(f"User selected: Sync card {card_id} -> lead")
                        sync.sync_task_to_lead(card_id)

            else:
                print("Invalid choice. Please enter a valid option (1-6 or Q).")

        except Exception as e:
            logger.error(f"Error while processing choice {choice}: {e}", exc_info=True)
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
