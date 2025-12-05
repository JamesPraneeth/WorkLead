## WORKLEAD - A Python based automation and integration of two SaaS systems.

Lead Tracker (Google Sheets) ⇄ Work Tracker (Trello)


## Overview

This project implements a small but realistic two-way sync integration between:

- **Lead Tracker:** Google Sheets  
- **Work Tracker:** Trello  

The sync layer:

- Creates Trello tasks (cards) for leads stored in Google Sheets.
- Keeps statuses in sync in both directions (Sheets → Trello and Trello → Sheets).
- Handles deletions (lead deleted → archive card, card deleted → delete lead).
- Is **idempotent**: safe to run multiple times without creating duplicates.
- Includes logging and basic error handling.

---

## Architecture & Flow

### High-level components

- `clients/lead_tracker.py`  
  Google Sheets client: CRUD operations for leads.

- `clients/work_tracker.py`  
  Trello client: CRUD-like operations for cards and status (list) moves.

- `core/sync_logic.py`  
  Sync engine: all orchestration, mapping, idempotency and deletion handling.

- `core/logger.py`  
  Centralized logging configuration (file + console).

- `main.py`  
  CLI entry point used to trigger different sync operations.

### Data flow (conceptual)

```text
 Google Sheets (Lead Tracker)             Trello (Work Tracker)
 ─────────────────────────────            ──────────────────────
 id, name, email, status, source   ⇄   card id, title, list (status)
              ▲                                ▲
              │                                │
              └──────── SyncEngine ────────────┘
                       (mapping.json)
```

### Status mapping

Lead status in Google Sheets is mapped to Trello lists as follows:

| Lead status (Sheets) | Trello list (Board) |
|----------------------|---------------------|
| NEW                  | TODO                |
| CONTACTED            | IN_PROGRESS         |
| QUALIFIED            | DONE                |
| LOST                 | LOST                |

The same mapping is used in reverse when syncing Trello → Sheets.

### Mapping and idempotency

A JSON mapping file (`data/mapping.json`) tracks relationships:

```json
{
  "lead_to_card": {
    "1": "trello_card_id_1"
  },
  "card_to_lead": {
    "trello_card_id_1": "1"
  },
  "last_sync": "2025-12-05T22:00:00",
  "sync_count": 3
}
```

This mapping is used to:

- Avoid creating duplicate cards for the same lead.
- Quickly look up the counterpart (lead/card) during sync.
- Detect items that have been deleted in one system.

---

## Setup Instructions

### 1. Tools & accounts

You will need:

- A **Google account**
- A **Trello account** (free workspace)
- Python 3.8+ installed locally

### 2. Clone repository and install dependencies

```bash
git clone https://github.com/your-user/WorkLead.git
cd WorkLead

pip install -r requirements.txt
```

Ensure `requirements.txt` includes (versions are examples):

```text
python-dotenv
gspread
google-auth
py-trello
```

### 3. Google Sheets configuration

1. Create a new Google Sheet (e.g. "Lead Tracker").
2. In the first sheet (Sheet1), create the header row with **exactly**:

   ```text
   id | name | email | status | source | trello_card_id
   ```

3. Get the Spreadsheet ID from the URL:  
   `https://docs.google.com/spreadsheets/d/<SPREADSHEET_ID>/edit`

4. In Google Cloud Console:
   - Create (or select) a project.
   - Enable:
     - Google Sheets API
     - Google Drive API
   - Create a **Service Account**.
   - Generate a JSON key and download it as `credentials.json` into the project root.

5. Share the Sheet with the Service Account email:
   - Open the sheet → Share
   - Add `<service-account-email>` with **Editor** permission.

### 4. Trello configuration

1. Create a new Trello board (e.g. "Work Tracker").
2. Add **exactly** these lists (by name):

   - `TODO`
   - `IN_PROGRESS`
   - `DONE`
   - `LOST`

3. Get Trello API key and token:
   - Go to https://trello.com/app-key
   - Copy your **API key**
   - Generate and copy your **token**

4. Get the **board ID**:
   - Open the board in a browser.
   - Add `.json` to the URL: `https://trello.com/b/<BOARD_SHORT_ID>.json`
   - In the JSON, find the `"id"` field – that is the board ID.

### 5. Environment variables

Create a `.env` file in the project root:

```env
# Google Sheets
GOOGLE_CREDENTIALS_PATH=credentials.json
SPREADSHEET_ID=your_spreadsheet_id_here

# Trello
TRELLO_API_KEY=your_trello_api_key_here
TRELLO_TOKEN=your_trello_token_here
TRELLO_BOARD_ID=your_trello_board_id_here

# Optional
MAPPING_FILE=data/mapping.json
```

Make sure `.env`, `credentials.json`, `data/`, and `logs/` are **gitignored**.

---

## Usage

### Start the CLI

```bash
python main.py
```

You will see:

```text
==============================
WELCOME TO WORKLEAD SYNC TOOL
==============================
Choose an option:
1. Initial sync (Leads -> Cards)
2. Full bidirectional sync
3. Bulk sync: ALL leads -> tasks (Sheets -> Trello)
4. Bulk sync: ALL tasks -> leads (Trello -> Sheets)
5. Sync ONE lead -> task (by lead ID)
6. Sync ONE task -> lead (by lead ID or card ID)
Q. Quit
==============================
```

### What each option does

- **1. Initial sync (Leads → Cards)**  
  - Reads all leads from Google Sheets.
  - Skips leads with status `LOST`.
  - For each lead without a card, creates a Trello card in the `TODO` list.
  - Saves the mapping (lead_id ↔ card_id) and updates `trello_card_id` in the sheet.
  - Safe to run multiple times (no duplicate cards).

- **2. Full bidirectional sync**  
  Runs the full pipeline:
  1. `initial_sync()` – create missing cards.
  2. `sync_deleted_tasks()` – if cards from the mapping no longer exist in Trello, delete those leads from Sheets.
  3. `sync_deleted_leads()` – if leads from the mapping no longer exist in Sheets, archive those cards in Trello.
  4. `sync_all_tasks_to_leads()` – propagate Trello list changes → lead statuses in Sheets.
  5. `sync_all_leads_to_tasks()` – propagate lead status changes in Sheets → Trello lists.

- **3. Bulk sync: ALL leads → tasks (Sheets → Trello)**  
  For every lead:
  - Finds its mapped Trello card (or repairs mapping from `trello_card_id`).
  - Moves the card to the list that corresponds to the lead status (e.g. `QUALIFIED` → `DONE`).

- **4. Bulk sync: ALL tasks → leads (Trello → Sheets)**  
  For every Trello card:
  - Finds its mapped lead.
  - Updates the lead's `status` field based on the card's current list.

- **5. Sync ONE lead → task (by lead ID)**  
  - Asks for a specific `lead_id`.
  - Syncs only that lead's status to its Trello card.

- **6. Sync ONE task → lead (by lead ID or card ID)**  
  - If you provide a **numeric** value:
    - Treated as `lead_id` → finds its `trello_card_id` → syncs card → lead.
  - Otherwise:
    - Treated as **Trello card ID** → syncs that card's status back to its lead.

---

## Assumptions & Limitations

- Sync is **polling-based** only (no webhooks).
- Only the **first sheet** (`sheet1`) in the spreadsheet is used.
- Trello lists must be named exactly: `TODO`, `IN_PROGRESS`, `DONE`, `LOST`.
- Conflict resolution is effectively "last sync wins" if both systems are changed between runs.
- Only **status** is synced bidirectionally; other fields (name, email, source) are only updated in the system where you edit them.

---

## Error Handling & Logging

- Logging is configured in `core/logger.py`:
  - Logs directory: `logs/`
  - Daily log file: `logs/sync_YYYYMMDD.log`
  - File handler: DEBUG and above.
  - Console handler: INFO and above.

- Typical logs include:
  - Successful connections to Google Sheets and Trello.
  - Created/updated/archived/deleted entities.
  - Warnings when a mapped lead/card is missing.
  - Errors for failed API calls or unexpected exceptions.

- Errors in processing one lead/card do **not** crash the entire bulk sync:
  - The error is logged.
  - The loop continues to the next item.

---

## AI Usage Notes

### Tools Used
- **ChatGPT (GPT-4)** - Code snippets and documentation research
- **Youtube**
---

### What I Used AI For

**1. API Documentation Research**
- Understanding Google Sheets Service Account authentication
- Learning Trello API structure (boards/lists/cards)
- Quick syntax examples for `gspread` and `py-trello`


---

**2. Small Code Snippets (~30%)**
- Regex pattern for extracting lead ID
- Logging setup with dual file/console output
- Environment variable validation boilerplate
- Set operations for detecting deleted items
- Help with sync logic

---

**3. Debugging Help**
- Understanding `ResourceUnavailable` exception in py-trello
- Fixing gspread row indexing (header row = 1, data = row 2+)

---

### Rejected/Modified suggestions

**Rejected: Generic exception handling**
- AI suggested: `except Exception as e`
- I used: Specific exceptions (`ResourceUnavailable`, `APIError`)
- **Why:** Better error messages and control flow

**Rejected: Single file structure**
- AI suggested: Everything in one file
- I used: Modular architecture (`clients/`, `core/`, `main.py`)
- **Why:** Better maintainability and separation of concerns

**Modified: Mapping strategy**
- AI suggested: Only use `mapping.json`
- I added: Also store `trello_card_id` in Google Sheets as backup
- **Why:** Provides fallback if mapping file is lost

---


### Key Takeaway

AI helped with **syntax and boilerplate**, but **design decisions, architecture, and core logic** were written by me after understanding requirements and edge cases.
---


## Project Structure

```
WorkLead/
├── clients/
│   ├── __init__.py
│   ├── lead_tracker.py          # Google Sheets client
│   └── work_tracker.py          # Trello client
├── core/
│   ├── __init__.py
│   ├── logger.py                # Logging setup
│   └── sync_logic.py            # Sync engine
├── data/
│   └── mapping.json             # Auto-generated
├── logs/
│   └── sync_YYYYMMDD.log        # Auto-generated daily logs
├── ai-notes/                    # (Optional) AI chat exports
├── main.py                      # CLI entry point
├── requirements.txt             # Dependencies
├── .env                         # Environment variables (not in Git)
├── .gitignore
├── credentials.json             # Google credentials (not in Git)
└── README.md                    # This file
```

---

## License

This project is submitted as part of a take-home assignment.

---

**Built for the Software Engineer Intern (Automation & Integrations) position**
