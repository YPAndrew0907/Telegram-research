# AGENTS.md

This document provides guidelines for **AI agents** contributing to the **Telegram Research** project. It outlines how to make code changes (exclusively via Jupyter notebooks), the project structure, environment setup, coding conventions, testing requirements, and forbidden actions. By following these rules, agents ensure their contributions are compatible with the project’s workflow and standards.

---

## Table of Contents
1. [Repository Structure](#repository-structure)  
2. [Environment Setup](#environment-setup)  
3. [Development Guidelines](#development-guidelines)  
4. [Testing and Validation](#testing-and-validation)  
5. [Forbidden Actions](#forbidden-actions)  
6. [Acceptance Criteria & Review](#acceptance-criteria--review)  

---

## Repository Structure

- **Main Notebook (Root)**  
  The primary Jupyter notebook **Telegram (5).ipynb** resides at the repository root. It implements a breadth-first search (BFS) to discover related Telegram channels and collect their messages, saving raw messages into a CSV (default: `messages.csv`) and then deduplicating/anonymizing them into a cleaned CSV (default: `cleaned.csv`).  
  - **All code changes should be made in notebooks like this** rather than standalone scripts.

- **Data Files (Root)**  
  The file **telegram_misinformation_channels_full.csv** in the repository root is the seed dataset of suspicious Telegram channels. **Agents must not modify** this file’s contents unless explicitly instructed—it serves as the primary input for the scraper.

- **Utility Module**  
  - **utils.py** (in the root) provides helper functions (e.g., parsing subscriber counts, deduplicating messages).  
  - Corresponding tests exist in the `tests/` directory.  
  - **Agents should avoid editing this file** unless a change is explicitly approved—new logic belongs in notebooks, not in new Python modules.

- **Tests Directory**  
  - **tests/** contains unit tests (e.g., `tests/test_utils.py`) for core utilities like parsing and deduplication.  
  - Agents can run these tests to ensure nothing breaks, but adding or modifying test files is discouraged unless instructed.

- **Output Data Directory**  
  - **telegram_data/** is a working directory (created at runtime) for scraper outputs: `progress.json` for crawl state, etc.  
  - **Do not commit** files from this directory to the repository—it stores temporary data for runs.

- **Configuration & Environment**  
  - The project uses **environment variables** (loaded via `python-dotenv`) rather than a traditional config file.  
  - Key environment variables include:
    - `API_ID`, `API_HASH`, `PHONE` – Telegram API credentials.
    - `SESSION_NAME`, `DATASET_PATH`, `PROGRESS_JSON`, `RAW_CSV`, `CLEAN_CSV` – Paths for the Telethon session file and I/O files.  
    - BFS parameters (e.g., `BFS_HOPS`, `BFS_PER_CH_LIMIT`, `BFS_MIN_SUBS`) – controlling depth, messages scanned per channel, subscriber thresholds, etc.
  - **Agents must not hardcode** these values; use the environment variables or existing constants in the notebook.

---

## Environment Setup

1. **Install Dependencies**  
   Make sure you have Python 3. Then install packages from `requirements.txt`:
   ```bash
   pip install -r requirements.txt
