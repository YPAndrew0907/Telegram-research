# Telegram Research

This repository collects messages from Telegram channels linked in `telegram_misinformation_channels_full.csv`.

## Setup

Install dependencies using pip:

```bash
pip install -r requirements.txt
```

Set the following environment variables before running the scraper:

- `API_ID`
- `API_HASH`
- `PHONE`

Optional variables allow customization of paths and crawling limits:

- `SESSION_NAME` – name for the Telethon session (default `tg_session`)
- `DATASET_PATH` – CSV file with seed channels (default `telegram_misinformation_channels_full.csv`)
- `PROGRESS_JSON` – where progress is stored (default `progress.json`)
- `RAW_CSV` – raw scraped messages (default `messages.csv`)
- `CLEAN_CSV` – cleaned output file (default `cleaned.csv`)
- `BFS_HOPS`, `BFS_PER_CH_LIMIT`, `BFS_MIN_SUBS`, `BFS_LANG_SAMPLE`, `BFS_EN_RATIO` – parameters for channel discovery
- `BFS_FWD_MIN`, `BFS_INCLUDE_MENTIONS`, `BFS_TOPIC_KEYWORDS`, `BFS_TOPIC_RATIO`, `BFS_TOPIC_SAMPLE`, `BFS_MAX_DORMANT_DAYS`, `BFS_PAUSE_SEC` – advanced crawler settings

These path variables are optional; if unset the defaults shown above are used.

The dataset of suspicious channels was manually curated and contains usernames and subscriber counts for various crypto signal groups.

## Usage

Run the scraper:

Run the provided Jupyter notebook:

```bash
jupyter notebook 'Telegram (5).ipynb'
```

The notebook discovers related channels using a breadth‑first search and saves raw messages to `RAW_CSV`. Afterwards, the cleaning step deduplicates and anonymizes messages, producing `CLEAN_CSV`.

## Testing

Lightweight tests are provided for basic utilities. Run them with:

```bash
pytest
```

