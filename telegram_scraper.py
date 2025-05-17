import os
import json
import csv
import re
import asyncio
import logging
from pathlib import Path
from urllib.parse import urlparse

try:
    import pandas as pd
except ModuleNotFoundError:  # allow limited functionality in tests
    pd = None
from langdetect import detect, DetectorFactory
from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import PeerChannel
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")

if not all([API_ID, API_HASH, PHONE]):
    raise RuntimeError("API_ID, API_HASH and PHONE environment variables must be set")

SESSION_NAME = os.getenv("SESSION_NAME", "tg_session")
DATASET_PATH = os.getenv("DATASET_PATH", "telegram_misinformation_channels_full.csv")
PROGRESS_JSON = os.getenv("PROGRESS_JSON", "progress.json")
RAW_CSV = os.getenv("RAW_CSV", "messages.csv")
CLEAN_CSV = os.getenv("CLEAN_CSV", "cleaned.csv")

BFS_HOPS = int(os.getenv("BFS_HOPS", 2))
BFS_PER_CH_LIMIT = int(os.getenv("BFS_PER_CH_LIMIT", 400))
BFS_MIN_SUBS = int(os.getenv("BFS_MIN_SUBS", 500))
BFS_LANG_SAMPLE = int(os.getenv("BFS_LANG_SAMPLE", 15))
BFS_EN_RATIO = float(os.getenv("BFS_EN_RATIO", 0.5))

DetectorFactory.seed = 0


def _parse_subs(s: str) -> int:
    s = str(s).replace("+", "").strip()
    mult = 1
    if s.lower().endswith("k"):
        mult = 1000
        s = s[:-1]
    elif s.lower().endswith("m"):
        mult = 1000000
        s = s[:-1]
    try:
        return int(float(s) * mult)
    except ValueError:
        return 0

# Allow tests to import this module without requiring heavy dependencies
_SKIP_DATASET = os.getenv("TS_SKIP_DATASET") == "1" or pd is None
if not _SKIP_DATASET:
    channels_df = pd.read_csv(DATASET_PATH)
    channels_df["subs_num"] = channels_df["Subscribers"].apply(_parse_subs)
    seed_usernames = channels_df["Username"].tolist()
    FOLLOWERS_DICT = channels_df.set_index("Username")["subs_num"].to_dict()
else:
    channels_df = None
    seed_usernames = []
    FOLLOWERS_DICT = {}

CHANNELS = seed_usernames
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)


def load_progress() -> dict:
    if Path(PROGRESS_JSON).exists():
        with open(PROGRESS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_progress(d: dict) -> None:
    Path(PROGRESS_JSON).parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_JSON, "w", encoding="utf-8") as f:
        json.dump(d, f)
    logger.info("Progress saved")


def append_header_if_needed() -> None:
    path = Path(RAW_CSV)
    exist = path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not exist:
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["channel", "msg_id", "date", "text", "fwd_from", "fwd_id"])


def join_if_needed(ent):
    if getattr(ent, "megagroup", False):
        try:
            return client(JoinChannelRequest(ent))
        except Exception:
            pass


async def scrape_channel(name: str, last_id: int) -> int:
    try:
        ent = await client.get_entity(name)
    except Exception as e:
        logger.warning("Skip %s: %s", name, e)
        return last_id

    await join_if_needed(ent)
    mx = last_id
    cnt = 0
    async for msg in client.iter_messages(ent, min_id=last_id, reverse=False):
        cnt += 1
        mx = max(mx, msg.id)
        fw, fwid = None, None
        if msg.forward:
            if msg.forward.chat:
                fw = msg.forward.chat.title or msg.forward.chat.username
                fwid = getattr(msg.forward.chat, "id", None)
            elif msg.forward.sender:
                fw = msg.forward.sender.first_name or msg.forward.sender.username or "Private"
                fwid = getattr(msg.forward.sender, "id", None)
        with open(RAW_CSV, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([name, msg.id, msg.date, msg.text or "", fw, fwid])
    logger.info("%s +%d msgs up to %s", name, cnt, mx)
    return mx


def run_scrape():
    asyncio.run(scrape_once())
    logger.info("Scraping done")


def deduplicate(messages):
    out = []
    seen = set()
    for m in messages:
        k = (m.get("fwd_id"), m.get("text"))
        if m.get("fwd_id"):
            if k in seen:
                continue
            seen.add(k)
        out.append(m)
    return out


def unshorten_url(u, t=5):
    import requests
    try:
        return requests.head(u, allow_redirects=True, timeout=t).url
    except Exception:
        return u


def expand_and_extract(m):
    txt = m["text"]
    urls = re.findall(r"https?://\S+", txt)
    dom = set()
    for u in urls:
        fin = unshorten_url(u)
        d = urlparse(fin).netloc
        dom.add(d)
        txt = txt.replace(u, fin)
    m["text"] = txt
    m["domains"] = list(dom)
    return m


def is_english(txt):
    if not txt.strip():
        return False
    try:
        return detect(txt) == "en"
    except Exception:
        return False


def anonymize(m, user_map):
    if m.get("fwd_id") and m["fwd_from"] and not str(m["fwd_from"]).startswith(("Channel", "@")):
        if m["fwd_id"] not in user_map:
            user_map[m["fwd_id"]] = f"User{len(user_map)+1}"
        m["fwd_from"] = user_map[m["fwd_id"]]
        m["fwd_id"] = None
    m["text"] = re.sub(r"@[\w\d_]+", "@USER", m["text"])
    return m


def preprocess_data():
    if not Path(RAW_CSV).exists():
        logger.warning("No raw CSV to preprocess")
        return
    df = pd.read_csv(RAW_CSV, names=["chan", "msg_id", "date", "text", "fwd_from", "fwd_id"], header=0)
    msgs = df.to_dict("records")
    msgs = deduplicate(msgs)
    out = []
    umap = {}
    for m in msgs:
        m = expand_and_extract(m)
        if is_english(m["text"]):
            m = anonymize(m, umap)
            out.append(m)
    pd.DataFrame(out).to_csv(CLEAN_CSV, index=False)
    logger.info("Saved %d cleaned msgs to %s", len(out), CLEAN_CSV)


async def _en_ratio(ent, sample=BFS_LANG_SAMPLE):
    en = 0
    async for m in client.iter_messages(ent, limit=sample):
        t = (m.message or "").strip()
        if t:
            try:
                en += detect(t) == "en"
            except Exception:
                pass
    return 0 if not sample else en / sample


async def _forward_sources(ent, limit=BFS_PER_CH_LIMIT):
    src = set()
    async for m in client.iter_messages(ent, limit=limit):
        fwd = m.forward
        if not fwd:
            continue
        pc = fwd.chat or fwd.original_fwd
        if isinstance(pc, PeerChannel):
            src.add(pc.channel_id)
    return src


async def _is_valid(ent) -> bool:
    username = (getattr(ent, "username", "") or "").strip()
    count = FOLLOWERS_DICT.get(username, 0)
    if count < BFS_MIN_SUBS:
        return False
    try:
        return (await _en_ratio(ent)) >= BFS_EN_RATIO
    except Exception:
        return False


async def snowball_discovery():
    global CHANNELS
    prog, seeds = load_progress(), [c.lstrip("@") for c in CHANNELS]
    frontier, visited, validated = set(seeds), set(seeds), []
    for hop in range(1, BFS_HOPS + 1):
        nxt = set()
        logger.info("hop %d frontier=%d", hop, len(frontier))
        for h in list(frontier):
            try:
                ent = await client.get_entity(h)
            except Exception:
                continue
            if not await _is_valid(ent):
                continue
            validated.append(h)
            for cid in await _forward_sources(ent):
                try:
                    e = await client.get_entity(PeerChannel(cid))
                    if e.username and e.username not in visited:
                        visited.add(e.username)
                        nxt.add(e.username)
                except Exception:
                    pass
        frontier = nxt
    for v in validated:
        if v not in prog:
            prog[v] = 0
    save_progress(prog)
    CHANNELS = list(set(CHANNELS) | set(prog))
    logger.info("validated %d | total CHANNELS %d", len(validated), len(CHANNELS))


async def scrape_once():
    await client.start(phone=PHONE)
    await snowball_discovery()
    last = load_progress()
    append_header_if_needed()
    for c in CHANNELS:
        last[c] = await scrape_channel(c, last.get(c, 0))
    save_progress(last)
    await client.disconnect()


def main():
    run_scrape()
    preprocess_data()
    logger.info("Done")


if __name__ == "__main__":
    main()
