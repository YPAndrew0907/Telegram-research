import os
import json
import csv
import asyncio
import re
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
try:
    import requests
except Exception:  # pragma: no cover - optional dependency for tests
    requests = None
from dotenv import load_dotenv
from langdetect import detect, DetectorFactory
from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import PeerChannel

load_dotenv()
DetectorFactory.seed = 0

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")

SESSION_NAME = os.getenv("SESSION_NAME", "tg_session")
DATASET_PATH = Path(os.getenv("DATASET_PATH", "telegram_misinformation_channels_full.csv"))

WORKDIR = Path(os.getenv("TG_WORKDIR", Path.cwd() / "telegram_data"))
WORKDIR.mkdir(parents=True, exist_ok=True)
PROGRESS_JSON = Path(os.getenv("PROGRESS_JSON", WORKDIR / "progress.json"))
RAW_CSV = Path(os.getenv("RAW_CSV", WORKDIR / "messages.csv"))
CLEAN_CSV = Path(os.getenv("CLEAN_CSV", WORKDIR / "cleaned.csv"))

BFS_HOPS = int(os.getenv("BFS_HOPS", 2))
BFS_PER_CH_LIMIT = int(os.getenv("BFS_PER_CH_LIMIT", 400))
BFS_MIN_SUBS = int(os.getenv("BFS_MIN_SUBS", 500))
BFS_LANG_SAMPLE = int(os.getenv("BFS_LANG_SAMPLE", 15))
BFS_EN_RATIO = float(os.getenv("BFS_EN_RATIO", 0.5))

def _parse_subs(s: str) -> int:
    s = str(s).replace("+", "").strip()
    mult = 1
    if s.lower().endswith("k"):
        mult = 1000
        s = s[:-1]
    elif s.lower().endswith("m"):
        mult = 1_000_000
        s = s[:-1]
    try:
        return int(float(s) * mult)
    except ValueError:
        return 0

if hasattr(pd, "read_csv"):
    channels_df = pd.read_csv(DATASET_PATH)
    channels_df["subs_num"] = channels_df["Subscribers"].apply(_parse_subs)
    seed_usernames = channels_df["Username"].tolist()
    FOLLOWERS_DICT = channels_df.set_index("Username")["subs_num"].to_dict()
else:  # fallback during tests when pandas is stubbed
    channels_df = None
    seed_usernames = []
    FOLLOWERS_DICT = {}

CHANNELS = seed_usernames.copy()

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)


def load_progress() -> dict:
    if PROGRESS_JSON.exists():
        with open(PROGRESS_JSON, "r") as f:
            return json.load(f)
    return {}


def save_progress(d: dict) -> None:
    PROGRESS_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_JSON, "w") as f:
        json.dump(d, f)
    print("Progress saved.")


def append_header_if_needed() -> None:
    existed = RAW_CSV.exists()
    RAW_CSV.parent.mkdir(parents=True, exist_ok=True)
    if not existed:
        with open(RAW_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "channel",
                "msg_id",
                "date",
                "text",
                "fwd_from",
                "fwd_id",
            ])


async def join_if_needed(ent):
    if getattr(ent, "megagroup", False):
        try:
            await client(JoinChannelRequest(ent))
        except Exception:
            pass


async def scrape_channel(name, last_id):
    try:
        ent = await client.get_entity(name)
    except Exception as e:
        print("Skip", name, e)
        return last_id
    await join_if_needed(ent)
    mx = last_id
    cnt = 0
    async for msg in client.iter_messages(ent, min_id=last_id, reverse=False):
        cnt += 1
        mx = max(mx, msg.id)
        fw = None
        fwid = None
        if msg.forward:
            if msg.forward.chat:
                fw = msg.forward.chat.title or msg.forward.chat.username
                fwid = getattr(msg.forward.chat, "id", None)
            elif msg.forward.sender:
                fw = msg.forward.sender.first_name or msg.forward.sender.username or "Private"
                fwid = getattr(msg.forward.sender, "id", None)
        with open(RAW_CSV, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([name, msg.id, msg.date, msg.text or "", fw, fwid])
    print(name, f"+{cnt} msgs", "up to", mx)
    return mx


def run_scrape():
    asyncio.run(scrape_once())
    print("Scraping done.")


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
    if requests is None:
        return u
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


def is_english(txt: str) -> bool:
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
    if not RAW_CSV.exists():
        print("No raw CSV.")
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
    print(f"Saved {len(out)} cleaned msgs to {CLEAN_CSV}")


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


async def _is_valid(ent):
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
    prog = load_progress()
    seeds = [c.lstrip("@") for c in CHANNELS]
    frontier = set(seeds)
    visited = set(seeds)
    validated = []
    for hop in range(1, BFS_HOPS + 1):
        nxt = set()
        print(f"· hop {hop}  frontier={len(frontier)}")
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
    print("validated:", len(validated), "| total CHANNELS:", len(CHANNELS))


async def scrape_once():
    if not all([API_ID, API_HASH, PHONE]):
        raise RuntimeError("API_ID, API_HASH and PHONE environment variables must be set")
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
    print("Done.")


if __name__ == "__main__":
    main()
