import sys
import types
import unittest

# Stub external dependencies so telegram_scraper can be imported without them
for name in ["pandas", "telethon", "telethon.tl.functions.channels", "telethon.tl.types", "langdetect", "dotenv"]:
    if name not in sys.modules:
        module = types.ModuleType(name)
        sys.modules[name] = module
        if name.startswith("telethon"):
            # minimal stubs used by telegram_scraper
            if name == "telethon":
                class DummyClient:
                    def __init__(self, *a, **k):
                        pass
                    async def start(self, *a, **k):
                        pass
                module.TelegramClient = DummyClient
            if name.endswith("functions.channels"):
                module.JoinChannelRequest = object
            if name.endswith("types"):
                module.PeerChannel = object
        if name == "dotenv":
            module.load_dotenv = lambda *a, **k: None
        if name == "langdetect":
            def detect(x):
                return "en"
            module.detect = detect
            module.DetectorFactory = types.SimpleNamespace(seed=0)
from telegram_scraper import _parse_subs, deduplicate

class TestUtils(unittest.TestCase):
    def test_parse_subs(self):
        self.assertEqual(_parse_subs('1k'), 1000)
        self.assertEqual(_parse_subs('2.5k'), 2500)
        self.assertEqual(_parse_subs('3m'), 3_000_000)
        self.assertEqual(_parse_subs('unknown'), 0)

    def test_deduplicate(self):
        msgs = [
            {"fwd_id": 1, "text": "hi"},
            {"fwd_id": 1, "text": "hi"},
            {"fwd_id": 2, "text": "hello"},
        ]
        self.assertEqual(len(deduplicate(msgs)), 2)

if __name__ == '__main__':
    unittest.main()
