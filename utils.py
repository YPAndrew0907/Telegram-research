
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
