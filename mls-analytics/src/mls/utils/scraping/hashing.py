import re
import hashlib
from urllib.parse import urlparse

def make_match_id(link):
    # normalize
    parsed = urlparse(link)
    path = parsed.path

    # 1️⃣ Try match pattern
    pattern = r'/matches/([a-z0-9-]+)vs([a-z0-9-]+)-(\d{2}-\d{2}-\d{4})'
    m = re.search(pattern, path)

    if m:
        team1, team2, date = m.groups()
        base = f"{team1}vs{team2}-{date}"
        return hashlib.md5(base.encode('utf-8')).hexdigest()[:8]

    parts = [p for p in path.split('/') if p]

    if parts:
        fallback = parts[-1]
    else:
        fallback = link
    return hashlib.md5(fallback.encode('utf-8')).hexdigest()[:8]