from pathlib import Path
import hashlib
import re


def make_match_id(link):
    # check if link matches expected pattern and extract teams and date
    pattern = r'/matches/([a-z]+)vs([a-z]+)-(\d{2}-\d{2}-\d{4})'
    m = re.search(pattern, link)
    if m:
        team1, team2, date = m.groups()
        return hashlib.md5(f"{team1}vs{team2}-{date}".encode('utf-8')).hexdigest()[:8]
    # fallback: hash the last part of the URL (after last slash, before query)
    return hashlib.md5(link.split('/')[-2].encode('utf-8')).hexdigest()[:8]
    
def save_to_csv(df, filename):
    path = Path("data/github_actions") / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)