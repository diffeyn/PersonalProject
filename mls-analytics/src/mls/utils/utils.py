from pathlib import Path

    
def save_to_csv(df, filename):
    path = Path("data/github_actions") / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)