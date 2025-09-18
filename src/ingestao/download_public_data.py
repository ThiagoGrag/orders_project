# src/io/download_public_data.py
from pathlib import Path
import requests
from tqdm import tqdm

FILES = {
    #"order.json.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz",
    "consumer.csv.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz",
    #"restaurant.csv.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz",
}

def download_file(url: str, dest: Path, chunk_size: int = 1024*1024):
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with tqdm(total=total, unit="B", unit_scale=True, desc=f"↓ {dest.name}") as pbar:
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

def main():
    raw_dir = Path("data/raw")
    for fname, url in FILES.items():
        target = raw_dir / fname
        print(f"Baixando {fname}...")
        download_file(url, target)
        print(f"✔️ Arquivo salvo em: {target}")

if __name__ == "__main__":
    main()