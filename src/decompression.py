import gzip
import shutil
from pathlib import Path
import os
import polars as pl

from env import appEnv

IN_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", appEnv.IN_DIR))
OUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", appEnv.OUT_DIR))
MID_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", appEnv.MID_DIR))


def decompress(in_dir: str, out_dir: str, mid_dir: str) -> None:
    if not Path(out_dir).exists:
        Path(out_dir).mkdir(parents=True, exist_ok=True)
    mid_dir = Path(mid_dir)
    out_dir = Path(out_dir)
    in_dir = Path(in_dir)

    for gz_file in Path(in_dir).glob("*.gz"):
        print(f"Processing {gz_file.name}...")

        temp_tsv = mid_dir / gz_file.stem

        try:
            with gzip.open(gz_file, "rb") as f_in:
                with open(temp_tsv, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            df = pl.read_csv(
                temp_tsv,
                separator="\t",
                null_values="\\N",
                quote_char=None,
                infer_schema_length=10000,
            )

            pq_file = out_dir / (temp_tsv.stem + ".parquet")
            df.write_parquet(pq_file)
            print(f"Saved to {pq_file}")
        finally:
            if temp_tsv.exists:
                temp_tsv.unlink()
                print(f"Deleted temporary file: {temp_tsv}")


if __name__ == "__main__":
    decompress(IN_DIR, OUT_DIR, appEnv.MID_DIR)
