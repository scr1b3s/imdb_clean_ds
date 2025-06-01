import gzip
import shutil
from pathlib import Path
import polars as pl

IN_FOLDER = "./datasets"
OUT_FOLDER = "./raw"

def decompress(in_folder, out_folder):
    if not Path(OUT_FOLDER).exists:
        Path(out_folder).mkdir(parents=True, exist_ok=True)
    out_folder = Path(out_folder)
    in_folder = Path(in_folder)
    
    for gz_file in Path(in_folder).glob("*.gz"):
        print(f"Processing {gz_file.name}...")

        temp_tsv = out_folder / gz_file.stem

        try:
            with gzip.open(gz_file, 'rb') as f_in:
                with open(temp_tsv, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            df = pl.read_csv(
                temp_tsv,
                separator="\t",
                null_values="\\N",
                quote_char=None,
                infer_schema_length=10000
            )

            pq_file = out_folder / (temp_tsv.stem + ".parquet")
            df.write_parquet(pq_file)
            print(f"Saved to {pq_file}")
        finally:
            if temp_tsv.exists:
                temp_tsv.unlink()
                print(f"Deleted temporary file: {temp_tsv}")

if __name__ == '__main__':
    decompress(IN_FOLDER, OUT_FOLDER)