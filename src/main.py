from pathlib import Path
import polars as pl
import logging as log
import os


def list_tsv_files_in_dir(dir: str) -> list[str]:
    files = []
    try:
        files = os.listdir(dir)
        files = [
            file
            for file in files
            if os.path.isfile(os.path.join(dir, file))
            and os.path.join(dir, file).endswith(".tsv")
        ]
    except FileNotFoundError as err:
        log.error(f"{err}")
        files = []
    except NotADirectoryError as err:
        log.error(f"{err}")
        files = []
    finally:
        return files


def tsv_to_parquet(
    tsv_file: str, parquet_filename: str | None = None, delete_csv: bool = False
):
    pass


def main():
    dataset_src_directory = Path(os.path.join(str(Path.cwd()), "datasets"))
    dataset_dst_directory = Path(os.path.join(str(Path.cwd()), "datasets"))
    tsv_files = list_tsv_files_in_dir(str(dataset_src_directory))
    for file in tsv_files:
        tsv_file = Path(os.path.join(dataset_dst_directory, file))
        filename_without_extension = tsv_file.stem
        pl.read_csv(
            tsv_file,
            has_header=True,
            separator="\t",
            null_values="\\N",
            infer_schema_length=10000,
            quote_char=None,
        ).write_parquet(
            str(dataset_dst_directory) + filename_without_extension + ".parquet"
        )
        tsv_file.unlink()


if __name__ == "__main__":
    main()
