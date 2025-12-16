import kagglehub
import shutil
from pathlib import Path

RAW_DIR = Path("/data/raw")

def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    
    downloaded_dir = kagglehub.dataset_download(
        "karkavelrajaj/amazon-sales-dataset",
        force_download=True
    )

    source_dir = Path(downloaded_dir)

    if not source_dir.exists() or not source_dir.is_dir():
        raise FileNotFoundError("Kaggle download failed")

    files = [f for f in source_dir.iterdir() if f.is_file()]

    if len(files) != 1:
        raise RuntimeError("Expected exactly one file in dataset")

    source_file = files[0]

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    target_file = RAW_DIR / f"amazon_sales_raw{source_file.suffix}"

    shutil.move(str(source_file), str(target_file))

    print(f"Bronze layer completed. File saved to: {target_file}")

if __name__ == "__main__":
    main()
