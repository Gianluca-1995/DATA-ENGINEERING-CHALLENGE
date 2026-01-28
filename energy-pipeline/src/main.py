from dotenv import load_dotenv
from datetime import datetime

from extract import run_extract
from transform import run_transform


def main() -> None:
    load_dotenv()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"Start extraction Run ID: {run_id}")
    run_extract(run_id=run_id)
    print("Extraction is completed.")

    print("Start transformation")
    run_transform(run_id=run_id)
    print("Transformation is completed.")


if __name__ == "__main__":
    main()
