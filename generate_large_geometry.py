import csv
import json
import random
from datetime import datetime, timedelta


def generate_fake_csv(file_name: str, num_rows: int) -> None:
    """Generates a CSV file with fake data.

    Args:
        file_name (str): The name of the output CSV file.
        num_rows (int): The number of rows to generate.
    """
    with open(file_name, "w", newline="") as csvfile:
        fieldnames = ["date", "value", "geometry"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for _ in range(num_rows):
            start_date = datetime.now() - timedelta(days=3650)
            random_date = start_date + timedelta(days=random.randint(0, 3650))
            date_str = random_date.strftime("%Y-%m-%d")

            value = round(random.uniform(0.0, 1000.0), 2)

            longitude = round(random.uniform(-180.0, 180.0), 6)
            latitude = round(random.uniform(-90.0, 90.0), 6)
            geometry = {"type": "Point", "coordinates": [longitude, latitude]}

            writer.writerow(
                {"date": date_str, "value": value, "geometry": json.dumps(geometry)}
            )


if __name__ == "__main__":
    rows = int(input("number of rows"))
    path = input("file name")
    import os

    if os.path.exists(path):
        raise Exception(f"{path} already exists")
    # Example usage
    generate_fake_csv(path, rows)
