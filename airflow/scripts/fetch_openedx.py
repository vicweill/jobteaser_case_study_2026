import pandas as pd
import requests
import os
import time

API_TOKEN_ENV = os.getenv("OPENEDX_API_TOKEN", "DEMO")
INPUT_PATH_ENV = os.getenv("INPUT_CSV_PATH", "data/completed_blocks.csv")
OUTPUT_PATH_ENV = os.getenv("OUTPUT_CSV_PATH", "data/ref_block_names.csv")

def fetch_block_names(input_path, output_path, api_token=None):
    """
    Reads blocks ID, get names from OpenEDX API and store them.
    """

    if not os.path.exists(input_path):
        print(f"Erreur : Le fichier {input_path} est introuvable.")
        return

    # I will use a DataFrame for this example but we should query a dbt model
    df_raw = pd.read_csv(input_path)
    block_ids = df_raw['block_id'].unique()

    results = []

    print(f"Getting {len(block_ids)} block names...")

    for block_id in block_ids:
        if not API_TOKEN_ENV or API_TOKEN_ENV == "3X4MPL3":
            display_name = f"Fake name for this case study"
        else:
            # Call OpenEDX API
            url = f"https://courses.edx.org/api/courses/v1/blocks/{block_id}"
            headers = {"Authorization": f"Bearer {API_TOKEN_ENV}"}
            try:
                res = requests.get(url, headers=headers, timeout=5)
                if res.status_code == 200:
                    display_name = res.json().get('display_name', 'unknown name')
                else:
                    display_name = f"API Error {res.status_code}"
            except Exception as e:
                print(f"Fail for {block_id}: {e}")
                display_name = "Connection error"
        # Avoid API overload
        time.sleep(0.05)

        results.append({"block_id": block_id, "block_name": display_name})

    df_res = pd.DataFrame(results)
    df_res.to_csv(output_path, index=False)
    print(f"Got {len(df_res)} names, and stored data here : {output_path}")

# For manual run
if __name__ == "__main__":
    fetch_block_names(
        input_path=INPUT_PATH_ENV,
        output_path=OUTPUT_PATH_ENV,
        api_token=API_TOKEN_ENV
    )
