import pandas as pd
import os

def merge_csv_files(folder_path, output_file='C:/Users/Lukasz Pindus/VS Code Python/car_price_analysis/data/merged01.csv'):
    # Get all CSV files from the folder
    #csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    csv_files = [f for f in os.listdir(folder_path) if f.startswith('all_offers_otomoto_no_vin_') and f.endswith('.csv')]
    if not csv_files:
        print("No CSV files found in the specified folder.")
        return

    # Read and concatenate all CSVs
    df_list = [pd.read_csv(os.path.join(folder_path, f)) for f in csv_files]
    merged_df = pd.concat(df_list, ignore_index=True)

    # Save to a single CSV file
    merged_df.to_csv(output_file, index=False)
    print(f"Merged {len(csv_files)} files into {output_file}")

if __name__ == '__main__':
    merge_csv_files('C:/Users/Lukasz Pindus/VS Code Python/car_price_analysis/data/merged')