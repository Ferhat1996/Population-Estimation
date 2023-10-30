import os
import pandas as pd

# Set the working directory
os.chdir(r"C:\Users\Lenovo\Desktop\Data Analytics\Lecture_2")

# Create temporary file names
temp_file = "temp"
merge_file = "MergedData"
base_file = 1  # Initialize with the first file number

# Create a list of CSV files
csv_files = [file for file in os.listdir("Output") if file.startswith("Data") and file.endswith(".csv")]

# Initialize an empty DataFrame to store merged data
merged_data = pd.DataFrame()

# Define the process_location function
def process_location(loc):
    # Import data from location
    data_path = f"Output/Data{loc}.csv"
    location_path = f"Output/Location{loc}.csv"

    data = pd.read_csv(data_path)
    location = pd.read_csv(location_path, skiprows=2, names=['county', 'diocese_jurisdiction', 'diocese_geographic', 'parish'])

    data['cced_id'] = loc
    location['cced_id'] = loc

    try:
        # Splitting "parish" column
        location['parish'] = location['parish'].astype(str)  # Convert to string
        location[['help', 'parish']] = location['parish'].str.split(":", n=1, expand=True)
        location['parish'] = location['parish'].str.strip()
    except Exception as e:
        print(f"Error processing location {loc}: {e}")

    try:
        # Ensure consistent data types for merging
        data['cced_id'] = data['cced_id'].astype(int)
        location['cced_id'] = location['cced_id'].astype(int)

        # Merge data and location information
        merged = pd.merge(data, location, on='cced_id')

        # Convert specific columns to string if they exist
        for col in ['office', 'diocese_jurisdiction', 'diocese_geographic', 'county']:
            if col in merged.columns:
                merged[col] = merged[col].astype(str)

        return merged
    except Exception as e:
        print(f"Error merging data for location {loc}: {e}")
        return None

# Loop through the CSV files
for line in csv_files:
    loc = int(line.replace("Data", "").replace(".csv", ""))
    print(loc)

    location_data = process_location(loc)
    if location_data is not None:
        merged_data = pd.concat([merged_data, location_data], ignore_index=True)

    if len(merged_data) >= 1000:
        temp_save_path = f"Temp/temp{base_file}.csv"
        merged_data.to_csv(temp_save_path, sep=';', index=False)
        print("SAVED")

        base_file += 1  # Increment the base file number
        merged_data = pd.DataFrame()

# Save the remaining merged data to a temporary CSV file
temp_save_path = f"Temp/temp{base_file}.csv"
merged_data.to_csv(temp_save_path, sep=';', index=False)

# Combine temporary files
max_val = base_file - 1
dfs = [pd.read_csv(f"Temp/temp{n}.csv", sep=';') for n in range(1, max_val + 1)]
final_merged_data = pd.concat(dfs, ignore_index=True)

# Drop variables starting with 'v'
final_merged_data = final_merged_data.drop(columns=[col for col in final_merged_data.columns if col.startswith('v')])

# Export to CSV
csv_export_path = "Final/CCEd.csv"
final_merged_data.to_csv(csv_export_path, sep=';', index=False)

print("Process complete.")

# Rest of the script for combining temporary files and exporting the final result remains unchanged

# Extract the part after ":" in the "parish" column
final_merged_data['parish_name'] = final_merged_data['parish'].str.split(":", n=1, expand=True)[1].str.strip()

# Drop the 'parish' and 'help' columns
final_merged_data.drop(columns=['parish', 'help'], inplace=True)

# Save the updated data
updated_csv_export_path = "MergedData/CCEd.csv"
final_merged_data.to_csv(updated_csv_export_path, sep=';', index=False)

print("Process complete.")
