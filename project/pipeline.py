import os
import pandas as pd
import urllib.request
import zipfile
import sqlite3
import kagglehub

# Define processed data directory
processed_data_dir = "../data/processed/"
faostat_cleaned_path = os.path.join(processed_data_dir, "FAOSTAT_cleaned.csv")
amazon_fires_cleaned_path = os.path.join(processed_data_dir, "Amazon_Fires_cleaned.csv")
merged_data_path = os.path.join(processed_data_dir, "Merged_Data.csv")
database_path = os.path.join(processed_data_dir, "data_pipeline.db")

# Ensure processed data directory exists
os.makedirs(processed_data_dir, exist_ok=True)

# Step 1: Download and process FAOSTAT Emissions data
print("Downloading FAOSTAT data...")
faostat_url = "https://bulks-faostat.fao.org/production/Emissions_Totals_E_Americas.zip"
faostat_zip_path = "Emissions_Totals_E_Americas.zip"

# Download the ZIP file
try:
    urllib.request.urlretrieve(faostat_url, faostat_zip_path)
    print("FAOSTAT data downloaded.")
except Exception as e:
    print(f"Error downloading FAOSTAT data: {e}")
    exit()

# Extract the ZIP file
try:
    with zipfile.ZipFile(faostat_zip_path, 'r') as zip_ref:
        zip_ref.extractall(".")
    print("FAOSTAT data extracted.")
except Exception as e:
    print(f"Error extracting FAOSTAT data: {e}")
    exit()

# Identify the main FAOSTAT file
faostat_main_file = "Emissions_Totals_E_Americas.csv"
if not os.path.exists(faostat_main_file):
    print(f"Error: Main file '{faostat_main_file}' not found.")
    exit()

# Load the FAOSTAT data
faostat_data = pd.read_csv(faostat_main_file)

# Filter the data for Brazil
filtered_data = faostat_data[faostat_data["Area"] == "Brazil"]

# Filter for CO2 emissions
filtered_data = filtered_data[filtered_data["Element"] == "Emissions (CO2)"]

# Filter for FAO TIER 1 Source
filtered_data = filtered_data[filtered_data["Source"] == "FAO TIER 1"]

# Reshape and filter for years 1999 to 2019
year_columns = [col for col in filtered_data.columns if col.startswith('Y') and col[1:].isdigit()]
filtered_data = filtered_data.melt(id_vars=["Area"], 
                                   value_vars=year_columns, 
                                   var_name="Year", 
                                   value_name="CO2 Emissions")  # Rename column here
filtered_data["Year"] = filtered_data["Year"].str[1:].astype(int)
filtered_data = filtered_data[(filtered_data["Year"] >= 1999) & (filtered_data["Year"] <= 2019)]

# Calculate total CO2 emissions per year
total_emissions = filtered_data.groupby("Year", as_index=False)["CO2 Emissions"].sum()

# Save the total emissions data to a CSV file
total_emissions.to_csv(faostat_cleaned_path, index=False)
print(f"Total CO2 emissions saved to: {faostat_cleaned_path}")

# Step 2: Download and process Amazon Fires dataset
print("Downloading Amazon Fires dataset from Kaggle...")
try:
    path = kagglehub.dataset_download("mbogernetto/brazilian-amazon-rainforest-degradation")
    amazon_fires_path = os.path.join(path, "inpe_brazilian_amazon_fires_1999_2019.csv")
    if os.path.exists(amazon_fires_path):
        amazon_fires_data = pd.read_csv(amazon_fires_path)
        
        # Aggregate fires data by year
        amazon_fires_aggregated = amazon_fires_data.groupby("year", as_index=False)["firespots"].sum()
        amazon_fires_aggregated = amazon_fires_aggregated[(amazon_fires_aggregated["year"] >= 1999) & (amazon_fires_aggregated["year"] <= 2019)]
        
        # Save the aggregated Amazon Fires data
        amazon_fires_aggregated.to_csv(amazon_fires_cleaned_path, index=False)
        print(f"Aggregated Amazon Fires data saved to: {amazon_fires_cleaned_path}")
    else:
        print(f"Error: File '{amazon_fires_path}' not found.")
except Exception as e:
    print(f"Error downloading Amazon Fires dataset: {e}")
    exit()

# Step 3: Merge FAOSTAT and Amazon Fires datasets
print("Merging datasets...")
faostat_data = pd.read_csv(faostat_cleaned_path)
amazon_fires_data = pd.read_csv(amazon_fires_cleaned_path)

# Merge datasets on 'Year'
merged_data = pd.merge(faostat_data, amazon_fires_data, left_on="Year", right_on="year", how="inner")

# Drop duplicate 'year' column
merged_data.drop(columns=["year"], inplace=True)

# Save the merged dataset to CSV
merged_data.to_csv(merged_data_path, index=False)
print(f"Merged data saved to: {merged_data_path}")

# Save the merged dataset to SQLite database
conn = sqlite3.connect(database_path)
merged_data.to_sql("MergedData", conn, if_exists="replace", index=False)
print(f"Merged data saved to SQLite database: {database_path}")
conn.close()

# Cleanup: remove temporary files
temp_files = [
    faostat_zip_path,
    "Emissions_Totals_E_Americas.csv",
    "Emissions_Totals_E_Americas_NOFLAG.csv",
    "Emissions_Totals_E_AreaCodes.csv",
    "Emissions_Totals_E_Elements.csv",
    "Emissions_Totals_E_Flags.csv",
    "Emissions_Totals_E_ItemCodes.csv",
    "Emissions_Totals_E_Sources.csv"
]

for file in temp_files:
    if os.path.exists(file):
        os.remove(file)

print("Data pipeline completed successfully, and temporary files cleaned up.")