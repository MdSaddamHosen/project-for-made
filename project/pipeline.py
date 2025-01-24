import os
import pandas as pd
import urllib.request
import zipfile
import sqlite3
import kagglehub
import logging

# Configure logging
logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define processed data directory
processed_data_dir = "../data/processed/"
merged_data_path = os.path.join(processed_data_dir, "Merged_Data.csv")
database_path = os.path.join(processed_data_dir, "data_pipeline.db")

# Ensure processed data directory exists
os.makedirs(processed_data_dir, exist_ok=True)

try:
    # Step 1: Download and process FAOSTAT Emissions data
    print("Downloading FAOSTAT data...")
    logging.info("Downloading FAOSTAT data...")
    faostat_url = "https://bulks-faostat.fao.org/production/Emissions_Totals_E_Americas.zip"
    faostat_zip_path = "Emissions_Totals_E_Americas.zip"

    urllib.request.urlretrieve(faostat_url, faostat_zip_path)
    print("FAOSTAT data downloaded.")
    logging.info("FAOSTAT data downloaded.")

    # Extract the ZIP file
    with zipfile.ZipFile(faostat_zip_path, 'r') as zip_ref:
        zip_ref.extractall(".")
    print("FAOSTAT data extracted.")
    logging.info("FAOSTAT data extracted.")

    # Load the FAOSTAT data
    faostat_main_file = "Emissions_Totals_E_Americas.csv"
    faostat_data = pd.read_csv(faostat_main_file)

    # Define valid elements, items, and sources
    valid_elements = [
        "Emissions (CO2)",
        "Emissions (CO2eq) (AR5)",
        "Emissions (CO2eq) from CH4 (AR5)",
        "Emissions (CO2eq) from F-gases (AR5)",
        "Emissions (CO2eq) from N2O (AR5)"
    ]
    valid_items = [
        "Net Forest conversion",
        "Forest fires",
        "Drained organic soils",
        "Savanna fires",
        "Burning - Crop residues"
    ]
    valid_sources = ["FAO TIER 1", "UNFCCC"]

    # Filter the data for Brazil, CO2-related emissions, and valid sources
    filtered_data = faostat_data[
        (faostat_data["Area"] == "Brazil") &
        (faostat_data["Element"].isin(valid_elements)) &
        (faostat_data["Source"].isin(valid_sources)) &
        (faostat_data["Item"].isin(valid_items))
    ]

    # Reshape and filter for years 1999 to 2019
    year_columns = [col for col in filtered_data.columns if col.startswith('Y') and col[1:].isdigit()]
    filtered_data = filtered_data.melt(
        id_vars=["Item", "Element", "Source"],
        value_vars=year_columns,
        var_name="Year",
        value_name="Emissions"
    )
    filtered_data["Year"] = filtered_data["Year"].str[1:].astype(int)
    filtered_data = filtered_data[(filtered_data["Year"] >= 1999) & (filtered_data["Year"] <= 2019)]

    # Drop missing or zero values
    filtered_data = filtered_data.dropna(subset=["Emissions"])
    filtered_data = filtered_data[filtered_data["Emissions"] > 0]

    # Rename columns for clarity
    filtered_data.rename(columns={"Item": "Reasons of Emission"}, inplace=True)

    print("FAOSTAT and UNFCCC data processed.")
    logging.info("FAOSTAT and UNFCCC data processed.")

except Exception as e:
    logging.error(f"Error processing FAOSTAT data: {e}")
    exit()

try:
    # Step 2: Download and process Amazon Fires dataset
    print("Downloading Amazon Fires dataset from Kaggle...")
    logging.info("Downloading Amazon Fires dataset from Kaggle...")
    path = kagglehub.dataset_download("mbogernetto/brazilian-amazon-rainforest-degradation")
    amazon_fires_path = os.path.join(path, "inpe_brazilian_amazon_fires_1999_2019.csv")

    if os.path.exists(amazon_fires_path):
        amazon_fires_data = pd.read_csv(amazon_fires_path)

        # Keep relevant columns
        amazon_fires_data = amazon_fires_data[["year", "firespots"]]

        # Aggregate firespots by year
        amazon_fires_data = amazon_fires_data.groupby("year", as_index=False)["firespots"].sum()

        # Filter for years 1999 to 2019
        amazon_fires_data = amazon_fires_data[(amazon_fires_data["year"] >= 1999) & (amazon_fires_data["year"] <= 2019)]

        print("Amazon Fires data processed.")
        logging.info("Amazon Fires data processed.")
    else:
        raise FileNotFoundError(f"File '{amazon_fires_path}' not found.")

except Exception as e:
    logging.error(f"Error processing Amazon Fires dataset: {e}")
    exit()

try:
    # Step 3: Align firespot data with emissions data proportionally
    print("Aligning firespot data with emissions data proportionally...")
    logging.info("Aligning firespot data with emissions data proportionally...")

    # Merge emissions data with firespots for alignment
    total_emissions_per_year = filtered_data.groupby("Year")["Emissions"].sum().reset_index()
    firespot_distribution = pd.merge(
        amazon_fires_data,
        total_emissions_per_year,
        left_on="year",
        right_on="Year",
        how="inner"
    )

    # Distribute firespots proportionally
    def distribute_firespots(row):
        year_emissions = filtered_data[filtered_data["Year"] == row["Year"]]
        year_emissions["firespots"] = (
            year_emissions["Emissions"] / row["Emissions"] * row["firespots"]
        )
        return year_emissions

    aligned_data = pd.concat(
        firespot_distribution.apply(distribute_firespots, axis=1).to_list(),
        ignore_index=True
    )

    print("Firespot data aligned proportionally with emissions data.")
    logging.info("Firespot data aligned proportionally with emissions data.")

    # Save the merged dataset to CSV and SQLite database
    aligned_data.to_csv(merged_data_path, index=False)
    print(f"Merged data saved to: {merged_data_path}")
    logging.info(f"Merged data saved to: {merged_data_path}")

    conn = sqlite3.connect(database_path)
    aligned_data.to_sql("MergedData", conn, if_exists="replace", index=False)
    print(f"Merged data saved to SQLite database: {database_path}")
    logging.info(f"Merged data saved to SQLite database: {database_path}")
    conn.close()

except Exception as e:
    logging.error(f"Error aligning firespot data with emissions data: {e}")
    exit()

finally:
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
    print("Temporary files cleaned up.")
    logging.info("Temporary files cleaned up.")
