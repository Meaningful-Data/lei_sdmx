"""
LEI to SDMX Pipeline

This module provides functionality to transform Legal Entity Identifier (LEI) data into SDMX format,
validate it structurally, and run VTL (Validation and Transformation Language) scripts for data quality checks.

The pipeline performs the following steps:
1. Loads LEI data from CSV
2. Reshapes and cleans the data
3. Converts to SDMX format
4. Performs structural validation
5. Runs VTL validation scripts

Dependencies:
    - pandas
    - pysdmx
    - vtlengine
"""

from pathlib import Path
import json

import pandas as pd
from pysdmx.api.fmr import RegistryClient
from pysdmx.io.format import StructureFormat
from pysdmx.io.pd import PandasDataset
from pysdmx.io.csv.sdmx20.writer import write as write_csv_20
from vtlengine import run_sdmx


from utils import validate_data_fmr



def load_lei_data(input_path, row_limit=10000):
    """
    Load LEI data from a CSV file.

    Args:
        input_path (str): Path to the input CSV file containing LEI data
        row_limit (int, optional): Maximum number of rows to read. Defaults to 10000.

    Returns:
        pandas.DataFrame: DataFrame containing the LEI data
    """

    data = pd.read_csv(input_path, dtype=str, nrows=row_limit)

    return data

def reshape_lei_data(data, get_only_active=True):
    """
    Reshape and clean LEI data by renaming columns and filtering active entities.

    Args:
        data (pandas.DataFrame): Input DataFrame containing LEI data
        get_only_active (bool, optional): Whether to filter only active entities. Defaults to True.

    Returns:
        pandas.DataFrame: Cleaned and reshaped DataFrame with standardized column names
    """

    RENAME_DICT = {
        "LEI": "LEI",
        "Entity.LegalName": "LEGAL_NAME",
        "Entity.LegalAddress.Country": "COUNTRY_INCORPORATION",
        "Entity.HeadquartersAddress.Country": "COUNTRY_HEADQUARTERS",
        "Entity.EntityCategory": "CATEGORY",
        "Entity.EntitySubCategory": "SUBCATEGORY",
        "Entity.LegalForm.EntityLegalFormCode": "LEGAL_FORM",
        "Entity.EntityStatus": "STATUS",
        "Entity.LegalAddress.PostalCode": "POSTAL_CODE",
    }

    data.rename(columns=RENAME_DICT, inplace=True)
    data = data[list(RENAME_DICT.values())]

    if get_only_active:
        data = data[data['STATUS'] == 'ACTIVE'].reset_index(drop=True)
    del data['STATUS']

    return data

def get_sdmx_dataset(data, sdmx_api_endpoint="https://fmr.meaningfuldata.eu/sdmx/v2", output_path=None):
    """
    Convert pandas DataFrame to SDMX dataset format.

    Args:
        data (pandas.DataFrame): Input DataFrame containing LEI data
        sdmx_api_endpoint (str, optional): SDMX API endpoint URL. 
            Defaults to "https://fmr.meaningfuldata.eu/sdmx/v2".

    Returns:
        PandasDataset: SDMX-formatted dataset
    """

    client = RegistryClient(
        sdmx_api_endpoint, format=StructureFormat.FUSION_JSON
    )
    schema = client.get_schema(
        "datastructure", agency="MD", id="LEI_DATA", version="1.0"
    )
    # Generate the PandasDataset
    dataset = PandasDataset(structure=schema, data=data)

    if output_path:
        write_csv_20([dataset], output_path)

    return dataset


def structural_validation(dataset, logs_folder=None):
    """
    Perform structural validation of the SDMX dataset using FMR (Fusion Metadata Registry).

    Args:
        dataset (PandasDataset): SDMX dataset to validate

    Returns:
        dict: Validation results from FMR
    """
    # Serialization on SDMX-CSV 2.0
    csv_text = write_csv_20([dataset])

    # Validate using FMR
    result = validate_data_fmr(csv_text, host="fmr.meaningfuldata.eu", port=443,
                            use_https=True)
    
    if logs_folder:
        with open(logs_folder / "structural_validation_logs.json", "w") as f:
            json.dump(result, f)

    return result


def run_vtl_script(vtl_script_query, datasets, logs_folder=None):
    """
    Run VTL (Validation and Transformation Language) script on the dataset.

    Args:
        vtl_script_query (dict): Dictionary containing VTL script parameters:
            - id: Script identifier
            - agency: Agency identifier
            - version: Script version
            - api_endpoint: API endpoint URL
        datasets: SDMX dataset(s) to validate

    Returns:
        dict: Results of the VTL validation
    """

    rc = RegistryClient(api_endpoint=vtl_script_query['api_endpoint'])

    vtl_transformation_scheme = rc.get_vtl_transformation_scheme(
        id=vtl_script_query['id'],
        agency=vtl_script_query['agency'],
        version=vtl_script_query['version'])
    result = run_sdmx(vtl_transformation_scheme, [datasets], return_only_persistent=True)

    if logs_folder:
        for key, value in result.items():
            value.data.to_csv(logs_folder / f"{key}_logs.csv", index=False)

    return result


def lei_to_sdmx_pipeline(
        input_path, 
        row_limit=10000, 
        sdmx_api_endpoint="https://fmr.meaningfuldata.eu/sdmx/v2", 
        vtl_script_query=None,
        output_path=None,
        logs_folder=None
        ):
    """
    Main pipeline function that orchestrates the entire LEI to SDMX transformation process.

    Args:
        input_path (str): Path to the input CSV file containing LEI data
        row_limit (int, optional): Maximum number of rows to process. Defaults to 10000.
        sdmx_api_endpoint (str, optional): SDMX API endpoint URL. 
            Defaults to "https://fmr.meaningfuldata.eu/sdmx/v2".
        vtl_script_query (dict, optional): VTL script parameters for validation. 
            If None, VTL validation is skipped.
        output_path (str, optional): Path to save the output. If None, no file is saved.

    Returns:
        tuple: (dataset, structural_validation_result, validation_result)
            - dataset: The SDMX-formatted dataset
            - structural_validation_result: Results of structural validation
            - validation_result: Results of VTL validation (if performed)
    """

    data = load_lei_data(input_path, row_limit)
    data = reshape_lei_data(data)
    dataset = get_sdmx_dataset(data, sdmx_api_endpoint, output_path)
    structural_validation_result = structural_validation(dataset, logs_folder)
    validation_result = run_vtl_script(vtl_script_query, dataset, logs_folder)
    return dataset, structural_validation_result, validation_result



if __name__ == "__main__":
    FMR_ENDPOINT = "https://fmr.meaningfuldata.eu/sdmx/v2"
    VTL_SCRIPT_QUERY = {
        'id': 'LEI_VALIDATIONS',
        'agency': 'MD',
        'version': '1.0',
        'api_endpoint': FMR_ENDPOINT
    }
    ROW_LIMIT = 10000
    BASE_PATH = Path(__file__).parent
    LEI_DATA_PATH = BASE_PATH / "lei_data" / "gleif-goldencopy-lei2-golden-copy.csv"
    OUTPUT_PATH = BASE_PATH / "output" / "lei_to_sdmx.csv"
    LOGS_FOLDER = BASE_PATH / "log"

    lei_ds, structural_validation_result, validation_result = lei_to_sdmx_pipeline(
        input_path=LEI_DATA_PATH, 
        row_limit=ROW_LIMIT, 
        sdmx_api_endpoint=FMR_ENDPOINT, 
        vtl_script_query=VTL_SCRIPT_QUERY,
        output_path=OUTPUT_PATH,
        logs_folder=LOGS_FOLDER
        )
    
    print(f"Process finished. SDMX dataset saved to {OUTPUT_PATH}")
    print(f"Logs saved to {LOGS_FOLDER}")
    print(validation_result.keys())
