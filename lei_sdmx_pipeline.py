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
import os
import zipfile

import pandas as pd
from pysdmx.api.fmr import RegistryClient
from pysdmx.io.format import StructureFormat
from pysdmx.io.pd import PandasDataset
from pysdmx.io.csv.sdmx20.writer import write as write_csv_20
from vtlengine import run_sdmx


from pysdmx.model.concept import DataType
from pysdmx.model.dataflow import Schema


# Mapping of the raw GLEIF golden-copy column names to the standardized names used
# downstream. The raw file has ~338 columns; we only ever need these. Loading just
# these (via `usecols`) instead of the whole file is the key to keeping memory sane.
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
SOURCE_COLUMNS = list(RENAME_DICT.keys())

# SDMX data types that should hold numeric values, used by structural validation.
NUMERIC_DATA_TYPES = {
    DataType.BIG_INTEGER, DataType.COUNT, DataType.DECIMAL, DataType.DOUBLE,
    DataType.FLOAT, DataType.INTEGER, DataType.LONG, DataType.SHORT, DataType.NUMERIC,
}

# Maximum number of offending example values recorded per validation error.
MAX_ERROR_SAMPLES = 5

# Headroom multiplier over the raw in-memory estimate, to account for pandas'
# intermediate copies during parsing and reshaping.
MEMORY_SAFETY_FACTOR = 2.0
# Warn (and ask the user) if the estimated need exceeds this fraction of free RAM.
MEMORY_WARN_FRACTION = 0.7


def _is_zip(input_path):
    return str(input_path).lower().endswith(".zip")


def get_available_memory():
    """
    Return available system memory in bytes, or None if it cannot be determined.

    Uses psutil when installed; otherwise falls back to a Linux sysconf query so
    the check still works in a plain venv without extra dependencies.
    """
    try:
        import psutil
        return psutil.virtual_memory().available
    except Exception:
        try:
            return os.sysconf("SC_AVAIL_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
        except (ValueError, AttributeError, OSError):
            return None


def _avg_line_bytes(file_handle, sample_lines):
    """Average byte length of the first `sample_lines` raw lines of a binary stream."""
    total = count = 0
    for line in file_handle:
        total += len(line)
        count += 1
        if count >= sample_lines:
            break
    return total / count if count else None


def estimate_load_memory(input_path, usecols=SOURCE_COLUMNS, sample_rows=100_000):
    """
    Estimate the RAM (in bytes) needed to load `usecols` from the file.

    Works by measuring the in-memory size per row of a sample of the selected
    columns, then projecting it over the total number of rows (estimated from the
    uncompressed file size divided by the average raw line length).

    Returns None if the estimate cannot be computed.
    """
    compression = "zip" if _is_zip(input_path) else "infer"

    sample = pd.read_csv(input_path, dtype=str, usecols=usecols,
                         nrows=sample_rows, compression=compression)
    if len(sample) == 0:
        return 0
    bytes_per_row = sample.memory_usage(deep=True).sum() / len(sample)

    if _is_zip(input_path):
        with zipfile.ZipFile(input_path) as zf:
            uncompressed = sum(info.file_size for info in zf.infolist())
            with zf.open(zf.namelist()[0]) as fh:
                avg_line = _avg_line_bytes(fh, sample_rows)
    else:
        uncompressed = os.path.getsize(input_path)
        with open(input_path, "rb") as fh:
            avg_line = _avg_line_bytes(fh, sample_rows)

    if not avg_line:
        return None
    estimated_rows = max(uncompressed / avg_line - 1, 0)  # subtract the header line
    return int(bytes_per_row * estimated_rows)


def memory_is_safe_to_load(input_path, usecols=SOURCE_COLUMNS):
    """
    Assess whether loading the file is likely to fit safely in memory.

    Returns a tuple (is_safe, required_bytes, available_bytes), where is_safe is
    True/False, or None if it could not be determined.
    """
    available = get_available_memory()
    estimated = estimate_load_memory(input_path, usecols)
    if available is None or estimated is None:
        return None, estimated, available
    required = estimated * MEMORY_SAFETY_FACTOR
    return required <= available * MEMORY_WARN_FRACTION, required, available


def load_lei_data(input_path, assume_yes=False):
    """
    Load LEI data from a CSV file, optionally compressed in a zip archive.

    Only the columns needed downstream (``SOURCE_COLUMNS``) are read, which keeps
    memory usage low even on the full GLEIF golden-copy file. Before loading, the
    estimated memory requirement is compared against available RAM; if loading is
    likely to exhaust memory, the user is asked to confirm (unless ``assume_yes``).

    Args:
        input_path (str): Path to the input data file containing LEI data.
            Can be a plain CSV file or a zip archive containing a single CSV.
        assume_yes (bool, optional): If True, skip the interactive confirmation and
            load even when memory looks tight. Defaults to False.

    Returns:
        pandas.DataFrame: DataFrame containing the needed LEI columns

    Raises:
        MemoryError: If the load looks unsafe and the user declines to proceed.
    """

    compression = "zip" if _is_zip(input_path) else "infer"

    print(f"[1/6] Loading LEI data from {input_path}...")
    print("      Checking whether this file fits safely in memory...")
    is_safe, required, available = memory_is_safe_to_load(input_path)
    if required is not None and available is not None:
        print(f"      Estimated need: ~{required / 1e9:.2f} GB "
              f"| available: ~{available / 1e9:.2f} GB")

    if is_safe is None:
        print("      Could not estimate memory usage; proceeding with caution.")
    elif not is_safe and not assume_yes:
        print("      WARNING: loading this file may exhaust memory and crash your computer.")
        print("      Consider creating a smaller snapshot instead.")
        answer = input("      Proceed anyway? [y/N]: ").strip().lower()
        if answer not in ("y", "yes"):
            raise MemoryError(
                "Aborted: file too large to load safely in available memory."
            )

    data = pd.read_csv(input_path, dtype=str, usecols=SOURCE_COLUMNS,
                       compression=compression)
    print(f"      Loaded {len(data)} rows and {len(data.columns)} columns "
          f"(of ~338 available).")

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

    print("[2/6] Reshaping and cleaning LEI data...")
    data.rename(columns=RENAME_DICT, inplace=True)
    data = data[list(RENAME_DICT.values())]

    if get_only_active:
        data = data[data['STATUS'] == 'ACTIVE'].reset_index(drop=True)
        print(f"      Filtered to {len(data)} ACTIVE entities.")
    del data['STATUS']
    print(f"      Reshaped to {len(data)} rows and {len(data.columns)} columns.")

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

    print(f"[3/6] Converting to SDMX dataset (fetching schema from {sdmx_api_endpoint})...")
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
        print(f"      SDMX dataset written to {output_path}.")

    return dataset


def _allowed_codes(component):
    """
    Return the set of valid code ids for a coded component, or None if the component
    is uncoded or its codelist is not resolved into iterable codes.
    """
    enumeration = component.enumeration
    if enumeration is None:
        return None
    codes = getattr(enumeration, "codes", enumeration)
    try:
        allowed = {code.id for code in codes}
    except (TypeError, AttributeError):
        return None
    return allowed or None


def structural_validation(dataset, logs_folder=None):
    """
    Perform structural validation of the SDMX dataset against its schema using pysdmx.

    Validates the dataset's DataFrame against the components of its ``Schema``
    (carried in ``dataset.structure``), checking, for each component:
      - required components are present and non-empty,
      - coded components only contain values from their codelist,
      - numeric-typed components only contain numeric values,
      - values respect the ``max_length`` facet when defined.

    Args:
        dataset (PandasDataset): SDMX dataset to validate. Its ``structure`` must
            be a resolved ``Schema`` (as produced by ``get_sdmx_dataset``).
        logs_folder (Path, optional): Folder to write the validation report as JSON.

    Returns:
        dict: Validation report with keys ``is_valid``, ``row_count`` and ``errors``
            (a list of {component, type, message, count, samples} entries).
    """
    print("[4/6] Running structural validation against the schema (pysdmx)...")

    schema = dataset.structure
    if not isinstance(schema, Schema):
        raise TypeError(
            "Dataset structure is not a resolved Schema; cannot validate structurally."
        )

    data = dataset.data
    errors = []

    def add_error(component_id, error_type, message, mask=None):
        entry = {"component": component_id, "type": error_type, "message": message}
        if mask is not None:
            count = int(mask.sum())
            if count == 0:
                return
            entry["count"] = count
            entry["samples"] = (
                data.loc[mask, component_id].head(MAX_ERROR_SAMPLES).tolist()
            )
        errors.append(entry)

    for component in schema.components:
        col = component.id

        # A required component whose column is entirely missing from the data.
        if col not in data.columns:
            if component.required:
                add_error(col, "missing_component",
                          f"Required component '{col}' is missing from the data.")
            continue

        series = data[col]
        is_empty = series.isna() | (series.astype(str).str.strip() == "")

        if component.required:
            add_error(col, "missing_value",
                      f"Required component '{col}' has empty values.", is_empty)

        present = series[~is_empty]

        allowed = _allowed_codes(component)
        if allowed is not None and not present.empty:
            invalid = ~present.isin(allowed)
            add_error(col, "invalid_code",
                      f"Component '{col}' has values outside its codelist.",
                      invalid.reindex(series.index, fill_value=False))

        if component.dtype in NUMERIC_DATA_TYPES and not present.empty:
            non_numeric = pd.to_numeric(present, errors="coerce").isna()
            add_error(col, "invalid_type",
                      f"Component '{col}' has non-numeric values.",
                      non_numeric.reindex(series.index, fill_value=False))

        facets = component.facets
        if facets is not None and facets.max_length and not present.empty:
            too_long = present.astype(str).str.len() > facets.max_length
            add_error(col, "max_length",
                      f"Component '{col}' exceeds max length {facets.max_length}.",
                      too_long.reindex(series.index, fill_value=False))

    result = {
        "is_valid": len(errors) == 0,
        "row_count": int(len(data)),
        "errors": errors,
    }

    if logs_folder:
        with open(logs_folder / "structural_validation_logs.json", "w") as f:
            json.dump(result, f, indent=2)
        print(f"      Structural validation logs saved to {logs_folder}.")
    status = "PASSED" if result["is_valid"] else f"FAILED ({len(errors)} issue(s))"
    print(f"      Structural validation finished: {status}.")

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

    print(f"[5/6] Running VTL script '{vtl_script_query['id']}' "
          f"({vtl_script_query['agency']}, v{vtl_script_query['version']})...")
    rc = RegistryClient(api_endpoint=vtl_script_query['api_endpoint'])

    vtl_transformation_scheme = rc.get_vtl_transformation_scheme(
        id=vtl_script_query['id'],
        agency=vtl_script_query['agency'],
        version=vtl_script_query['version'])
    result = run_sdmx(vtl_transformation_scheme, [datasets],
                      return_only_persistent=True, use_duckdb=True)

    if logs_folder:
        for key, value in result.items():
            value.data.to_csv(logs_folder / f"{key}_logs.csv", index=False)
        print(f"      VTL validation logs saved to {logs_folder}.")
    print(f"      VTL script finished. Produced {len(result)} result dataset(s).")

    return result


def run_calculation(calculation_query, datasets, logs_folder=None, output_path=None):
    """
    Run a VTL calculation script on the dataset (e.g. the FMR ``LEI_CALCULATION`` scheme).

    Unlike ``run_vtl_script``, which is used for validations, this runs a calculation
    transformation scheme and returns its computed (persistent) output datasets.

    Args:
        calculation_query (dict): Dictionary containing the calculation script parameters:
            - id: Script identifier (e.g. 'LEI_CALCULATION')
            - agency: Agency identifier
            - version: Script version
            - api_endpoint: API endpoint URL
        datasets: SDMX dataset(s) to feed into the calculation
        logs_folder (Path, optional): Folder to write each result dataset as a CSV.
        output_path (Path, optional): Folder to write each calculated dataset as a CSV.

    Returns:
        dict: Computed calculation result datasets
    """

    print(f"[6/6] Running VTL calculation '{calculation_query['id']}' "
          f"({calculation_query['agency']}, v{calculation_query['version']})...")
    rc = RegistryClient(api_endpoint=calculation_query['api_endpoint'])

    vtl_transformation_scheme = rc.get_vtl_transformation_scheme(
        id=calculation_query['id'],
        agency=calculation_query['agency'],
        version=calculation_query['version'])
    result = run_sdmx(vtl_transformation_scheme, [datasets],
                      return_only_persistent=True, use_duckdb=True)

    if output_path:
        for key, value in result.items():
            value.data.to_csv(output_path / f"{key}.csv", index=False)
        print(f"      Calculation results saved to {output_path}.")
    if logs_folder:
        for key, value in result.items():
            value.data.to_csv(logs_folder / f"{key}_calculation_logs.csv", index=False)
        print(f"      Calculation logs saved to {logs_folder}.")
    print(f"      VTL calculation finished. Produced {len(result)} result dataset(s).")

    return result


def lei_to_sdmx_pipeline(
        input_path,
        sdmx_api_endpoint="https://fmr.meaningfuldata.eu/sdmx/v2",
        vtl_script_query=None,
        calculation_query=None,
        output_path=None,
        logs_folder=None,
        assume_yes=False
        ):
    """
    Main pipeline function that orchestrates the entire LEI to SDMX transformation process.

    Args:
        input_path (str): Path to the input CSV file containing LEI data
        sdmx_api_endpoint (str, optional): SDMX API endpoint URL.
            Defaults to "https://fmr.meaningfuldata.eu/sdmx/v2".
        vtl_script_query (dict, optional): VTL script parameters for validation.
            If None, VTL validation is skipped.
        calculation_query (dict, optional): VTL script parameters for calculation
            (e.g. the 'LEI_CALCULATION' scheme). If None, the calculation is skipped.
        output_path (str, optional): Path to save the output. If None, no file is saved.
        assume_yes (bool, optional): If True, skip the memory-safety confirmation when
            loading the input file. Defaults to False.

    Returns:
        tuple: (dataset, structural_validation_result, validation_result, calculation_result)
            - dataset: The SDMX-formatted dataset
            - structural_validation_result: Results of structural validation
            - validation_result: Results of VTL validation (if performed)
            - calculation_result: Results of the VTL calculation (if performed)
    """

    print("=== Starting LEI to SDMX pipeline ===")
    data = load_lei_data(input_path, assume_yes=assume_yes)
    data = reshape_lei_data(data)
    dataset = get_sdmx_dataset(data, sdmx_api_endpoint, output_path)
    structural_validation_result = structural_validation(dataset, logs_folder)
    validation_result = run_vtl_script(vtl_script_query, dataset, logs_folder)
    calculation_result = None
    if calculation_query:
        output_dir = Path(output_path).parent if output_path else None
        calculation_result = run_calculation(calculation_query, dataset, logs_folder, output_dir)
    print("=== Pipeline finished ===")
    return dataset, structural_validation_result, validation_result, calculation_result



if __name__ == "__main__":
    FMR_ENDPOINT = "https://fmr.meaningfuldata.eu/sdmx/v2"
    VTL_SCRIPT_QUERY = {
        'id': 'LEI_VALIDATIONS',
        'agency': 'MD',
        'version': '1.0',
        'api_endpoint': FMR_ENDPOINT
    }
    CALCULATION_QUERY = {
        'id': 'LEI_CALCULATION',
        'agency': 'MD',
        'version': '1.0',
        'api_endpoint': FMR_ENDPOINT
    }
    BASE_PATH = Path(__file__).parent
    LEI_DATA_PATH = BASE_PATH / "lei_data" / "20260601-0800-gleif-goldencopy-lei2-golden-copy.csv.zip"
    OUTPUT_PATH = BASE_PATH / "output" / "lei_to_sdmx.csv"
    LOGS_FOLDER = BASE_PATH / "log"

    lei_ds, structural_validation_result, validation_result, calculation_result = lei_to_sdmx_pipeline(
        input_path=LEI_DATA_PATH,
        sdmx_api_endpoint=FMR_ENDPOINT,
        vtl_script_query=VTL_SCRIPT_QUERY,
        calculation_query=CALCULATION_QUERY,
        output_path=OUTPUT_PATH,
        logs_folder=LOGS_FOLDER
        )

    print(f"Process finished. SDMX dataset saved to {OUTPUT_PATH}")
    print(f"Logs saved to {LOGS_FOLDER}")
    print(f"Validation results: {list(validation_result.keys())}")
    print(f"Calculation results: {list(calculation_result.keys()) if calculation_result else None}")
