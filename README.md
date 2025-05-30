# LEI to SDMX Pipeline

This project provides a pipeline for transforming Legal Entity Identifier (LEI) data into SDMX (Statistical Data and Metadata eXchange) format, with built-in validation and data quality checks.

## Overview

The pipeline processes LEI data through several stages:
1. Data loading from CSV format
2. Data cleaning and reshaping
3. Conversion to SDMX format
4. Structural validation using FMR (Fusion Metadata Registry)
5. Data quality validation using VTL (Validation and Transformation Language) scripts

## Prerequisites

- Python 3.7 or higher
- Required Python packages (install via `pip install -r requirements.txt`):
  - pandas
  - pysdmx
  - vtlengine

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/lei_sdmx.git
cd lei_sdmx
```

2. Install dependencies (using poetry):
```bash
poetry install
```

## Usage

The main pipeline can be used as follows:

```python
from pathlib import Path
from lei_sdmx_pipeline import lei_to_sdmx_pipeline

# Configure paths
base_path = Path(__file__).parent
lei_data_path = base_path / "lei_data" / "gleif-goldencopy-lei2-golden-copy.csv"
output_path = base_path / "output" / "lei_to_sdmx.csv"
logs_folder = base_path / "log"

# Configure the pipeline
sdmx_api_endpoint = "https://fmr.meaningfuldata.eu/sdmx/v2"
vtl_script_query = {
    'id': 'LEI_VALIDATIONS',
    'agency': 'MD',
    'version': '1.0',
    'api_endpoint': sdmx_api_endpoint
}

# Run the pipeline
dataset, structural_validation_result, validation_result = lei_to_sdmx_pipeline(
    input_path=lei_data_path,
    row_limit=10000,
    sdmx_api_endpoint=sdmx_api_endpoint,
    vtl_script_query=vtl_script_query,
    output_path=output_path,
    logs_folder=logs_folder
)

# Check results
print(f"Process finished. SDMX dataset saved to {output_path}")
print(f"Logs saved to {logs_folder}")
print("Available validation results:", validation_result.keys())
```

Note that the function is already implemented in the file lei_sdmx_pipeline.py

### Input Data Format

The input CSV file is the LEI golden copy, whic can be found [here](https://www.gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy#/)
Please bear in mind that you should download a file, and change the parameters in the code to point to the right CSV file.

### Output

The pipeline produces:
1. An SDMX-formatted dataset
2. Structural validation results from FMR (saved to `log/structural_validation_logs.json`)
3. Data quality validation results from VTL scripts (saved to CSV files in the `log` folder)
4. A CSV file in SDMX CSV 2.0 format (saved to the specified output path)

## Project Structure

```
lei_sdmx/
├── lei_sdmx_pipeline.py    # Main pipeline implementation
├── utils.py               # Utility functions for FMR validation
├── pyproject.toml         # Poetry dependencies
├── README.md             # This file
├── lei_data/             # Directory for input LEI data
├── output/               # Directory for SDMX output files
└── log/                  # Directory for validation logs
```

## Validation

The pipeline performs two types of validation:

1. **Structural Validation**: Ensures the data conforms to the SDMX structure defined in the FMR
2. **VTL Validation**: Runs custom validation rules defined in VTL scripts to check data quality
