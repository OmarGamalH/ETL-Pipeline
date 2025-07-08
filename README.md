# Tax Data ETL Pipeline

## Overview

This project implements a simple **Extract, Transform, Load (ETL)** pipeline in Python to process U.S. tax data per industry. It downloads a dataset from a remote source, transforms the data by calculating new fields and filtering on specific conditions, and finally stores the result as a **Parquet file**.

## Features

- ✅ **Extract**: Downloads and reads a CSV file of raw tax data.
- 🔁 **Transform**: 
  - Calculates `tax_rate` and `average_taxable_income`.
  - Filters rows where `average_taxable_income > 100`.
  - Sets `industry_name` as index.
- 📦 **Load**: Saves the cleaned dataset to a `.parquet` file.
- 🧪 **Testing**: Includes unit tests to verify functionality and data integrity.
- 🪵 **Logging**: Tracks ETL steps and records any failures.

## Technologies Used

- Python
- Pandas
- NumPy
- Pytest
- Parquet format (via Pandas)
- Logging

## File Structure

```
.
├── ETL.py               # Main ETL pipeline script
├── unit_test.py         # Unit tests using pytest
├── logging.txt          # Logs of the latest ETL execution
├── raw_tax_data.csv     # Downloaded raw tax dataset (auto-downloaded)
└── clean_tax_data.parquet # Cleaned dataset (output)
```

## How to Run

1. **Install Dependencies**
   Make sure `pandas`, `numpy`, and `pytest` are installed:
   ```bash
   pip install pandas numpy pytest
   ```

2. **Run the ETL Pipeline**
   ```bash
   python ETL.py
   ```

3. **Run Tests**
   ```bash
   pytest unit_test.py
   ```

## Output

- The cleaned dataset will be saved as: `clean_tax_data.parquet`
- Logs of the pipeline run are saved in: `logging.txt`

## Example Log Output

```
INFO : raw_tax_data.csv has been extracted
INFO : average taxable income column was created
INFO : data has been filtered for average taxable income larger than 100
INFO : industry_name has been set as index column
DEBUG : clean data shape : (82, 6)
INFO : Successfully extracted, transformed, and loaded data to the destination.
```

## License

This project is open-source and free to use under the [MIT License](https://opensource.org/licenses/MIT).
