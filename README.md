# CMS Hospital Data Downloader

This script automates the process of downloading and processing CMS provider datasets related to the theme **"Hospitals"**. 

## Features

- **Incremental Updates**: Tracks file modification dates to avoid redundant downloads.
- **Parallel Processing**: Uses multi-threading to download and process multiple CSV files concurrently.
- **Header Transformation**: Automatically converts CSV column names to `snake_case` (e.g., `Patients’ rating...` becomes `patients_rating...`).
- **Data Persistence**: Saves processed files locally for easy access.

## Requirements

- Python 3.7+
- External packages:
  - `requests`: For API interaction.
  - `pandas`: For CSV processing and header transformation.

## Setup

1. **Create a virtual environment**:
   ```bash
   python -m venv venv
   ```

2. **Activate the virtual environment**:
   - **Windows**: `venv\Scripts\activate`
   - **macOS/Linux**: `source venv/bin/activate`

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the script using Python:

```bash
python main.py
```

### Output

- **Processed Data**: CSV files are saved in the `data/` directory with `snake_case` headers.
- **Tracking**: A `metadata.json` file is created to store the last modified dates of processed datasets, enabling incremental runs.

## Technical Notes

- The script uses `concurrent.futures.ThreadPoolExecutor` for parallelism.
- Column transformation logic removes special characters (like apostrophes) and replaces spaces with underscores.
- Run tracking is handled via a local JSON metastore (`metadata.json`).
