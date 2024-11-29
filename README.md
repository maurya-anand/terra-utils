# terra-utils

Custom utilities for Terra.bio

## Get data from a data table

This script can be used in an interactive session to download the files listed in a data table.

This is handy when the data table contains results from several workflow submissions.

### Setup

1. Open an interactive Jupyter or RStudio session from a particular workspace which contains the table holding the data.

1. Run the code as described below:

```code
usage: get_data.py [-h] -t TABLE [-c COLUMN] [-o OUTPUT] [-r THREADS]

Fetch data from a table in the Terra workspace.

options:
  -h, --help            show this help message and exit
  -t TABLE, --table TABLE
                        Name of the table to fetch data from in the Terra
                        workspace
  -c COLUMN, --column COLUMN
                        Name of the column to fetch data from. If not provided,
                        all columns will be fetched
  -o OUTPUT, --output OUTPUT
                        Directory to save the fetched data (default: current
                        working directory)
  -r THREADS, --threads THREADS
                        Number of parallel downloads (default: number of
                        available CPUs)
```
