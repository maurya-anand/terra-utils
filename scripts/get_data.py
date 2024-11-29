#!/usr/bin/env python

from firecloud import api as fapi
import pandas as pd
import os
import io
import argparse
import concurrent.futures

parser = argparse.ArgumentParser(
    description='Fetch data from a table in the Terra workspace.')
parser.add_argument('-t', '--table', type=str, required=True,
                    help='Name of the table to fetch data from in the Terra workspace')
parser.add_argument('-c', '--column', type=str, required=False,
                    help='Name of the column to fetch data from. If not provided, all columns will be fetched')
parser.add_argument('-o', '--output', type=str, default=os.getcwd(),
                    help='Directory to save the fetched data (default: current working directory)')
parser.add_argument('-r', '--threads', type=int, required=False,
                    help='Number of parallel downloads (default: number of available CPUs)')
args = parser.parse_args()

project = os.environ['WORKSPACE_NAMESPACE']
workspace = os.environ['WORKSPACE_NAME']
workspace_bucket = os.environ['WORKSPACE_BUCKET']

table_name = args.table
column_name = args.column
output_dir = args.output

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

results_table = pd.read_csv(io.StringIO(fapi.get_entities_tsv(
    project, workspace, table_name, model="flexible").text), sep='\t')
tab_id_col = f"entity:{table_name}_id"
results_table.rename(columns={tab_id_col: 'ID'}, inplace=True)

def copy_file(value, column_dir, column, row):
    if pd.notna(value) and value.startswith("gs://"):
        print(f"Column: {column} -- Row: {row + 1} -- Value: {value}")
        os.system(f'gsutil cp "{value}" "{column_dir}/" > /dev/null 2>&1')

max_workers = args.threads if args.threads else os.cpu_count()

if column_name in results_table.columns:
    column_dir = os.path.join(output_dir, column_name)
    if not os.path.exists(column_dir):
        os.makedirs(column_dir)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(copy_file, value, column_dir, column_name, index)
                   for index, value in results_table[column_name].items()]
        for future in concurrent.futures.as_completed(futures):
            future.result()
else:
    for column in results_table.columns:
        if column == "ID":
            continue
        column_dir = os.path.join(output_dir, column)
        if not os.path.exists(column_dir):
            os.makedirs(column_dir)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(copy_file, value, column_dir, column, index)
                       for index, value in results_table[column].items()]
            for future in concurrent.futures.as_completed(futures):
                future.result()
        print("-" * 40)
