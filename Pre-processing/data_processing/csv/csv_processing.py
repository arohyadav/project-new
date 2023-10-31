import apache_beam as beam
import csv
import io
import json
import os
import psycopg2
import logging
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.pubsub import WriteToPubSub

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../dataplatform.json"

def fetch_column_mapping(host, port, database, user, password):
    column_mapping = {}
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        print("Successfully connected to the PostgreSQL database")
    except psycopg2.Error as e:
        print("Failed to connect to the PostgreSQL database:", e)
        return column_mapping

    if conn is not None and not conn.closed:
        try:
            cursor = conn.cursor()

            # List of child table names
            child_tables = ["child_customers", "child_marketing", "child_finance"]

            for child_table in child_tables:
                # Fetch data from the current child table
                cursor.execute(f'SELECT field_id, field_names FROM "RestaurantMVP".{child_table}')
                child_data = cursor.fetchall()

                # Fetch data from the master table for each child table
                cursor.execute('SELECT field_id, field_names FROM "RestaurantMVP".master_restaurant')
                master_data = cursor.fetchall()

                # Perform inner join to match field_id from the current child table with master table
                joined_data = {c[0]: (c[1], m[1]) for c in child_data for m in master_data if c[0] == m[0]}

                # Store the field_names from both tables in a dictionary
                for field_id, (child_field_name, master_field_name) in joined_data.items():
                    column_mapping[child_field_name] = master_field_name

            print("Column mapping fetched successfully")
        except Exception as e:
            print("Failed to fetch column mapping:", e)
        finally:
            cursor.close()
            conn.close()
    else:
        print("Connection to the PostgreSQL database is not established.")

    return column_mapping

# Example usage:
host = "34.29.183.25"
port = "5432"
database = "postgres"
user = "postgres"
password = "FURc+($fTmqAgXF2"

column_mapping = fetch_column_mapping(host, port, database, user, password)
def clean_column_name(column_name):
    column_name = column_name.replace('\n', ' ').strip()
    while '(' in column_name and ')' in column_name:
        start_idx = column_name.index('(')
        end_idx = column_name.index(')')
        column_name = column_name[:start_idx] + column_name[end_idx + 1:]
    return column_name.strip()

class ReadCSVAsDict(beam.DoFn):
    def process(self, element, batch_size, *args, **kwargs):
        rows = csv.reader(io.TextIOWrapper(element.open(), encoding='utf-8'))
        header = next(rows)
        mapped_header = [col for col in header if clean_column_name(col) in column_mapping]

        batch = []
        for row in rows:
            extracted_row = {
                column_mapping[clean_column_name(col)]: row[idx]
                for idx, col in enumerate(header)
                if col in mapped_header
            }
            batch.append(extracted_row)

            if len(batch) == batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

class ConvertToJSON(beam.DoFn):
    def process(self, batch, *args, **kwargs):
        for row in batch:
            if row:
                yield json.dumps(row)

class ReadCSVFiles(beam.PTransform):
    def __init__(self, file_pattern, batch_size):
        self.file_pattern = file_pattern
        self.batch_size = batch_size

    def expand(self, pcoll):
        return (pcoll
                | fileio.MatchFiles(self.file_pattern)
                | fileio.ReadMatches()
                | beam.ParDo(ReadCSVAsDict(), batch_size=self.batch_size)
               )

class RemoveEmptyDicts(beam.DoFn):
    def process(self, element):
        if element:
            yield element

# Assuming you have a global `column_mapping` defined in your `main.py`

def process_csv_files(p, file_pattern, batch_size, pubsub_topic):
    csv_data = (
        p
        | f'match files {file_pattern}' >> ReadCSVFiles(file_pattern=file_pattern, batch_size=batch_size)
    )
    filtered_data = csv_data | f'RemoveEmptyDicts {file_pattern}' >> beam.ParDo(RemoveEmptyDicts())
    json_data = filtered_data | f'ConvertToJSON {file_pattern}' >> beam.ParDo(ConvertToJSON())

    encoded_json_data = json_data | f"Serialize JSON data {file_pattern}" >> beam.Map(lambda json_data: json_data.encode('utf-8'))

    encoded_json_data | f"Write to Pub/Sub {file_pattern}" >> WriteToPubSub(
        topic=pubsub_topic,
        with_attributes=False
    )
