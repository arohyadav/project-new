import argparse
import apache_beam as beam
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from data_processing.csv import csv_processing

def create_pipeline_options(args):
    pipeline_options = PipelineOptions.from_dictionary({
        'runner': args.runner,
        'project': args.project,
        'job_name': args.job_name,
        'temp_location': args.temp_location,
        'region': 'asia-south2',
        'streaming': True,
        'save_main_session': True,
    })

    return pipeline_options

def run(args):
    file_patterns = [
        'gs://aroh_source_bucket/*.csv',  # Replace with your GCS bucket and file pattern
        # Add more patterns as needed
    ]
    batch_size = 1000
    pubsub_topic = "projects/data-platform-391617/topics/aroh-mappingpubsub"

    with beam.Pipeline(options=create_pipeline_options(args)) as p:
        for file_pattern in file_patterns:
            csv_data = (
                p 
                | f'match files {file_pattern}' >> csv_processing.ReadCSVFiles(file_pattern=file_pattern, batch_size=batch_size) 
            )
            filtered_data = csv_data | f'RemoveEmptyDicts {file_pattern}' >> beam.ParDo(csv_processing.RemoveEmptyDicts())
            json_data = filtered_data | f'ConvertToJSON {file_pattern}' >> beam.ParDo(csv_processing.ConvertToJSON())

            # Serialize and encode JSON data using beam.Map
            encoded_json_data = json_data | f"Serialize JSON data {file_pattern}" >> beam.Map(lambda json_data: json_data.encode('utf-8'))
            
            # Write encoded JSON data to Pub/Sub
            encoded_json_data | f"Write to Pub/Sub {file_pattern}" >> WriteToPubSub(
                topic=pubsub_topic,
                with_attributes=False
            )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Dataflow Pipeline with Dataflow Credentials')
    parser.add_argument('--runner', default='DirectRunner', help='Dataflow runner (default: DirectRunner)')
    parser.add_argument('--project', required=True, help='GCP Project ID')
    parser.add_argument('--job_name', required=True, help='Dataflow job name')
    parser.add_argument('--temp_location', required=True, help='GCS location for temporary files')

    args = parser.parse_args()
    run(args)