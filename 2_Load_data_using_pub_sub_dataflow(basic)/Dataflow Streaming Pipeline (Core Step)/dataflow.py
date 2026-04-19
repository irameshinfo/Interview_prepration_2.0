import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

class ParseJSON(beam.DoFn):
    def process(self, element):
        record = json.loads(element.decode('utf-8'))
        yield record

options = PipelineOptions(
    streaming=True,
    project='ranjanrishi-project',
    region='us-central1',
    temp_location='gs://dataflowtempbucket7'
)

with beam.Pipeline(options=options) as p:
    (
        p
        | "Read from PubSub" >> beam.io.ReadFromPubSub(
            topic="projects/ranjanrishi-project/topics/emp-stream-topic"
        )
        | "Parse JSON" >> beam.ParDo(ParseJSON())
        | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            "ranjanrishi-project:emp_dataset.emp_stream_table",
            schema="emp_id:INTEGER,name:STRING,dept:STRING,sal:INTEGER",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )