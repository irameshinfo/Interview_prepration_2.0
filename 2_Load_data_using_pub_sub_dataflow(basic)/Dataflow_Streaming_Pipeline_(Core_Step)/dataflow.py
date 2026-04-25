import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

class ParseJSON(beam.DoFn):
    def process(self, element):
        record = json.loads(element.decode('utf-8'))
        yield record

options = PipelineOptions(
    streaming=True,
    project='rameshgcplearning',
    region='us-central1',
    temp_location='gs://dataflowtempbucket7'
)

with beam.Pipeline(options=options) as p:
    (
        p
        | "Read from PubSub" >> beam.io.ReadFromPubSub(
            subscription="projects/rameshgcplearning/subscriptions/emp-stream-sub"
        )
        | "Parse JSON" >> beam.ParDo(ParseJSON())
        | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            "rameshgcplearning:emp_dataset.emp_stream_table",
            schema="emp_id:INTEGER,name:STRING,dept:STRING,sal:INTEGER",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )