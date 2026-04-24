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
            topic="projects/rameshgcplearning/topics/emp-trans-stream-topic"
        )
        | "Parse JSON" >> beam.ParDo(ParseJSON())
        # ✅ Transformation 1: Filter 
        | "Filter IT Dept" >> beam.Filter(lambda x: x["dept"] == "IT")
        # ✅ Transformation 2: Add bonus
        | "Add Bonus" >> beam.Map(lambda x: { **x, "bonus": x["sal"] * 0.2 })
        # ✅ Transformation 3: Increase salary
        | "Increase Salary" >> beam.Map(lambda x: { **x, "sal": int(x["sal"] * 1.1) })
        | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            "rameshgcplearning:emp_trans_dataset.emp_stream_table",
            schema="emp_id:INTEGER,name:STRING,dept:STRING,sal:INTEGER,bonus:FLOAT",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )