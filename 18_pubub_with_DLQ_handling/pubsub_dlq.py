import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import WriteToPubSub


PROJECT_ID = "rameshgcplearning"
SUBSCRIPTION = "projects/rameshgcplearning/subscriptions/employee-sub"
DLQ_TOPIC = "projects/rameshgcplearning/topics/employee-dlq-topic"

BQ_TABLE = "rameshgcplearning:emp_dataset.employee_stream"


class ValidateAndTransform(beam.DoFn):

    def process(self, element):

        try:
            record = json.loads(element.decode('utf-8'))

            required_fields = ['emp_id', 'emp_name', 'salary']

            for field in required_fields:
                if field not in record:
                    raise ValueError(f"Missing field: {field}")

            output = {
                'emp_id': int(record['emp_id']),
                'emp_name': record['emp_name'],
                'salary': int(record['salary'])
            }

            yield beam.pvalue.TaggedOutput('valid', output)

        except Exception as e:

            error_record = {
                "error": str(e),
                "payload": element.decode('utf-8')
            }

            yield beam.pvalue.TaggedOutput(
                'invalid',
                json.dumps(error_record).encode('utf-8')
            )


pipeline_options = PipelineOptions(
    streaming=True,
    project=PROJECT_ID,
    region='us-central1',
    temp_location='gs://ramesh-dataflow-bucket/temp',
    staging_location='gs://ramesh-dataflow-bucket/staging'
)

with beam.Pipeline(options=pipeline_options) as p:

    messages = (
        p
        | "Read PubSub" >> ReadFromPubSub(
            subscription=SUBSCRIPTION
        )
    )

    results = (
        messages
        | "Validate Records" >> beam.ParDo(
            ValidateAndTransform()
        ).with_outputs('valid', 'invalid')
    )

    valid_records = results.valid
    invalid_records = results.invalid

    valid_records | "Write BQ" >> WriteToBigQuery(
        BQ_TABLE,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )

    invalid_records | "Write DLQ" >> WriteToPubSub(
        DLQ_TOPIC
    )