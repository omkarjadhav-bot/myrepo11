import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set Dataflow pipeline options
options = PipelineOptions(
    runner='DataflowRunner',
    project='my-gcp-proj1-473011',
    region='europe-west1',
    zone='europe-west1-b',
    temp_location='gs://beam-demo-bucket-euw1/temp',
    staging_location='gs://beam-demo-bucket-euw1/staging',
    job_name='beam-demo-job'
)

class LogStep(beam.DoFn):
    def __init__(self, message):
        self.message = message
    def process(self, element):
        logger.info(f"{self.message}: {element}")
        yield element

def run():
    try:
        data = [1, 2, 3, 4, 5]

        with beam.Pipeline(options=options) as pipeline:
            pc1 = (
                pipeline
                | 'create numbers' >> beam.Create(data)
                | 'log created' >> beam.ParDo(LogStep("Created PCollection from data"))
            )
            pc2 = (
                pc1
                | 'filter data' >> beam.Filter(lambda x: x % 2 == 0)
                | 'log filtered' >> beam.ParDo(LogStep("Filtered even numbers"))
            )
            pc3 = (
                pc2
                | 'square' >> beam.Map(lambda x: x * x)
                | 'log squared' >> beam.ParDo(LogStep("Squared filtered numbers"))
            )
            pc3 | 'log result' >> beam.ParDo(LogStep("Result"))
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")

if __name__ == "__main__":
    run()
