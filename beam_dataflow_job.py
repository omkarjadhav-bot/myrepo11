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

def run():
    try:
        logger.info("Starting Dataflow pipeline.")
        data = [1, 2, 3, 4, 5]
        logger.info(f"Data initialized: {data}")

        def log_result(x):
            logger.info(f"Result: {x}")

        with beam.Pipeline(options=options) as pipeline:
            logger.info("Pipeline object created.")
            pc1 = pipeline | 'create numbers' >> beam.Create(data)
            logger.info("Created PCollection from data.")
            pc2 = pc1 | 'filter data' >> beam.Filter(lambda x: x % 2 == 0)
            logger.info("Filtered even numbers.")
            pc3 = pc2 | 'square' >> beam.Map(lambda x: x * x)
            logger.info("Squared filtered numbers.")
            pc3 | 'log result' >> beam.Map(log_result)
            logger.info("Logged results.")
        logger.info("Pipeline finished.")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")

if __name__ == "__main__":
    run()
