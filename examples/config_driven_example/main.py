import pathlib
import yaml
from azure.mgmt.datafactory.models import BlobSink, OracleSource

from adfpy.activities.execution import AdfCopyActivity, AdfDatabricksSparkPythonActivity
from adfpy.pipeline import AdfPipeline


def load_yaml(path):
    with open(path, 'r') as file:
        return yaml.safe_load(file)


config = load_yaml(f"{pathlib.Path(__file__).parent}/pipeline_config.yml")
dataFlowSettings = config["dataFlowSettings"]

extract = AdfCopyActivity(
    name="Extract data",
    input_dataset_name=dataFlowSettings["source"]["dataset"],
    output_dataset_name=dataFlowSettings["landing"]["dataset"],
    source_type=OracleSource,
    sink_type=BlobSink,
)

ingestion_parameters = [
    "--source_path",
    dataFlowSettings['landing']['path'],
    "--data_schema",
    config["dataDefinitions"]['tables'],
    "--target_path",
    dataFlowSettings['ingested']['path']
]

ingest = AdfDatabricksSparkPythonActivity(name="Ingest Data",
                                          python_file="my_ingestion_script.py",
                                          parameters=ingestion_parameters)

extract >> ingest

pipeline = AdfPipeline(name=config["meta"]["name"],
                       activities=[extract, ingest],
                       schedule=config["meta"]["trigger"]["schedule"])
