from azure.mgmt.datafactory.models import BlobSource, BlobSink

from adfpy.activities.execution import AdfCopyActivity, AdfDatabricksSparkPythonActivity
from adfpy.pipeline import AdfPipeline

extract = AdfCopyActivity(
    name="copyBlobToBlob",
    input_dataset_name="staging",
    output_dataset_name="landing",
    source_type=BlobSource,
    sink_type=BlobSink,
)

ingest = AdfDatabricksSparkPythonActivity(name="ingest", python_file="foo.py")

extract >> ingest

pipeline = AdfPipeline(name="copyPipeline", activities=[extract, ingest], schedule="5 4 * * *")
