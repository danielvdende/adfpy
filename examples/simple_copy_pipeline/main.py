from azure.mgmt.datafactory.models import BlobSource, BlobSink

from better_adf.activities.execution import AdfCopyActivity, AdfDatabricksSparkPythonActivity
from better_adf.pipeline import AdfPipeline

extract = AdfCopyActivity(
    name="copyBlobToBlob",
    input_dataset_name="staging",
    output_dataset_name="landing",
    source_type=BlobSource,
    sink_type=BlobSink,
)

ingest = AdfDatabricksSparkPythonActivity(name="ingest", python_file="foo.py")

extract >> ingest

pipeline = AdfPipeline(name="copyPipeline", activities=[extract, ingest])
