from azure.mgmt.datafactory.models import BlobSource, BlobSink

from adfpy.activities.execution import AdfCopyActivity, AdfDatabricksSparkPythonActivity
from adfpy.pipeline import AdfPipeline
from adfpy.datasets.file_based import AdfBinaryDataset

staging_dataset = AdfBinaryDataset(
    name="generated_staging",
    linked_service_name="AzureBlobStorage1",
    storage_type="AzureBlob",
    container_name="metaflow",
    path="foo/bar/*.baz"
)

landing_dataset = AdfBinaryDataset(
    name="generated_landing",
    linked_service_name="AzureBlobStorage1",
    storage_type="AzureBlob",
    container_name="metaflow",
    path="tralalala"
)

extract = AdfCopyActivity(
    name="copyBlobToBlob",
    input_dataset=staging_dataset,
    output_dataset="landing",
    source_type=BlobSource,
    sink_type=BlobSink,
)

ingest = AdfDatabricksSparkPythonActivity(name="ingest", python_file="foo.py")

extract >> ingest

pipeline = AdfPipeline(name="copyPipeline", activities=[extract, ingest], schedule="* * 5 * *")
