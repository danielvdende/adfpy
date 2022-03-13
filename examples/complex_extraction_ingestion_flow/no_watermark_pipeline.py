from azure.mgmt.datafactory.models import BlobSource, BlobSink


from better_adf.activities.control import AdfSetVariableActivity
from better_adf.activities.execution import AdfCopyActivity
from better_adf.pipeline import AdfPipeline

no_watermark_pipeline = AdfPipeline(
    name="complex_extraction_ingestion_flow_no_watermark",
)

set_no_watermark_query = AdfSetVariableActivity(
    name="noWatermarkQuery", value="select * from source", pipeline=no_watermark_pipeline
)

table_to_temp = AdfCopyActivity(
    name="copyTableToTemp",
    input_dataset_name="landing",
    output_dataset_name="staging",
    source_type=BlobSource,
    sink_type=BlobSink,
    pipeline=no_watermark_pipeline,
)

set_no_watermark_query >> table_to_temp
