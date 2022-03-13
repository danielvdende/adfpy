from azure.mgmt.datafactory.models import BlobSource, BlobSink


from pyadf.activities.control import AdfSetVariableActivity, AdfIfConditionActivity
from pyadf.activities.execution import (
    AdfCopyActivity,
    AdfLookupActivity,
    AdfSqlServerStoredProcedureActivity,
)
from pyadf.pipeline import AdfPipeline

watermark_pipeline = AdfPipeline(
    name="complex_extraction_ingestion_flow_watermark",
)

set_hash_content = AdfSetVariableActivity(
    name="hashContent", value="select * from source", pipeline=watermark_pipeline
)

set_watermark_query = AdfSetVariableActivity(
    name="watermarkQuery", value="select * from source", pipeline=watermark_pipeline
)

previous_watermark = AdfLookupActivity(
    name="previousWatermark", dataset="landing", source=BlobSource, pipeline=watermark_pipeline
)

current_watermark = AdfLookupActivity(
    name="currentWatermark", dataset="staging", source=BlobSource, pipeline=watermark_pipeline
)

table_to_temp = AdfCopyActivity(
    name="copyTableToTemp",
    input_dataset_name="landing",
    output_dataset_name="staging",
    source_type=BlobSource,
    sink_type=BlobSink,
    pipeline=watermark_pipeline,
)

watermark_update_condition = AdfIfConditionActivity(
    name="row_count_gt_0",
    expression="@greater(activity('table_to_temp').output. rowsCopied, 0)",
    if_false_activities=[],
    if_true_activities=[
        AdfSqlServerStoredProcedureActivity(
            name="watermark_name",
            stored_procedure_name="epic_stored_proc",
            linked_service="AzureBlobStorage1",
        )
    ],
    pipeline=watermark_pipeline,
)

set_hash_content >> previous_watermark >> set_watermark_query >> table_to_temp >> watermark_update_condition
current_watermark >> set_watermark_query
