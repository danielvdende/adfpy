from azure.mgmt.datafactory.models import BlobSource, BlobSink

from better_adf.activities.control import (
    AdfExecutePipelineActivity,
    AdfIfConditionActivity,
    AdfForEachActivity,
)
from better_adf.activities.execution import (
    AdfCopyActivity,
    AdfDeleteActivity,
    AdfDatabricksSparkPythonActivity,
)
from better_adf.pipeline import AdfPipeline

pipeline = AdfPipeline(
    name="complex_extraction_ingestion_flow"
)

fetch = AdfForEachActivity(
    name="fetch",
    items="@variables('foo')",
    activities=[
        AdfIfConditionActivity(
            name="if_foo",
            expression="",
            if_false_activities=[AdfExecutePipelineActivity(name="run_copyPipeline", pipeline_name="copyPipeline")],
            if_true_activities=[AdfExecutePipelineActivity(name="run_copyPipeline", pipeline_name="copyPipeline")],
        )
    ],
    pipeline=pipeline
)

temp_to_landing = AdfCopyActivity(
    name="tempToLanding",
    input_dataset_name="staging",
    output_dataset_name="landing",
    source_type=BlobSource,
    sink_type=BlobSink,
    pipeline=pipeline
)

temp_to_archive = AdfCopyActivity(
    name="tempToArchive",
    input_dataset_name="landing",
    output_dataset_name="staging",
    source_type=BlobSource,
    sink_type=BlobSink,
    pipeline=pipeline
)

ingest = AdfDatabricksSparkPythonActivity(name="ingest", python_file="foo.py", pipeline=pipeline)

delete_temp_files = AdfDeleteActivity(name="delete_temp_files",
                                      dataset_name="staging",
                                      recursive=True,
                                      wildcard="foo_temp*",
                                      pipeline=pipeline)

delete_landing_files = AdfDeleteActivity(name="delete_landing_files",
                                         dataset_name="landing",
                                         recursive=True,
                                         wildcard="foo_landing*",
                                         pipeline=pipeline)


fetch >> [temp_to_archive, temp_to_landing] >> ingest >> delete_landing_files
delete_temp_files.add_dependencies({
    temp_to_landing.name: ["Succeeded", "Skipped"],
    temp_to_archive.name: ["Succeeded", "Skipped"]
})


# lookup

# set variable

# stored procedure
