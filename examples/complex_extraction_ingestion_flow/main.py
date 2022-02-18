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
)

temp_to_landing = AdfCopyActivity(
    name="tempToLanding",
    input_dataset_name="staging",
    output_dataset_name="landing",
    source_type=BlobSource,
    sink_type=BlobSink,
)

temp_to_archive = AdfCopyActivity(
    name="tempToArchive",
    input_dataset_name="landing",
    output_dataset_name="staging",
    source_type=BlobSource,
    sink_type=BlobSink,
)

ingest = AdfDatabricksSparkPythonActivity(name="ingest", python_file="foo.py")

delete_temp_files = AdfDeleteActivity(name="delete_temp_files",
                                      dataset_name="staging",
                                      recursive=True,
                                      wildcard="foo_temp*")

delete_landing_files = AdfDeleteActivity(name="delete_landing_files",
                                         dataset_name="landing",
                                         recursive=True,
                                         wildcard="foo_landing*")


fetch >> [temp_to_archive, temp_to_landing] >> ingest >> delete_landing_files
temp_to_archive >> delete_temp_files

# TODO: allow bitshift with lists of activities
# fetch >> temp_to_landing >> ingest
# fetch >> temp_to_archive >> ingest >> delete_landing_files
# temp_to_archive >> delete_temp_files

print(temp_to_archive.depends_on)
print(delete_temp_files.depends_on)

# would be cool if we could get rid of this silly list of activities. It's a bit ott to define it twice
pipeline = AdfPipeline(
    name="complex_extraction_ingestion_flow",
    activities=[fetch, temp_to_landing, temp_to_archive, ingest, delete_temp_files, delete_landing_files],
)


# lookup

# set variable

# stored procedure
