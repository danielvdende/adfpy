# TODO: we should abstract this away, it's not nice to have to deal with these models
from azure.mgmt.datafactory.models import BlobSource, BlobSink

from adfpy.activities.control import (
    AdfExecutePipelineActivity,
    AdfIfConditionActivity,
    AdfForEachActivity,
    AdfSetVariableActivity,
)
from adfpy.activities.execution import (
    AdfCopyActivity,
    AdfDeleteActivity,
    AdfDatabricksSparkPythonActivity,
)
from adfpy.pipeline import AdfPipeline
from examples.complex_extraction_ingestion_flow.no_watermark_pipeline import (
    no_watermark_pipeline,
)
from examples.complex_extraction_ingestion_flow.watermark_pipeline import (
    watermark_pipeline,
)

parent_pipeline = AdfPipeline(
    name="complex_extraction_ingestion_flow",
    depends_on_pipelines={watermark_pipeline, no_watermark_pipeline},
)

fetch = AdfForEachActivity(
    name="fetch",
    items="@variables('foo')",
    activities=[
        AdfIfConditionActivity(
            name="if_foo",
            expression="",
            if_false_activities=[
                AdfExecutePipelineActivity(
                    name="run_no_watermark",
                    pipeline_name="complex_extraction_ingestion_flow_no_watermark",
                )
            ],
            if_true_activities=[
                AdfExecutePipelineActivity(
                    name="run_watermark",
                    pipeline_name="complex_extraction_ingestion_flow_watermark",
                )
            ],
        ),
        AdfSetVariableActivity("foo", "bar"),
    ],
    pipeline=parent_pipeline,
)

temp_to_landing = AdfCopyActivity(
    name="tempToLanding",
    input_dataset_name="staging",
    output_dataset_name="landing",
    source_type=BlobSource,
    sink_type=BlobSink,
    pipeline=parent_pipeline,
)

temp_to_archive = AdfCopyActivity(
    name="tempToArchive",
    input_dataset_name="landing",
    output_dataset_name="staging",
    source_type=BlobSource,
    sink_type=BlobSink,
    pipeline=parent_pipeline,
)

ingest = AdfDatabricksSparkPythonActivity(name="ingest", python_file="foo.py", pipeline=parent_pipeline)

delete_temp_files = AdfDeleteActivity(
    name="delete_temp_files",
    dataset_name="staging",
    recursive=True,
    wildcard="foo_temp*",
    pipeline=parent_pipeline,
)

delete_landing_files = AdfDeleteActivity(
    name="delete_landing_files",
    dataset_name="landing",
    recursive=True,
    wildcard="foo_landing*",
    pipeline=parent_pipeline,
)


fetch >> [temp_to_archive, temp_to_landing] >> ingest >> delete_landing_files
delete_temp_files.add_dependencies(
    {
        temp_to_landing.name: ["Succeeded", "Skipped"],
        temp_to_archive.name: ["Succeeded", "Skipped"],
    }
)
