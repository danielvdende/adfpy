# Pipelines

Pipelines are one of the fundamental building blocks of both ADF and adfPy. To create an AdfPipeline:
```python
from adfpy.pipeline import AdfPipeline

pipeline = AdfPipeline(name="copyPipeline", activities=[extract, ingest], schedule="* * 5 * *")
```
The only required argument for an AdfPipeline is its name. Adding activities to a AdfPipeline can be done in 2 ways:
1. (As above) pass in a list of AdfActivity objects. 
2. Specify the pipeline object as a parameter when creating each AdfActivity (shown below)
```python
from adfpy.activities.execution import AdfCopyActivity, AdfDatabricksSparkPythonActivity

extract = AdfCopyActivity(
    name="copyBlobToBlob",
    input_dataset_name="staging",
    output_dataset_name="landing",
    source_type=BlobSource,
    sink_type=BlobSink,
    pipeline=pipeline
)

ingest = AdfDatabricksSparkPythonActivity(name="ingest", python_file="foo.py", pipeline=pipeline)
```
Both approaches will give you exactly the same result when you deploy to ADF.

Note also that instead of specifying a separate `Trigger` resource (which is the approach ADF takes), adfPy allows you to set this on the adfPipeline object. For more information on this, take a look at the [Triggers](triggers.md) page.

## Setting dependencies between activities
Setting dependencies between activities in adfPy is very similar to how Airflow does this using the bitshift operators:
```python
activity1 >> activity2
```
will create a dependency on `activity1` for `activity2`. This can also be used with lists, as in Airflow:
```python
activity1 >> [activity2, activity3] >> activity4
```
is equivalent to
```python
activity1 >> activity2 >> activity4
activity1 >> activity3 >> activity4
```

The implicit assumption here is that the dependency condition between the activities is `Succeeded`. This means that in the previous examples `activity2` will only execute if `activity1` completed successfully. If you want to deviate from this, for example to define failure handling, you can explicitly set dependencies for an activity:
```python
activity2.add_dependency(activity_name="activity1", dependency_conditions=["Failed"])
```
Possible dependency conditions are the same as in ADF:

- Succeeded
- Failed
- Skipped
- Completed
