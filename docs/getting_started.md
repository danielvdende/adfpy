# Getting Started

## Installation
Installation of adfPy is simple:
```shell
pip install adfpy
```

This will give you:

 - The adfPy classes and functions you need to build awesome pipelines.
 - A CLI command: `adfpy-deploy` which parses your adfPy resource definitions and ensures they are correctly configured in your ADF instance.

## Writing your first adfPy pipeline
For those of you familiar with Airflow, the syntax will (hopefully) look quite similar. To create a simple pipeline in ADF, the following adfPy code can be used:
```python
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

pipeline = AdfPipeline(name="copyPipeline", activities=[extract, ingest], schedule="* * 5 * *")
```

There are a few noteworthy things here:
1. All adfPy resources are prefixed with `Adf`. Other than that, the aim is to keep the naming as much in line with ADF as possible to ensure it's clear what kind of Activity will be created in ADF when you deploy.
2. Dependencies between activities can be set using the bitshift operators (`>>` and `<<`). They work exactly the same as they do in [Airflow](https://airflow.apache.org/docs/apache-airflow/1.10.5/concepts.html?highlight=trigger%20rule#bitshift-composition).
3. As in Airflow, you can define the execution schedule as an attribute of the Pipeline. 

This code is available as an example in the [examples directory](https://github.com/danielvdende/adfpy/tree/main/examples) in the adfPy repository along with more complex cases.