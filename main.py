from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import BlobSource, BlobSink
from azure.mgmt.resource import ResourceManagementClient

from better_adf.activity import AdfCopyActivity, AdfDatabricksSparkPythonActivity
from better_adf.pipeline import AdfPipeline


subscription_id = "5ddf05c0-b972-44ca-b90a-3e49b5de80dd"

rg_name = "daniel-playground"

# The data factory name. It must be globally unique.
df_name = "danieladftest"

# Specify your Active Directory client ID, client secret, and tenant ID
credentials = ClientSecretCredential(client_id="0a3b9172-1c56-4337-bae9-c494d317c1ff", client_secret="bU07Q~rlxDUKKidHqlwi3MEKE8KTmUVMeqBfG", tenant_id="3d4d17ea-1ae4-4705-947e-51369c5a5f79")
resource_client = ResourceManagementClient(credentials, subscription_id)
adf_client = DataFactoryManagementClient(credentials, subscription_id)


# Create a copy activity
# act_name = 'copyBlobtoBlob'
# blob_source = BlobSource()
# blob_sink = BlobSink()
# dsin_ref = DatasetReference(reference_name='staging')
# dsOut_ref = DatasetReference(reference_name='landing')
# extract = CopyActivity(name=act_name, inputs=[dsin_ref], outputs=[
#     dsOut_ref], source=blob_source, sink=blob_sink)
extract = AdfCopyActivity(name='copyBlobToBlob',
                          input_dataset_name='staging',
                          output_dataset_name='landing',
                          source_type=BlobSource,
                          sink_type=BlobSink)

ingest = AdfDatabricksSparkPythonActivity(name="ingest",
                                          python_file="foo.py")

extract >> ingest

pipeline = AdfPipeline(name="copyPipeline",
                       activities=[extract, ingest])



# put this in CI
p = adf_client.pipelines.create_or_update(rg_name, df_name, pipeline.name, pipeline.to_adf())

# ingest = DatabricksSparkPythonActivity(name="ingest", python_file="foo.py", depends_on=[ActivityDependency(activity='copyBlobtoBlob', dependency_conditions=['Succeeded'])])
# Create a pipeline with the copy activity
# p_name = 'copyPipeline'
# params_for_pipeline = {}
# p_obj = PipelineResource(
#     activities=[extract, ingest], parameters=params_for_pipeline)

# AdfCopyActivity(name, input_dataset, output_dataset) >> AdfDatabricksSparkPythonActivity(name, python_file)