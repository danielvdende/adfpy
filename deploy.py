# 0. setup client
import importlib.util
import os

from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.resource import ResourceManagementClient

from better_adf.pipeline import AdfPipeline

subscription_id = "5ddf05c0-b972-44ca-b90a-3e49b5de80dd"
rg_name = "daniel-playground"
df_name = "danieladftest"

credentials = ClientSecretCredential(client_id="0a3b9172-1c56-4337-bae9-c494d317c1ff", client_secret="", tenant_id="3d4d17ea-1ae4-4705-947e-51369c5a5f79")
resource_client = ResourceManagementClient(credentials, subscription_id)
adf_client = DataFactoryManagementClient(credentials, subscription_id)

# 1. fetch existing pipelines from ADF
existing_pipelines = [x.name for x in adf_client.pipelines.list_by_factory(resource_group_name=rg_name, factory_name=df_name)]

# 2. Retrieve all pipeline objects from path

# module_name, _ = os.path.splitext(dag_file)
# module_path = os.path.join(DAG_PATH, dag_file)
# mod_spec = importlib.util.spec_from_file_location(main, module_path)
# module = importlib.util.module_from_spec(mod_spec)
mod_spec = importlib.util.spec_from_file_location("main", "/Users/danielvanderende/code/gdd/adf_python/main.py")
module = importlib.util.module_from_spec(mod_spec)
mod_spec.loader.exec_module(module)
# objs = importlib.import_module('adf_python.main')
pipelines = [var for var in vars(module).values() if isinstance(var, AdfPipeline)]

# 3. Create/Update pipelines
pipelines_names = []
for p in pipelines:
    print(f"Creating/updating {p.name}")
    adf_client.pipelines.create_or_update(rg_name, df_name, p.name, p.to_adf())
    pipelines_names.append(p.name)

# 4. Fetch pipelines from ADF again. Anything not in path, but in adf should be removed
existing_pipelines = [x.name for x in adf_client.pipelines.list_by_factory(resource_group_name=rg_name, factory_name=df_name)]

for p in existing_pipelines:
    if p not in pipelines_names:
        print(f"Pipeline {p} no longer exists. Deleting from ADF")
        adf_client.pipelines.delete(resource_group_name=rg_name,
                                    factory_name=df_name,
                                    pipeline_name=p)



