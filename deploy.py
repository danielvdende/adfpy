import importlib.util
import os
from pathlib import Path
from typing import Set

from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.resource import ResourceManagementClient

from better_adf.pipeline import AdfPipeline

subscription_id = "5ddf05c0-b972-44ca-b90a-3e49b5de80dd"
rg_name = "daniel-playground"
df_name = "danieladftest"
credentials = ClientSecretCredential(
    client_id="0a3b9172-1c56-4337-bae9-c494d317c1ff",
    client_secret=os.getenv("SP_SECRET"),
    tenant_id="3d4d17ea-1ae4-4705-947e-51369c5a5f79",
)
resource_client = ResourceManagementClient(credentials, subscription_id)
adf_client = DataFactoryManagementClient(credentials, subscription_id)

adf_pipelines_path = Path("/Users/daniel/code/opensource/better_adf/examples/")

# 1. fetch existing pipelines from ADF
existing_pipelines = [
    x.name for x in adf_client.pipelines.list_by_factory(resource_group_name=rg_name, factory_name=df_name)
]


# 2. Retrieve all pipeline objects from path (which can be a dir)
def load_pipelines_from_file(file_path):
    mod_spec = importlib.util.spec_from_file_location(
        "main_foo",
        file_path,
    )
    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)
    return set(var for var in vars(module).values() if isinstance(var, AdfPipeline))


def load_pipelines_from_path(path: Path, pipelines: Set[AdfPipeline] = None) -> Set[AdfPipeline]:
    if not pipelines:
        pipelines = set()
    child_elements = path.glob("**/*.py")
    for el in child_elements:
        if el.is_dir():
            load_pipelines_from_path(el, pipelines)
        else:
            pipelines = pipelines.union(load_pipelines_from_file(el))
    return pipelines


pipelines = load_pipelines_from_path(adf_pipelines_path)


def create_pipeline(p: AdfPipeline):
    # TODO: hackaround with required pipelines. Might want to revisit this
    for required_pipeline in p.depends_on_pipelines:
        if required_pipeline.name not in pipelines_names:
            create_pipeline(required_pipeline)
    print(f"Creating/updating {p.name}")
    adf_client.pipelines.create_or_update(rg_name, df_name, p.name, p.to_adf())
    pipelines_names.append(p.name)


# 3. Create/Update pipelines

# TODO: move this over to use the set for uniqueness

pipelines_names = []
for p in pipelines:
    if p.name not in pipelines_names:
        create_pipeline(p)
print(pipelines_names)

# 4. Fetch pipelines from ADF again. Anything not in path, but in adf should be removed
existing_pipelines = [
    x.name for x in adf_client.pipelines.list_by_factory(resource_group_name=rg_name, factory_name=df_name)
]

for p in existing_pipelines:
    if p not in pipelines_names:
        print(f"Pipeline {p} no longer exists. Deleting from ADF")
        adf_client.pipelines.delete(resource_group_name=rg_name, factory_name=df_name, pipeline_name=p)
