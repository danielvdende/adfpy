import argparse
import importlib.util
import os
from pathlib import Path
from typing import Set, List

from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

from adfpy.pipeline import AdfPipeline


def fetch_existing_pipelines(adf_client: DataFactoryManagementClient,
                             resource_group: str,
                             data_factory: str) -> List[str]:
    """

    Args:
        adf_client:
        resource_group:
        data_factory:

    Returns:

    """
    return [
        x.name for x in adf_client.pipelines.list_by_factory(resource_group_name=resource_group,
                                                             factory_name=data_factory)
    ]


def load_pipelines_from_file(file_path: Path) -> Set[AdfPipeline]:
    """
        # 2. Retrieve all pipeline objects from path (which can be a dir)

    Args:
        file_path:

    Returns:

    """
    mod_spec = importlib.util.spec_from_file_location(
        "main_foo",
        file_path,
    )
    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)
    return set(var for var in vars(module).values() if isinstance(var, AdfPipeline))


def load_pipelines_from_path(path: Path, pipelines: Set[AdfPipeline] = None) -> Set[AdfPipeline]:
    """

    Args:
        path:
        pipelines:

    Returns:

    """
    if not pipelines:
        pipelines = set()
    child_elements = path.glob("**/*.py")
    for el in child_elements:
        if el.is_dir():
            load_pipelines_from_path(el, pipelines)
        else:
            pipelines = pipelines.union(load_pipelines_from_file(el))
    return pipelines


def create_or_update_pipeline(adf_client: DataFactoryManagementClient,
                              resource_group: str,
                              data_factory: str,
                              pipeline: AdfPipeline):
    """

    Args:
        adf_client:
        resource_group:
        data_factory:
        pipeline:

    Returns:

    """
    for required_pipeline in pipeline.depends_on_pipelines:
        # We only process a required pipeline if it hasn't been processed before
        if required_pipeline.name not in processed_pipelines_names:
            create_or_update_pipeline(adf_client, resource_group, data_factory, required_pipeline)
    print(f"Creating/updating {pipeline.name}")
    adf_client.pipelines.create_or_update(resource_group, data_factory, pipeline.name, pipeline.to_adf())
    processed_pipelines_names.append(pipeline.name)


def ensure_all_pipelines_up_to_date(pipelines: Set[AdfPipeline],
                                    adf_client: DataFactoryManagementClient,
                                    resource_group: str,
                                    data_factory: str):
    """

    Args:
        pipelines:
        adf_client:
        resource_group:
        data_factory:

    Returns:

    """
    for p in pipelines:
        if p.name not in processed_pipelines_names:
            create_or_update_pipeline(adf_client, resource_group, data_factory, p)


def remove_stale_pipelines(adf_client: DataFactoryManagementClient, resource_group: str, data_factory: str):
    """

    Args:
        adf_client:
        resource_group:
        data_factory:

    Returns:

    """
    existing_pipelines = fetch_existing_pipelines(adf_client, resource_group, data_factory)
    print(existing_pipelines)
    print(processed_pipelines_names)
    for p in existing_pipelines:
        if p not in processed_pipelines_names:
            print(f"Pipeline {p} no longer exists. Deleting from ADF")
            adf_client.pipelines.delete(resource_group_name=resource_group, factory_name=data_factory, pipeline_name=p)


# global variable to store which pipelines have been processed
processed_pipelines_names = []


def run_deployment():
    parser = argparse.ArgumentParser(description='Parse input parameters')
    parser.add_argument('--pipelines_path', type=Path, dest="pipelines_path", help='Path containing AdfPy pipelines')
    args = parser.parse_args()

    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    resource_group = os.environ["AZURE_RESOURCE_GROUP_NAME"]
    data_factory = os.environ["AZURE_DATA_FACTORY_NAME"]
    credentials = ClientSecretCredential(
        client_id=os.environ["AZURE_SERVICE_PRINCIPAL_CLIENT_ID"],
        client_secret=os.environ["AZURE_SERVICE_PRINCIPAL_SECRET"],
        tenant_id=os.environ["AZURE_TENANT_ID"],
    )
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    pipelines = load_pipelines_from_path(args.pipelines_path)

    ensure_all_pipelines_up_to_date(pipelines, adf_client, resource_group, data_factory)

    remove_stale_pipelines(adf_client, resource_group, data_factory)


if __name__ == "__main__":
    run_deployment()
