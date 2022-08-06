import importlib.util
import logging
import os
import sys

import click

from dataclasses import dataclass
from pathlib import Path
from typing import Set, List

from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient  # type: ignore

from adfpy.error import PipelineModuleParseException
from adfpy.pipeline import AdfPipeline

stdout_handler = logging.StreamHandler(stream=sys.stdout)
handlers = [stdout_handler]
logging.basicConfig(
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=handlers
)

logger = logging.getLogger("adfPy")
logger.setLevel(os.getenv("LOG_LEVEL", logging.INFO))


@dataclass
class ConfiguredDataFactory:
    resource_group: str
    name: str
    client: DataFactoryManagementClient


# global variable to store which pipelines have been processed
processed_pipelines_names = []


def fetch_existing_pipelines(adf: ConfiguredDataFactory) -> List[str]:
    """Fetch existing pipelines from the Azure Data Factory instance

    The pipelines are represented only by their names as this is sufficient for further usage.

    Args:
        adf: authorized data factory client

    Returns:
        List of pipeline names found in the configured Azure Data Factory
    """
    return [
        x.name for x in adf.client.pipelines.list_by_factory(resource_group_name=adf.resource_group,
                                                             factory_name=adf.name)
    ]


def load_pipelines_from_file(file_path: Path) -> Set[AdfPipeline]:
    """Load all AdfPipeline objects from a file

    This function only works with single files. Raises IsADirectoryError if the provided path is a directory

    Args:
        file_path:

    Raises: IsADirectoryError if the provided path is a directory

    Returns:
        Set of AdfPipeline objects found in the provided file
    """
    if file_path.is_dir():
        raise IsADirectoryError("load_pipelines_from_file is not intended to load pipelines from a directory."
                                "Please use load_pipelines_from_path instead")
    mod_spec = importlib.util.spec_from_file_location(
        "main",
        file_path,
    )
    if mod_spec:
        module = importlib.util.module_from_spec(mod_spec)
        mod_spec.loader.exec_module(module)  # type: ignore
        return set(var for var in vars(module).values() if isinstance(var, AdfPipeline))
    else:
        raise PipelineModuleParseException(f"Could not parse module spec from path {file_path}")


def load_pipelines_from_path(path: Path, pipelines: Set[AdfPipeline] = None) -> Set[AdfPipeline]:
    """Load all AdfPipeline objects from a given path.

    The path can be either a directory or a file. If it's a directory, the function will recursively step through
    the directory structure and look for all `.py` files. The pipelines parameter of this function is used for this
    recursion.

    Args:
        path: Path to scan for AdfPipeline objects
        pipelines: Set of AdfPipelines found. Used for recursively adding pipelines as they are found

    Returns:
        The final set of AdfPipelines found in the path
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


def create_or_update_pipeline(adf: ConfiguredDataFactory,
                              pipeline: AdfPipeline,
                              dry_run: bool = False):
    """Create or update a pipeline in Azure Data Factory based on the provided pipeline definition

    This function will also ensure that any pipelines that the provided pipeline depends on are created first. To
    achieve this, it makes use of the `processed_pipelines_names` global variable. After creating/updating a pipeline,
    this function will ensure that pipeline is added to this global, so it will not be created/updated again.

    Args:
        adf: ConfiguredDataFactory object
        pipeline: AdfPipeline object based on provided configuration
        dry_run: boolean indicating whether the actions are to be executed as a dry-run. Defaults to False.
    """
    for required_pipeline in pipeline.depends_on_pipelines:
        # We only process a required pipeline if it hasn't been processed before
        if required_pipeline.name not in processed_pipelines_names:
            create_or_update_pipeline(adf, required_pipeline, dry_run)
    logger.info(f"Creating/updating pipeline {pipeline.name}")
    if not dry_run:
        adf.client.pipelines.create_or_update(adf.resource_group, adf.name, pipeline.name, pipeline.to_adf())
    if pipeline.schedule:
        logger.info(f"Creating/updating trigger for {pipeline.name}")
        if not dry_run:
            adf.client.triggers.create_or_update(adf.resource_group,
                                                 adf.name,
                                                 pipeline.schedule.name,
                                                 pipeline.schedule.to_adf())
    processed_pipelines_names.append(pipeline.name)


def ensure_all_pipelines_up_to_date(pipelines: Set[AdfPipeline],
                                    adf: ConfiguredDataFactory,
                                    dry_run: bool = False):
    """Create or update pipelines in ADF based on a provided set of pipelines.

    This function checks with the global `processed_pipelines_names` variable to avoid duplicate processing.
    There is no functional requirement for this (i.e. it would be ok to process the same pipeline multiple times), but
    it's not very nice.

    Args:
        pipelines: Set of AdfPipeline objects retrieved from the local path
        adf: ConfiguredDataFactory object
        dry_run: boolean indicating whether the actions are to be executed as a dry-run. Defaults to False.
    """
    for p in pipelines:
        if p.name not in processed_pipelines_names:
            create_or_update_pipeline(adf, p, dry_run)


def remove_stale_pipelines(adf: ConfiguredDataFactory, dry_run: bool = False):
    """Removes any pipelines that are (no longer) available in the configured (local) pipeline path

    This function is destructive, as any pipeline not managed by adfPy will be removed.

    Args:
        adf: ConfiguredDataFactory object
        dry_run: boolean indicating whether the actions are to be executed as a dry-run. Defaults to False.
    """
    existing_pipelines = fetch_existing_pipelines(adf)
    for p in existing_pipelines:
        if p not in processed_pipelines_names:
            logger.info(f"Deleting pipeline {p} from ADF. Pipeline {p} no longer exists in path")
            if not dry_run:
                adf.client.pipelines.delete(resource_group_name=adf.resource_group,
                                            factory_name=adf.name,
                                            pipeline_name=p)


def configure_data_factory() -> ConfiguredDataFactory:
    """Configure your data factory, based mostly on environment variables

    Returns:
        A ConfiguredDataFactory object, with all required fields for interacting with ADF
    """
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    resource_group = os.environ["AZURE_RESOURCE_GROUP_NAME"]
    data_factory = os.environ["AZURE_DATA_FACTORY_NAME"]
    credentials = ClientSecretCredential(
        client_id=os.environ["AZURE_SERVICE_PRINCIPAL_CLIENT_ID"],
        client_secret=os.environ["AZURE_SERVICE_PRINCIPAL_SECRET"],
        tenant_id=os.environ["AZURE_TENANT_ID"],
    )
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    return ConfiguredDataFactory(resource_group, data_factory, adf_client)


@click.command()
@click.option('--path', required=True, type=Path, help='Path containing adfPy resources')
@click.option('--delete-stale-resources/--no-delete-stale-resources', type=bool, default=True,
              help="Flag indicating whether or not to remove resources from ADF that are not available in the "
                   "configured path. Defaults to False")
@click.option("--dry-run", type=bool, is_flag=True, default=False, help="Execute the deployment as a dry-run or not. If"
                                                                        "enabled, the deploy script will only output "
                                                                        "what will be changed rather than actually "
                                                                        "executing the required action")
def run_deployment(path, delete_stale_resources, dry_run):
    """Deploy your adfPy resources to ADF

    This tool deploys your adfPy resources. For authentication, you should set a number of
    environment variables:

    \b
    AZURE_SUBSCRIPTION_ID
    AZURE_RESOURCE_GROUP_NAME
    AZURE_DATA_FACTORY_NAME
    AZURE_SERVICE_PRINCIPAL_CLIENT_ID
    AZURE_SERVICE_PRINCIPAL_SECRET
    AZURE_TENANT_ID
    """
    configured_adf = configure_data_factory()
    logger.info("Welcome to adfPy!")
    logger.info(f"Starting up deployment to factory: {configured_adf.name}")
    if dry_run:
        logger.info("Dry run enabled. All changes below will not be executed")

    pipelines = load_pipelines_from_path(path)
    logger.info(f"Loaded {len(pipelines)} pipelines")

    ensure_all_pipelines_up_to_date(pipelines, configured_adf, dry_run)

    if delete_stale_resources:
        remove_stale_pipelines(configured_adf, dry_run)


if __name__ == "__main__":
    run_deployment()
