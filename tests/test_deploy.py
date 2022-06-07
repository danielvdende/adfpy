from unittest import mock

from adfpy import deploy as victim
from adfpy.pipeline import AdfPipeline


@mock.patch("adfpy.deploy.fetch_existing_pipelines", return_value=["foo"])
@mock.patch("adfpy.deploy.processed_pipelines_names", [])
def test_remove_stale_pipelines(_):
    m_adf_client = mock.Mock()
    conf_adf_client = victim.ConfiguredDataFactory(resource_group="foo", name="bar", client=m_adf_client)
    victim.remove_stale_pipelines(conf_adf_client)
    m_adf_client.pipelines.delete.assert_called_once_with(resource_group_name="foo",
                                                          factory_name="bar",
                                                          pipeline_name="foo")


@mock.patch("adfpy.deploy.fetch_existing_pipelines", return_value=["foo"])
@mock.patch("adfpy.deploy.processed_pipelines_names", ["foo"])
def test_remove_stale_pipelines_nothing_to_remove(_):
    m_adf_client = mock.Mock()
    conf_adf_client = victim.ConfiguredDataFactory(resource_group="foo", name="bar", client=m_adf_client)
    victim.remove_stale_pipelines(conf_adf_client)
    m_adf_client.pipelines.delete.assert_not_called()


def test_create_or_update_pipeline_no_dependencies():
    m_adf_client = mock.Mock()
    conf_adf_client = victim.ConfiguredDataFactory(resource_group="foo", name="bar", client=m_adf_client)
    pipeline = AdfPipeline(name="foo")

    victim.create_or_update_pipeline(conf_adf_client, pipeline)

    m_adf_client.pipelines.create_or_update.assert_called_once_with("foo", "bar", "foo", pipeline.to_adf())

    m_adf_client.triggers.create_or_update.assert_not_called()


def test_create_or_update_pipeline_with_dependencies():
    m_adf_client = mock.Mock()
    conf_adf_client = victim.ConfiguredDataFactory(resource_group="foo", name="bar", client=m_adf_client)
    child_pipeline = AdfPipeline(name="child")
    parent_pipeline = AdfPipeline(name="parent", depends_on_pipelines=[child_pipeline])

    victim.create_or_update_pipeline(conf_adf_client, parent_pipeline)

    # poor man's check if recursion worked correctly
    assert m_adf_client.pipelines.create_or_update.call_count == 2

    m_adf_client.triggers.create_or_update.assert_not_called()


def test_create_or_update_pipeline_preset_definition_schedule():
    m_adf_client = mock.Mock()
    conf_adf_client = victim.ConfiguredDataFactory(resource_group="foo", name="bar", client=m_adf_client)
    pipeline = AdfPipeline(name="foo", schedule="@daily")

    victim.create_or_update_pipeline(conf_adf_client, pipeline)

    m_adf_client.pipelines.create_or_update.assert_called_once_with("foo", "bar", "foo", pipeline.to_adf())

    m_adf_client.triggers.create_or_update.assert_called_with("foo", "bar", "foo-trigger", pipeline.schedule.to_adf())
