from unittest import mock

from adfpy import deploy as victim


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
