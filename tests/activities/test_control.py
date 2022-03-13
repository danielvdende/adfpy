from better_adf.activities import control as victim

from azure.mgmt.datafactory.models import (
    ForEachActivity,
    SetVariableActivity,
    Expression,
    ActivityDependency
)


class TestForEachActivity:
    def test_single_activity(self):
        activity = victim.AdfForEachActivity(
            name="foobar",
            items="@variables('foo')",
            activities=[victim.AdfSetVariableActivity("foo", "bar")],
        )

        adf_activity = activity.to_adf()
        expected_result = ForEachActivity(
                name="foobar",
                items=Expression(value="@variables('foo')"),
                activities=[SetVariableActivity(name="foo", value="bar", depends_on=[])],
                depends_on=[],
            )
        assert adf_activity == expected_result

    def test_multiple_activities(self):
        activity = victim.AdfForEachActivity(
            name="foobar",
            items="@variables('foo')",
            activities=[victim.AdfSetVariableActivity("foo", "bar"), victim.AdfSetVariableActivity("baz", "qux")],
        )

        adf_activity = activity.to_adf()
        expected_result = ForEachActivity(
            name="foobar",
            items=Expression(value="@variables('foo')"),
            activities=[SetVariableActivity(name="foo", value="bar", depends_on=[]), SetVariableActivity(name="baz", value="qux", depends_on=[ActivityDependency(activity="foo", dependency_conditions=["Succeeded"])])],
            depends_on=[],
        )

        assert adf_activity == expected_result
