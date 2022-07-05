from adfpy.activity import AdfActivity  


class TestAdfActivity:
    def test_right_shift(self):
        act1 = AdfActivity("act1")
        act2 = AdfActivity("act2")

        act1 >> act2

        assert act2.depends_on == {"act1": ["Succeeded"]}

    def test_left_shift(self):
        act1 = AdfActivity("act1")
        act2 = AdfActivity("act2")

        act1 << act2

        assert act1.depends_on == {"act2": ["Succeeded"]}

    def test_left_shift_list(self):
        act1 = AdfActivity("act1")
        act2 = AdfActivity("act2")

        act1 << [act2]

        assert act1.depends_on == {"act2": ["Succeeded"]}

    def test_right_shift_list(self):
        act1 = AdfActivity("act1")
        act2 = AdfActivity("act2")

        act1 >> [act2]

        assert act2.depends_on == {"act1": ["Succeeded"]}

    def test_adding_dependency_default_conditions(self):
        act = AdfActivity("act")
        act.add_dependency("foo")

        assert act.depends_on == {"foo": ["Succeeded"]}

    def test_adding_dependency_custom_conditions(self):
        act = AdfActivity("act")
        act.add_dependency("foo", ["Failed"])

        assert act.depends_on == {"foo": ["Failed"]}
