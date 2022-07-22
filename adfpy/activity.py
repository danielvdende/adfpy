import abc
from typing import List, Union, Dict


class AdfActivity:
    depends_on: Dict

    # String as type hint to avoid a circular import
    def __init__(self, name: str, pipeline: "AdfPipeline" = None):  # type: ignore # noqa: F821
        self.name = name
        self.depends_on = dict()
        if pipeline:
            pipeline.activities.append(self)

    def add_dependency(self, activity_name: str, dependency_conditions: List[str] = None):
        if not dependency_conditions:
            dependency_conditions = ["Succeeded"]
        self.depends_on[activity_name] = dependency_conditions

    def add_dependencies(self, activities: Dict[str, List[str]]):
        for activity_name, dependency_conditions in activities.items():
            self.add_dependency(activity_name, dependency_conditions)

    def __rshift__(self, other: Union["AdfActivity", List["AdfActivity"]]):
        """Implements Activity >> Activity"""
        if isinstance(other, List):
            for x in other:
                x.add_dependency(self.name)
        else:
            other.add_dependency(self.name)
        return other

    def __lshift__(self, other: "AdfActivity"):
        """Implements Activity << Activity"""
        if isinstance(other, List):
            for x in other:
                self.add_dependency(x.name)
        else:
            self.add_dependency(other.name)
        return other

    def __rrshift__(self, other: "AdfActivity"):
        """Called for Activity << [Activity] because list don't have __rshift__ operators."""
        self.__lshift__(other)
        return self

    def __rlshift__(self, other: "AdfActivity"):
        """Called for Activity >> [Activity] because list don't have __lshift__ operators."""
        self.__rshift__(other)
        return self

    @abc.abstractmethod
    def to_adf(self):
        pass
