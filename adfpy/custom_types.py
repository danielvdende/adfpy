from azure.mgmt.datafactory.models import Expression


class AdfExpression(Expression):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, type=super.__class__.__name__)