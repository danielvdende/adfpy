class AdfPyException(Exception):
    """
    Top-level AdfPyException
    """
    pass


class NotSupportedError(AdfPyException):
    pass


class InvalidCronExpressionError(AdfPyException):
    pass


class PipelineModuleParseException(AdfPyException):
    pass
