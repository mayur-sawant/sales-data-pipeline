"""Custom exceptions with clear, actionable error messages."""


class PipelineError(Exception):
    """Base class for all pipeline errors."""


class JavaNotFoundError(PipelineError):
    """Raised when no Java runtime is available for PySpark."""


class SparkInitializationError(PipelineError):
    """Raised when the SparkSession cannot be created."""


class InputDataError(PipelineError):
    """Raised when the raw input file is missing, empty or unreadable."""


class SchemaValidationError(PipelineError):
    """Raised when the input data does not contain the required columns."""


class DataValidationError(PipelineError):
    """Raised when processed data violates a business rule."""
