"""
pipeline/base_datasource.py
─────────────────────────────────────────────────────────────────────────────
Abstract Base Class for all data sources in the ETL pipeline.

TEACHING NOTE — WHY AN ABSTRACT BASE CLASS?
  Python's `abc` module lets you define a contract: any class that claims
  to be a DataSource MUST implement the `read()` method. If a subclass
  forgets to implement it, Python raises a TypeError at instantiation time —
  catching developer mistakes early rather than at runtime deep in the pipeline.

  This is the same idea as an interface in Java/C#, but in Python style.

  Without ABC:
      class BadSource:
          pass                   # no read() — silent until pipeline crashes

  With ABC:
      class BadSource(DataSource):
          pass                   # TypeError: Can't instantiate abstract class
                                 # BadSource with abstract method read

  OOP benefits here:
    - All sources have the same interface → parallel_loader can treat them
      identically without knowing whether they are CSV, API, database, etc.
    - New source types (S3, Kafka, JDBC) can be added without touching any
      existing code (Open/Closed Principle).
    - Unit tests can use a FakeSource that just returns a dummy DataFrame.
"""

from abc import ABC, abstractmethod


class DataSource(ABC):
    """
    Abstract base class that every data source must extend.

    Subclasses must implement:
        read() → returns loaded data (Spark DataFrame or Python list)
    """

    def __init__(self, name: str, config: dict):
        """
        Args:
            name:   Logical name of the source (from config.yaml key).
            config: Dict of source-specific options from config.yaml.
        """
        self.name = name
        self.config = config

    @abstractmethod
    def read(self):
        """
        Load data from the underlying store and return it.

        TEACHING NOTE:
          The @abstractmethod decorator marks this as a requirement.
          The method body here is just pass — it is never called.
          Subclasses provide the real implementation.
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"
