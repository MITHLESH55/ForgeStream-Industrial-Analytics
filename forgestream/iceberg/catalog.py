"""
Apache Iceberg SqlCatalog Management.
Provides catalog initialization, namespace lifecycle, and warehouse directory setup.
"""

from pathlib import Path
from typing import Optional, Union
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from forgestream.config import settings
from forgestream.observability.logging import get_logger

logger = get_logger("iceberg.catalog")


class IcebergCatalogManager:
    """Manages PyIceberg SqlCatalog lifecycle and configuration."""

    def __init__(
        self,
        catalog_name: str = "default",
        catalog_uri: Optional[str] = None,
        warehouse_path: Optional[str] = None,
        namespace: Optional[str] = None,
    ):
        self.catalog_name = catalog_name
        self.namespace = namespace or settings.iceberg.namespace

        # Resolve paths
        raw_uri = catalog_uri or settings.iceberg.catalog_uri
        raw_warehouse = warehouse_path or settings.iceberg.warehouse_path

        # Handle local sqlite path resolution
        if raw_uri.startswith("sqlite:///"):
            sqlite_file = raw_uri.replace("sqlite:///", "")
            if not Path(sqlite_file).is_absolute():
                abs_sqlite_file = (Path(settings.project_root) / sqlite_file).resolve()
            else:
                abs_sqlite_file = Path(sqlite_file)
            abs_sqlite_file.parent.mkdir(parents=True, exist_ok=True)
            self.catalog_uri = f"sqlite:///{abs_sqlite_file.as_posix()}"
        else:
            self.catalog_uri = raw_uri

        # Handle local warehouse path resolution
        if raw_warehouse.startswith("file:///"):
            wh_dir = raw_warehouse.replace("file:///", "")
            if not Path(wh_dir).is_absolute():
                abs_wh_dir = (Path(settings.project_root) / wh_dir).resolve()
            else:
                abs_wh_dir = Path(wh_dir)
            abs_wh_dir.mkdir(parents=True, exist_ok=True)
            self.warehouse_path = abs_wh_dir.as_posix()
        else:
            self.warehouse_path = raw_warehouse
            Path(self.warehouse_path).mkdir(parents=True, exist_ok=True)

        self._catalog: Optional[Catalog] = None

    @property
    def catalog(self) -> Catalog:
        """Initializes and returns the PyIceberg SqlCatalog instance."""
        if self._catalog is None:
            properties = {
                "uri": self.catalog_uri,
                "warehouse": self.warehouse_path,
            }
            self._catalog = SqlCatalog(self.catalog_name, **properties)
            self._ensure_namespace()
            logger.info(
                f"PyIceberg SqlCatalog initialized: uri={self.catalog_uri}, warehouse={self.warehouse_path}"
            )
        return self._catalog

    def _ensure_namespace(self) -> None:
        """Creates the iceberg namespace if it does not exist."""
        try:
            if self.namespace not in self.catalog.list_namespaces():
                self.catalog.create_namespace_if_not_exists(self.namespace)
                logger.info(f"Created Iceberg namespace: {self.namespace}")
        except Exception as e:
            logger.debug(f"Namespace check: {e}")
