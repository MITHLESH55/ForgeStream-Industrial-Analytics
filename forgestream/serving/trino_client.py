"""
ForgeStream Phase 4 Trino Query Client.
Provides typed execution of analytical SQL queries against Apache Trino,
measuring execution timing, result parsing, and error diagnostics.
"""

from datetime import datetime, timezone
import time
from typing import Any, Dict, List, Optional, Tuple, Union
import requests
from forgestream.observability.logging import get_logger

logger = get_logger("serving.trino_client")


class TrinoQueryResult:
    """Encapsulates the execution results and metrics of a Trino SQL query."""

    def __init__(
        self,
        query: str,
        columns: List[str],
        rows: List[List[Any]],
        execution_time_ms: float,
        query_id: Optional[str] = None,
        status: str = "SUCCESS",
        error_message: Optional[str] = None,
    ):
        self.query = query
        self.columns = columns
        self.rows = rows
        self.execution_time_ms = execution_time_ms
        self.query_id = query_id
        self.status = status
        self.error_message = error_message

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def to_dict_list(self) -> List[Dict[str, Any]]:
        """Converts tabular result rows to a list of column-keyed dictionaries."""
        return [dict(zip(self.columns, row)) for row in self.rows]

    def to_summary(self) -> Dict[str, Any]:
        """Returns structured metadata summary of query execution."""
        return {
            "query_id": self.query_id,
            "status": self.status,
            "row_count": self.row_count,
            "column_count": len(self.columns),
            "columns": self.columns,
            "execution_time_ms": round(self.execution_time_ms, 2),
            "error_message": self.error_message,
        }

    def to_dict(self) -> Dict[str, Any]:
        """Returns dictionary representation of query result summary."""
        return self.to_summary()


class TrinoClient:
    """
    Client for interacting with Apache Trino coordinator REST API and JDBC/Python driver.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8085,
        user: str = "forgestream_admin",
        catalog: str = "lakehouse",
        schema: str = "forgestream",
        timeout_sec: float = 30.0,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self.timeout_sec = timeout_sec
        self.base_url = f"http://{self.host}:{self.port}"

    def is_alive(self) -> bool:
        """Checks if the Trino coordinator is healthy and accepting queries."""
        try:
            resp = requests.get(f"{self.base_url}/v1/info", timeout=3.0)
            if resp.status_code == 200:
                data = resp.json()
                return not data.get("starting", False)
            return False
        except Exception:
            return False

    def get_server_info(self) -> Dict[str, Any]:
        """Retrieves server info and version from Trino coordinator."""
        try:
            resp = requests.get(f"{self.base_url}/v1/info", timeout=5.0)
            if resp.status_code == 200:
                return resp.json()
            return {"error": f"HTTP {resp.status_code}"}
        except Exception as ex:
            return {"error": str(ex)}

    def execute_query(
        self,
        query: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> TrinoQueryResult:
        """
        Submits an analytical SQL statement to Trino REST API (/v1/statement)
        and polls until results are ready.
        """
        cat = catalog or self.catalog
        sch = schema or self.schema
        start_t = time.perf_counter()

        # Trino REST API requires queries without trailing semicolons
        clean_query = query.strip().rstrip(";").strip()

        headers = {
            "X-Trino-User": self.user,
            "X-Trino-Catalog": cat,
            "X-Trino-Schema": sch,
        }

        try:
            resp = requests.post(
                f"{self.base_url}/v1/statement",
                data=clean_query.encode("utf-8"),
                headers=headers,
                timeout=self.timeout_sec,
            )
            if resp.status_code != 200:
                elapsed_ms = (time.perf_counter() - start_t) * 1000.0
                return TrinoQueryResult(
                    query=query,
                    columns=[],
                    rows=[],
                    execution_time_ms=elapsed_ms,
                    status="FAILED",
                    error_message=f"HTTP {resp.status_code}: {resp.text}",
                )

            data = resp.json()
            query_id = data.get("id")
            next_uri = data.get("nextUri")

            columns: List[str] = []
            rows: List[List[Any]] = []

            if "columns" in data:
                columns = [c["name"] for c in data["columns"]]
            if "data" in data:
                rows.extend(data["data"])

            while next_uri:
                poll_resp = requests.get(next_uri, headers=headers, timeout=self.timeout_sec)
                if poll_resp.status_code != 200:
                    elapsed_ms = (time.perf_counter() - start_t) * 1000.0
                    return TrinoQueryResult(
                        query=query,
                        columns=columns,
                        rows=rows,
                        execution_time_ms=elapsed_ms,
                        query_id=query_id,
                        status="FAILED",
                        error_message=f"Polling failed HTTP {poll_resp.status_code}",
                    )
                data = poll_resp.json()
                if "error" in data:
                    err = data["error"]
                    err_msg = err.get("message", "Unknown Trino execution error")
                    elapsed_ms = (time.perf_counter() - start_t) * 1000.0
                    return TrinoQueryResult(
                        query=query,
                        columns=columns,
                        rows=rows,
                        execution_time_ms=elapsed_ms,
                        query_id=query_id,
                        status="FAILED",
                        error_message=err_msg,
                    )
                if "columns" in data and not columns:
                    columns = [c["name"] for c in data["columns"]]
                if "data" in data:
                    rows.extend(data["data"])
                next_uri = data.get("nextUri")

            elapsed_ms = (time.perf_counter() - start_t) * 1000.0
            return TrinoQueryResult(
                query=query,
                columns=columns,
                rows=rows,
                execution_time_ms=elapsed_ms,
                query_id=query_id,
                status="SUCCESS",
            )

        except Exception as ex:
            elapsed_ms = (time.perf_counter() - start_t) * 1000.0
            return TrinoQueryResult(
                query=query,
                columns=[],
                rows=[],
                execution_time_ms=elapsed_ms,
                status="ERROR",
                error_message=str(ex),
            )

    execute = execute_query
    check_health = is_alive

