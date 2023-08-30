# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
from typing import Dict, List, Union
from common.source_type import SourceType

logger = logging.getLogger(__name__)


class DiscoverResultParser:
  SOURCE_TYPE_TO_RDBMS: Dict[SourceType, str] = {
      SourceType.MYSQL: "mysqlRdbms",
      SourceType.ORACLE: "oracleRdbms",
  }
  SOURCE_TYPE_TO_SCHEMAS: Dict[SourceType, str] = {
      SourceType.MYSQL: "mysqlDatabases",
      SourceType.ORACLE: "oracleSchemas",
  }
  SOURCE_TYPE_TO_SCHEMA: Dict[SourceType, str] = {
      SourceType.MYSQL: "database",
      SourceType.ORACLE: "schema",
  }
  SOURCE_TYPE_TO_TABLES: Dict[SourceType, str] = {
      SourceType.MYSQL: "mysqlTables",
      SourceType.ORACLE: "oracleTables",
  }
  SOURCE_TYPE_TO_COLUMNS: Dict[SourceType, str] = {
      SourceType.MYSQL: "mysqlColumns",
      SourceType.ORACLE: "oracleColumns",
  }

  def __init__(
      self,
      discover_result_path: str,
      source_type: SourceType,
  ):
    self.discover_result_path: str = discover_result_path
    self.source_type: SourceType = source_type
    self.discover_result = self._load_discover_result()

  def _load_discover_result(self):
    logger.debug(f"Loading '{self.discover_result_path}'..")
    with open(self.discover_result_path, "r") as f:
      try:
        discover_result = json.load(f)
      except Exception as ex:
        raise TypeError(
            ex,
            f"Failed to load {self.discover_result_path} as a `.json` file."
            f" Make sure {self.discover_result_path} was generated using"
            " `discover.py` and try again.",
        )

    return discover_result[self.SOURCE_TYPE_TO_RDBMS[self.source_type]][
        self.SOURCE_TYPE_TO_SCHEMAS[self.source_type]
    ]

  def list_schemas(self) -> List[str]:
    return [
        d[self.SOURCE_TYPE_TO_SCHEMA[self.source_type]]
        for d in self.discover_result
    ]

  def list_tables(self, schema_name: str) -> List[str]:
    schema = self._get_schema(schema_name)
    return self._list_tables(schema)

  def get_table(
      self, schema_name: str, table_name: str
  ) -> List[Dict[str, Union[str, int]]]:
    logger.debug(
        f"Extracting table '{table_name}' in database"
        f" '{schema_name}' from `discover` result.."
    )

    schema = self._get_schema(schema_name=schema_name)
    return self._get_table(
        schema=schema, table_name=table_name, schema_name=schema_name
    )

  @staticmethod
  def _list_tables(schema):
    return [t["table"] for t in schema]

  def _get_schema(self, schema_name: str):
    schema = next(
        (
            d.get(self.SOURCE_TYPE_TO_TABLES[self.source_type], None)
            for d in self.discover_result
            if d[self.SOURCE_TYPE_TO_SCHEMA[self.source_type]] == schema_name
        ),
        None,
    )

    if not schema:
      available_schemas = self.list_schemas()
      raise KeyError(
          f"Source database `{schema_name}` does not appear in the discover"
          " result, or the database has no tables. Make sure the database is"
          " present in the connection profile. Available databases are:"
          f" {available_schemas}"
      )

    return schema

  def _get_table(self, schema, table_name, schema_name):
    table = next(
        (
            t[self.SOURCE_TYPE_TO_COLUMNS[self.source_type]]
            for t in schema
            if t["table"] == table_name
        ),
        None,
    )

    if not table:
      available_tables = self._list_tables(schema=schema)
      raise KeyError(
          f"Source table `{table_name}` does not appear in the"
          f" database `{schema_name}`. Available tables are:"
          f" {available_tables}"
      )
    return table
