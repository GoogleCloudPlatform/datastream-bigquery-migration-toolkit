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

import logging
from typing import Dict, NamedTuple
from common.bigquery_type import BigQueryType
from common.file_writer import write
from sql_generators.copy_rows.ddl_parser import DDLParser

logger = logging.getLogger(__name__)


COPY_DATA_SQL = (
    "INSERT INTO {destination_table}\n"
    "(\n"
    "  {destination_columns}\n"
    ")\n"
    "SELECT\n"
    "  {source_columns}\n"
    "FROM {source_table};"
)


class ColumnSchema(NamedTuple):
  source: BigQueryType
  destination: BigQueryType


# This dictionary provides the appropriate casts between tables that were created
# with Dataflow's "Datastream to BigQuery" template and tables that were created
# with Datastream's native BigQuery solution.
COLUMN_SCHEMAS_TO_CAST_EXPRESSION: Dict[ColumnSchema, str] = {
    # MySQL BINARY, VARBINARY
    # Oracle BLOB
    ColumnSchema(
        BigQueryType.BYTES, BigQueryType.STRING
    ): "SAFE_CONVERT_BYTES_TO_STRING({column_name})",
    # MySQL DATETIME
    ColumnSchema(
        BigQueryType.TIMESTAMP, BigQueryType.DATETIME
    ): "CAST({column_name} as DATETIME)",
    # MySQL DECIMAL
    ColumnSchema(
        BigQueryType.BIGNUMERIC, BigQueryType.NUMERIC
    ): "CAST({column_name} as NUMERIC)",
    # MySQL DECIMAL
    # Oracle NUMBER
    ColumnSchema(
        BigQueryType.BIGNUMERIC, BigQueryType.STRING
    ): "CAST({column_name} as STRING)",
    # MySQL TIME
    ColumnSchema(BigQueryType.STRING, BigQueryType.INTERVAL): (
        "MAKE_INTERVAL(hour=>DIV(CAST({column_name} as INT64), "
        "3600000000), second=>DIV(MOD(CAST({column_name} as "
        "INT64), 3600000000), 1000000)) as {column_name}"
    ),
    # MySQL YEAR
    ColumnSchema(
        BigQueryType.STRING, BigQueryType.INT64
    ): "CAST({column_name} as INT64)",
    # MySQL JSON
    ColumnSchema(
        BigQueryType.STRING, BigQueryType.JSON
    ): "PARSE_JSON({column_name})",
    # Oracle NUMBER with negative precision
    ColumnSchema(
        BigQueryType.BIGNUMERIC, BigQueryType.INT64
    ): "CAST({column_name} as INT64)",
}


class CopyDataSQLGenerator:

  def __init__(
      self,
      source_bigquery_table_ddl: str,
      destination_bigquery_table_ddl: str,
      filepath: str,
  ):
    self.source_ddl_parser = DDLParser(source_bigquery_table_ddl)
    self.destination_ddl_parser = DDLParser(destination_bigquery_table_ddl)
    self.filepath = filepath

  def generate_sql(self):
    source_columns = []
    destination_columns = []
    for (
        column_name,
        source_type,
    ) in self.source_ddl_parser.get_schema().items():
      destination_type = self.destination_ddl_parser.get_schema().get(
          column_name
      )

      if not destination_type:
        raise ValueError(
            "Column names must match in source and destination, but could not"
            f" find column name {column_name} in destination table. Destination"
            f" schema is {self.destination_ddl_parser.get_schema()}"
        )
      column_name = f"`{column_name}`"

      destination_columns.append(column_name)

      if source_type == destination_type:
        logger.debug(f"Type match for column '{column_name}'")
        source_columns.append(column_name)
      else:
        logger.debug(
            f"Type mismatch for column '{column_name}': {source_type} == >"
            f" {destination_type}"
        )
        source_columns.append(
            COLUMN_SCHEMAS_TO_CAST_EXPRESSION[
                ColumnSchema(source_type, destination_type)
            ].format(column_name=column_name)
        )

    sql = COPY_DATA_SQL.format(
        destination_table=self.destination_ddl_parser.get_fully_qualified_table_name(),
        source_columns=",\n  ".join(source_columns),
        destination_columns=",\n  ".join(destination_columns),
        source_table=self.source_ddl_parser.get_fully_qualified_table_name(),
    )
    logger.info(f"Generated copy rows SQL statement:\n'{sql}'")
    self._write_to_file(sql)

  def _write_to_file(self, sql):
    write(filepath=self.filepath, data=sql)
