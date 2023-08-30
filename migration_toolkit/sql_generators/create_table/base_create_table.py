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

from abc import ABC
import logging
import re
from typing import Dict, List, Union
from common.file_writer import write
from common.source_type import SourceType
from sql_generators.create_table.column_converters.base_bigquery_column_converter import BaseBigQueryColumnConverter
from sql_generators.create_table.column_converters.mysql_to_bigquery_column_converter import MySqlBigQueryColumnConverter
from sql_generators.create_table.column_converters.oracle_to_bigquery_column_converter import OracleBigQueryColumnConverter
from sql_generators.create_table.discover_result_parser import DiscoverResultParser

logger = logging.getLogger(__name__)


class BaseCreateTable(ABC):
  CREATE_TABLE_WITH_PRIMARY_KEYS_DDL_TEMPLATE = (
      "CREATE TABLE `{table_name}` \n"
      "(\n"
      "  {columns},\n"
      "  PRIMARY KEY({primary_keys}) NOT ENFORCED\n"
      ")\n"
      "CLUSTER BY {clustering_keys}\n"
      "OPTIONS(\n"
      "  max_staleness=MAKE_INTERVAL(0, 0, 0, 0, 0, {max_staleness})\n"
      ");"
  )
  CREATE_TABLE_WITHOUT_PRIMARY_KEYS_DDL_TEMPLATE = (
      "CREATE TABLE `{table_name}` \n"
      "(\n"
      "  {columns}\n"
      ")\n"
      " OPTIONS(\n"
      "  max_staleness=MAKE_INTERVAL(0, 0, 0, 0, 0, {max_staleness})\n"
      ");"
  )

  def __init__(
      self,
      source_type: SourceType,
      discover_result_path: str,
      create_target_table_ddl_filepath: str,
      source_schema_name: str,
      source_table_name: str,
      project_id: str,
      bigquery_max_staleness_seconds: int,
      fully_qualified_bigquery_table_name: str,
  ):
    self.source_type: SourceType = source_type
    self.discover_result_parser: DiscoverResultParser = DiscoverResultParser(
        discover_result_path=discover_result_path, source_type=source_type
    )
    self.create_target_table_ddl_filepath = create_target_table_ddl_filepath
    self.source_schema_name: str = source_schema_name
    self.source_table_name: str = source_table_name
    self.project_id: str = project_id
    self.bigquery_max_staleness_seconds: int = bigquery_max_staleness_seconds
    self.fully_qualified_bigquery_table_name: str = (
        fully_qualified_bigquery_table_name
    )

  def get_fully_qualified_bigquery_table_name(self):
    return self.fully_qualified_bigquery_table_name

  def _generate_create_table_ddl(
      self,
  ):
    source_table: List[Dict[str, Union[str, int]]] = (
        self.discover_result_parser.get_table(
            schema_name=self.source_schema_name,
            table_name=self.source_table_name,
        )
    )
    bigquery_columns: List[str] = self._get_bigquery_columns(
        source_table=source_table
    )

    bigquery_primary_keys: List[str] = self._get_primary_keys(
        source_table, bigquery_columns
    )
    logger.debug(f"BigQuery table columns are {bigquery_columns}")
    logger.debug(
        f"BigQuery table primary key columns are {bigquery_primary_keys}"
    )

    bigquery_clustering_keys = self._get_clustering_keys(bigquery_primary_keys)
    logger.debug(
        f"BigQuery table clustering columns are {bigquery_clustering_keys}"
    )

    logger.debug(
        "BigQuery table max staleness option is"
        f" {self.bigquery_max_staleness_seconds} seconds"
    )

    bigquery_columns = ",\n  ".join(bigquery_columns)

    if bigquery_primary_keys:
      create_table_ddl = (
          BaseCreateTable.CREATE_TABLE_WITH_PRIMARY_KEYS_DDL_TEMPLATE.format(
              table_name=self.fully_qualified_bigquery_table_name,
              columns=bigquery_columns,
              primary_keys=", ".join(bigquery_primary_keys),
              clustering_keys=", ".join(bigquery_clustering_keys),
              max_staleness=self.bigquery_max_staleness_seconds,
          )
      )
    else:
      create_table_ddl = (
          BaseCreateTable.CREATE_TABLE_WITHOUT_PRIMARY_KEYS_DDL_TEMPLATE.format(
              table_name=self.fully_qualified_bigquery_table_name,
              columns=bigquery_columns,
              max_staleness=self.bigquery_max_staleness_seconds,
          )
      )

    logger.info(f"Generated create table DDL:\n{create_table_ddl}")

    return create_table_ddl

  def _write_to_file(self, ddl: str):
    write(filepath=self.create_target_table_ddl_filepath, data=ddl)

  def _get_primary_keys(
      self,
      source_table: List[Dict[str, Union[str, int]]],
      bigquery_columns: List[str],
  ) -> List[str]:
    primary_keys = [
        f"`{self._clean_column_name(column['column'])}`"
        for column in source_table
        if column.get("primaryKey") is True
    ]

    # For Oracle source, we use ROWID as primary key if no primary key is provided.
    if len(primary_keys) == 0 and self.source_type == SourceType.ORACLE:
      primary_keys = ["`ROWID`"]
      bigquery_columns.append("`ROWID` STRING")

    return primary_keys

  @staticmethod
  def _get_clustering_keys(primary_keys: List[str]) -> List[str]:
    return primary_keys if len(primary_keys) <= 4 else primary_keys[:4]

  def _get_bigquery_columns(
      self, source_table: List[Dict[str, Union[str, int]]]
  ) -> List[str]:
    bigquery_columns: List[str] = []

    converter: BaseBigQueryColumnConverter
    if self.source_type == SourceType.MYSQL:
      converter = MySqlBigQueryColumnConverter()
    else:
      if self.source_type != SourceType.ORACLE:
        raise AssertionError(f"Unexpected source type: {self.source_type}")
      converter = OracleBigQueryColumnConverter()

    for column in source_table:
      column_name = self._clean_column_name(column["column"])
      column_type = converter.convert(column)

      bigquery_columns.append(f"`{column_name}` {column_type}")

    return bigquery_columns

  @staticmethod
  def _clean_column_name(column_name):
    cleaned_name = str(re.sub("[^\\w]", "_", column_name, flags=re.ASCII))
    if cleaned_name[0].isdigit():
      return f"_{cleaned_name}"

    return cleaned_name
