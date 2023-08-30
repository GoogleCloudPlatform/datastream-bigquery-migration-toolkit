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

from collections import defaultdict
import logging
from typing import Dict, Union
from common.bigquery_type import BigQueryType
from sql_generators.create_table.column_converters.base_bigquery_column_converter import BaseBigQueryColumnConverter

logger = logging.getLogger(__name__)


class MySqlBigQueryColumnConverter(BaseBigQueryColumnConverter):
  # Taken from https://cloud.google.com/datastream/docs/destination-bigquery#map_data_types
  MYSQL_TYPE_TO_BIGQUERY_TYPE: defaultdict[str, BigQueryType] = defaultdict(
      lambda: _log_and_fallback(),
      {
          "BIGINT": BigQueryType.INT64,
          "BINARY": BigQueryType.STRING,
          "BIT": BigQueryType.INT64,
          "BLOB": BigQueryType.STRING,
          "BOOL": BigQueryType.INT64,
          "CHAR": BigQueryType.STRING,
          "DATE": BigQueryType.DATE,
          "DATETIME": BigQueryType.DATETIME,
          "DOUBLE": BigQueryType.FLOAT64,
          "ENUM": BigQueryType.STRING,
          "FLOAT": BigQueryType.FLOAT64,
          "INTEGER": BigQueryType.INT64,
          "INT": BigQueryType.INT64,
          "JSON": BigQueryType.JSON,
          "LONGBLOB": BigQueryType.STRING,
          "LONGTEXT": BigQueryType.STRING,
          "MEDIUMBLOB": BigQueryType.STRING,
          "MEDIUMINT": BigQueryType.INT64,
          "MEDIUMTEXT": BigQueryType.STRING,
          "SET": BigQueryType.STRING,
          "SMALLINT": BigQueryType.INT64,
          "TEXT": BigQueryType.STRING,
          "TIME": BigQueryType.INTERVAL,
          "TIMESTAMP": BigQueryType.TIMESTAMP,
          "TINYBLOB": BigQueryType.STRING,
          "TINYINT": BigQueryType.INT64,
          "TINYTEXT": BigQueryType.STRING,
          "VARBINARY": BigQueryType.STRING,
          "VARCHAR": BigQueryType.STRING,
          "YEAR": BigQueryType.INT64,
      },
  )

  def convert(
      self, column: Dict[str, Union[str, int]]
  ) -> Union[str, BigQueryType]:
    mysql_type = column["dataType"].upper()

    if mysql_type == "DECIMAL":
      bigquery_type = self._convert_mysql_decimal(column)
    else:
      bigquery_type = MySqlBigQueryColumnConverter.MYSQL_TYPE_TO_BIGQUERY_TYPE[
          mysql_type
      ]

    logger.debug(
        f"Converted column to BigQuery type: {column} ==> {bigquery_type}"
    )
    return bigquery_type

  def _convert_mysql_decimal(
      self, mysql_column: Dict[str, Union[str, int]]
  ) -> Union[str, BigQueryType]:
    precision = mysql_column.get("precision", None)
    if precision is None:
      return BigQueryType.BIGNUMERIC

    scale = mysql_column["scale"]
    return self._to_bigquery_decimal(precision, scale)


def _log_and_fallback():
  logger.warning("Falling back to STRING")
  return BigQueryType.STRING
