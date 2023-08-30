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
from sql_generators.create_table.column_converters.base_bigquery_column_converter import BIGQUERY_BIGNUMERIC_MAX_PRECISION, BIGQUERY_INT64_MAX_PRECISION, BaseBigQueryColumnConverter

logger = logging.getLogger(__name__)


class OracleBigQueryColumnConverter(BaseBigQueryColumnConverter):
  # Taken from https://cloud.google.com/datastream/docs/destination-bigquery#map_data_types
  ORACLE_TYPE_TO_BIGQUERY_TYPE: defaultdict[str, BigQueryType] = defaultdict(
      lambda: BigQueryType.STRING,
      {
          "BFILE": BigQueryType.STRING,
          "BINARY_DOUBLE": BigQueryType.FLOAT64,
          "BINARY_FLOAT": BigQueryType.FLOAT64,
          "CHAR": BigQueryType.STRING,
          "DATE": BigQueryType.DATETIME,
          "DOUBLE_PRECISION": BigQueryType.FLOAT64,
          "FLOAT": BigQueryType.FLOAT64,
          "LONG": BigQueryType.STRING,
          "LONG_RAW": BigQueryType.STRING,
          "NCHAR": BigQueryType.STRING,
          "NVARCHAR2": BigQueryType.STRING,
          "RAW": BigQueryType.STRING,
          "ROWID": BigQueryType.STRING,
          "SMALLINT": BigQueryType.INT64,
          "TIMESTAMP": BigQueryType.TIMESTAMP,
          "TIMESTAMP WITH TIME ZONE": BigQueryType.TIMESTAMP,
          "UROWID": BigQueryType.STRING,
          "VARCHAR": BigQueryType.STRING,
          "VARCHAR2": BigQueryType.STRING,
      },
  )

  def convert(
      self, column: Dict[str, Union[str, int]]
  ) -> Union[str, BigQueryType]:
    oracle_type = column["dataType"].upper()

    if oracle_type == "NUMBER":
      bigquery_type = self._convert_oracle_number(column)
    else:
      # TIMESTAMP(*) and TIMESTAMP(*) WITH TIME ZONE are converted to BigQuery TIMESTAMP
      if oracle_type.startswith("TIMESTAMP"):
        oracle_type = "TIMESTAMP"
      bigquery_type = (
          OracleBigQueryColumnConverter.ORACLE_TYPE_TO_BIGQUERY_TYPE[
              oracle_type
          ]
      )

    logger.debug(
        f"Converted column to BigQuery type: {column} ==> {bigquery_type}"
    )
    return bigquery_type

  def _convert_oracle_number(
      self, oracle_column: Dict[str, Union[str, int]]
  ) -> Union[str, BigQueryType]:
    if not oracle_column.get("precision"):
      return BigQueryType.STRING

    precision = oracle_column["precision"]
    scale = oracle_column.get("scale", 0)

    if scale <= 0:
      if precision <= BIGQUERY_INT64_MAX_PRECISION:
        return BigQueryType.INT64
      elif precision <= BIGQUERY_BIGNUMERIC_MAX_PRECISION:
        return self._to_bigquery_decimal(precision, scale)
      else:
        return BigQueryType.STRING
    else:
      if precision <= BIGQUERY_BIGNUMERIC_MAX_PRECISION:
        return self._to_bigquery_decimal(precision, scale)
      else:
        return BigQueryType.STRING
