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

from abc import ABC, abstractmethod
from typing import Dict, Union
from common.bigquery_type import BigQueryType

# Taken from https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
BIGQUERY_NUMERIC_MAX_SCALE = 9
BIGQUERY_NUMERIC_PRECISION_TO_SCALE_MAX_DIFF = 29

BIGQUERY_BIGNUMERIC_MAX_SCALE = 38
BIGQUERY_BIGNUMERIC_PRECISION_TO_SCALE_MAX_DIFF = 38

BIGQUERY_BIGNUMERIC_MAX_PRECISION = 78
BIGQUERY_INT64_MAX_PRECISION = 18


class BaseBigQueryColumnConverter(ABC):

  @abstractmethod
  def convert(self, mysql_column: Dict[str, Union[str, int]]) -> str:
    raise NotImplementedError

  @staticmethod
  def _to_bigquery_decimal(
      precision: int, scale: int
  ) -> Union[BigQueryType, str]:
    if (
        scale <= BIGQUERY_NUMERIC_MAX_SCALE
        and (precision - scale) <= BIGQUERY_NUMERIC_PRECISION_TO_SCALE_MAX_DIFF
    ):
      return BigQueryType.NUMERIC.with_precision_and_scale(precision, scale)
    if (
        scale <= BIGQUERY_BIGNUMERIC_MAX_SCALE
        and (precision - scale)
        <= BIGQUERY_BIGNUMERIC_PRECISION_TO_SCALE_MAX_DIFF
    ):
      return BigQueryType.BIGNUMERIC.with_precision_and_scale(precision, scale)
    return BigQueryType.STRING
