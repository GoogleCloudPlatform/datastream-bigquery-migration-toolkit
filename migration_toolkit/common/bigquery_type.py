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

from enum import Enum


class BigQueryType(Enum):
  STRING = "STRING"
  INT64 = "INT64"
  FLOAT64 = "FLOAT64"
  BYTES = "BYTES"
  TIMESTAMP = "TIMESTAMP"
  INTERVAL = "INTERVAL"
  DATE = "DATE"
  DATETIME = "DATETIME"
  NUMERIC = "NUMERIC"
  BIGNUMERIC = "BIGNUMERIC"
  JSON = "JSON"

  def with_precision_and_scale(self, p, s) -> str:
    return f"{self.value}({p}, {s})"

  def __str__(self):
    return self.value
