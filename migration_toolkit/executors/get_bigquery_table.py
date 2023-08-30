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
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery.table import Table

logger = logging.getLogger(__name__)


def execute_get_bigquery_table(
    bigquery_table_name: str, bigquery_client: bigquery.Client
) -> Table:
  logger.debug(f"Executing get table for {bigquery_table_name}")

  try:
    table: Table = bigquery_client.get_table(bigquery_table_name)
  except NotFound:
    table = None

  logger.debug(f"Done. Table: {table}")

  return table
