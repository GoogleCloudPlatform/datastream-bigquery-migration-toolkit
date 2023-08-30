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
from typing import List
from common.file_reader import read
from common.file_writer import write
from google.cloud import bigquery

logger = logging.getLogger(__name__)


def execute_fetch_bigquery_table_ddl(
    sql_filepath: str, output_path: str, bigquery_client: bigquery.Client
):
  logger.debug(f"Executing fetch BigQuery table DDL. Filepath: {sql_filepath}")

  sql = read(filepath=sql_filepath)

  logger.info(f"Running SQL query: {sql}")
  query_job = bigquery_client.query(sql)
  rows = [row for row in query_job.result()]

  if len(rows) != 1:
    raise AssertionError(
        f"Expected only one match for query: '{sql}', but got: {rows}"
    )

  ddl: List[str] = rows[0]["ddl"]
  logger.info(f"Got response: {ddl}")

  _write_to_file(path=output_path, ddl=ddl)


def _write_to_file(path, ddl):
  write(
      filepath=path,
      data=ddl,
  )
