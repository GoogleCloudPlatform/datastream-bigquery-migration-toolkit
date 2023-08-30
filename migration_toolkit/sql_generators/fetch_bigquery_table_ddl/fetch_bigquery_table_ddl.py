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
from common.file_writer import write

logger = logging.getLogger(__name__)


FETCH_BIGQUERY_TABLE_DDL_SQL_TEMPLATE = (
    "SELECT ddl FROM `{project_id}.{dataset}`.INFORMATION_SCHEMA.TABLES WHERE"
    " table_name='{table}';"
)


class BigQueryTableDDLFetcher:

  def __init__(self, project_id: str, dataset: str, table: str, filepath: str):
    self.project_id: str = project_id
    self.dataset: str = dataset
    self.table: str = table
    self.filepath = filepath

  def fetch_table_schema(self):
    sql = FETCH_BIGQUERY_TABLE_DDL_SQL_TEMPLATE.format(
        project_id=self.project_id, dataset=self.dataset, table=self.table
    )
    logger.info(f"Generated table schema SQL: {sql}")
    self._write_to_file(sql)

  def _write_to_file(self, sql: str):
    write(filepath=self.filepath, data=sql)
