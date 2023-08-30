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
from common.name_mapper import single_dataset_table_name
from common.source_type import SourceType
from sql_generators.create_table.base_create_table import BaseCreateTable

logger = logging.getLogger(__name__)


class SingleDatasetCreateTable(BaseCreateTable):

  def __init__(
      self,
      source_type: SourceType,
      discover_result_path: str,
      create_target_table_ddl_filepath: str,
      source_schema_name: str,
      source_table_name: str,
      project_id: str,
      bigquery_max_staleness_seconds: int,
      bigquery_dataset_name: str,
  ):
    bigquery_table_name = single_dataset_table_name(
        source_schema_name=source_schema_name,
        source_table_name=source_table_name,
    )
    fully_qualified_bigquery_table_name = (
        project_id + "." + bigquery_dataset_name + "." + bigquery_table_name
    )

    super().__init__(
        source_type=source_type,
        discover_result_path=discover_result_path,
        create_target_table_ddl_filepath=create_target_table_ddl_filepath,
        source_schema_name=source_schema_name,
        source_table_name=source_table_name,
        project_id=project_id,
        bigquery_max_staleness_seconds=bigquery_max_staleness_seconds,
        fully_qualified_bigquery_table_name=fully_qualified_bigquery_table_name,
    )

  def generate_ddl(
      self,
  ):
    create_table_ddl = self._generate_create_table_ddl()
    self._write_to_file(create_table_ddl)
