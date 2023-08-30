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

import os

OUTPUT_DIRECTORY_BASE = "output"

DATASTREAM_DISCOVER_RESULT_DIRECTORY = os.path.join(
    OUTPUT_DIRECTORY_BASE, "discover_result"
)
DATASTREAM_DISCOVER_RESULT_FILENAME_TEMPLATE = (
    "{connection_profile_name}_{schema_name}_{table_name}.json"
)

SOURCE_TABLE_DDL_DIRECTORY = os.path.join(
    OUTPUT_DIRECTORY_BASE, "source_table_ddl"
)
SOURCE_TABLE_DDL_FILENAME_TEMPLATE = "{table_name}.sql"

CREATE_TARGET_TABLE_DDL_DIRECTORY = os.path.join(
    OUTPUT_DIRECTORY_BASE, "create_target_table"
)
CREATE_TARGET_TABLE_DDL_FILENAME_TEMPLATE = "{table_name}.sql"

BIGQUERY_TABLE_SCHEMA_DIRECTORY = os.path.join(
    OUTPUT_DIRECTORY_BASE, "bigquery_table_schema"
)
BIGQUERY_TABLE_SCHEMA_FILENAME_TEMPLATE = "{table_name}.sql"

FETCH_BIGQUERY_TABLE_DDL_DIRECTORY = os.path.join(
    OUTPUT_DIRECTORY_BASE, "fetch_source_bigquery_table_ddl"
)
FETCH_BIGQUERY_TABLE_DDL_FILENAME_TEMPLATE = "{table_name}.sql"

COPY_ROWS_DIRECTORY = os.path.join(OUTPUT_DIRECTORY_BASE, "copy_rows")
COPY_ROWS_FILENAME_TEMPLATE = "{source_table}__to__{destination_table}.sql"
