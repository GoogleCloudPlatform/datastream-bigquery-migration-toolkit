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

import argparse
from common.migration_mode import MigrationMode


def migration_mode(parser):
  parser.add_argument(
      "migration_mode",
      help=(
          f"Migration mode.\n'{MigrationMode.DRY_RUN.value}': only generate the"
          " DDL for 'CREATE TABLE' and SQL for copying data, without"
          f" executing.\n'{MigrationMode.CREATE_TABLE.value}': create a table"
          " in BigQuery, and only generate SQL for copying data without"
          f" executing.\n'{MigrationMode.FULL.value}': create a table in"
          " BigQuery and copy all rows from existing BigQuery table."
      ),
      type=MigrationMode,
      choices=list(MigrationMode),
  )


def force(parser):
  parser.add_argument(
      "--force",
      "-f",
      help="Don't wait for the user prompt.",
      default=False,
      action="store_true",
  )


def verbose(parser):
  parser.add_argument(
      "--verbose",
      "-v",
      help="Verbose logging.",
      default=False,
      action="store_true",
  )


def project_id(parser):
  parser.add_argument(
      "--project-id",
      required=True,
      help=(
          "Google Cloud project ID/number (cross-project migration isn't"
          " supported), for example `datastream-proj` or `5131556981`."
      ),
  )


def datastream_api_endpoint_override(parser):
  parser.add_argument(
      "--datastream-api-endpoint-override",
      required=False,
      help=argparse.SUPPRESS,
  )


def datastream_region(parser):
  parser.add_argument(
      "--datastream-region",
      required=True,
      help="Datastream stream location, for example `us-central1`.",
  )


def stream_id(parser):
  parser.add_argument(
      "--stream-id",
      required=True,
      help="Datastream stream ID, for example `mysql-to-bigquery`.",
  )


def source_schema_name(parser):
  parser.add_argument(
      "--source-schema-name",
      required=True,
      help="Source schema name, for example `my_db`.",
  )


def source_table_name(parser):
  parser.add_argument(
      "--source-table-name",
      required=True,
      help="Source table name, for example `my_table`.",
  )


def bigquery_source_dataset_name(parser):
  parser.add_argument(
      "--bigquery-source-dataset-name",
      required=True,
      help=(
          "BigQuery dataset name of the existing BigQuery table, for example"
          " `dataflow_dataset`."
      ),
  )


def bigquery_source_table_name(parser):
  parser.add_argument(
      "--bigquery-source-table-name",
      required=True,
      help=(
          "The name of the existing BigQuery table, for example"
          " `dataflow_table`."
      ),
  )
