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
import logging
import sys
from common.migration_mode import MigrationMode
from common.monitoring_consts import LABEL_KEY, LABEL_VALUE, USER_AGENT
from executors.copy_rows import execute_copy_rows
from executors.create_table import execute_create_table
from executors.discover import execute_discover
from executors.fetch_bigquery_table_ddl import execute_fetch_bigquery_table_ddl
from executors.get_bigquery_table import execute_get_bigquery_table
from executors.update_stream import execute_update_stream
from google.api_core.gapic_v1.client_info import ClientInfo
from google.cloud import bigquery
from google.cloud.bigquery.table import Table
from google.cloud.datastream_v1.types import Stream
from migration_config import get_config
from sql_generators.copy_rows.copy_rows import CopyDataSQLGenerator
from sql_generators.create_table.dynamic_datasets_create_table import DynamicDatasetsCreateTable
from sql_generators.create_table.single_dataset_create_table import SingleDatasetCreateTable
from sql_generators.fetch_bigquery_table_ddl.fetch_bigquery_table_ddl import BigQueryTableDDLFetcher

logger = logging.getLogger(__name__)
WANTED_USER_PROMPT = "go"


def main():
  config: argparse.Namespace = get_config()
  logger.debug(f"Using config {vars(config)}")

  _add_stream_label(
      stream=config.stream,
      datastream_api_endpoint_override=config.datastream_api_endpoint_override,
  )

  # Run Datastream's discover on connection profile and save response to a file
  execute_discover(
      connection_profile_name=config.connection_profile_name,
      source_schema_name=config.source_schema_name,
      source_table_name=config.source_table_name,
      source_type=config.source_type,
      datastream_api_endpoint_override=config.datastream_api_endpoint_override,
      filepath=config.discover_result_filepath,
  )

  bigquery_client = bigquery.Client(
      client_info=ClientInfo(user_agent=USER_AGENT)
  )

  if config.single_target_stream:
    # Generate CREATE TABLE DDL for single dataset stream and save it to a file
    table_creator = SingleDatasetCreateTable(
        source_type=config.source_type,
        discover_result_path=config.discover_result_filepath,
        create_target_table_ddl_filepath=config.create_target_table_ddl_filepath,
        source_table_name=config.source_table_name,
        source_schema_name=config.source_schema_name,
        bigquery_dataset_name=config.bigquery_target_dataset_name,
        bigquery_max_staleness_seconds=config.bigquery_max_staleness_seconds,
        project_id=config.project_id,
    )
  else:
    # Generate CREATE TABLE DDL for dynamic dataset stream and save it to a file
    table_creator = DynamicDatasetsCreateTable(
        source_type=config.source_type,
        discover_result_path=config.discover_result_filepath,
        create_target_table_ddl_filepath=config.create_target_table_ddl_filepath,
        source_table_name=config.source_table_name,
        source_schema_name=config.source_schema_name,
        bigquery_max_staleness_seconds=config.bigquery_max_staleness_seconds,
        project_id=config.project_id,
        bigquery_region=config.bigquery_region,
        bigquery_kms_key_name=config.bigquery_kms_key_name,
        bigquery_dataset_name=config.bigquery_target_dataset_name,
    )

  table_creator.generate_ddl()
  table_id = table_creator.get_fully_qualified_bigquery_table_name()

  if (
      config.migration_mode == MigrationMode.CREATE_TABLE
      or config.migration_mode == MigrationMode.FULL
  ):
    _verify_bigquery_table_not_exist(
        table_id=table_id, bigquery_client=bigquery_client
    )

    _wait_for_user_prompt_if_necessary("Creating BigQuery table", config.force)
    # Run DDL on BigQuery
    execute_create_table(
        filepath=config.create_target_table_ddl_filepath,
        bigquery_client=bigquery_client,
    )

  # Generate SQL statement for fetching source BigQuery table DDL and save it to a file
  BigQueryTableDDLFetcher(
      project_id=config.project_id,
      dataset=config.bigquery_source_dataset_name,
      table=config.bigquery_source_table_name,
      filepath=config.fetch_bigquery_source_table_ddl_filepath,
  ).fetch_table_schema()

  # Run SQL statement and save the DDL to a file
  execute_fetch_bigquery_table_ddl(
      sql_filepath=config.fetch_bigquery_source_table_ddl_filepath,
      output_path=config.create_source_table_ddl_filepath,
      bigquery_client=bigquery_client,
  )

  # Generate copy rows SQL statement and save it to a file
  CopyDataSQLGenerator(
      source_bigquery_table_ddl=config.create_source_table_ddl_filepath,
      destination_bigquery_table_ddl=config.create_target_table_ddl_filepath,
      filepath=config.copy_rows_filepath,
  ).generate_sql()

  if config.migration_mode == MigrationMode.FULL:
    _wait_for_user_prompt_if_necessary(
        "Copying rows from"
        f" {config.project_id}.{config.bigquery_source_dataset_name}.{config.bigquery_source_table_name} to"
        f" {table_id}",
        config.force,
    )

    # Run SQL statement to copy rows
    execute_copy_rows(
        config.copy_rows_filepath, bigquery_client=bigquery_client
    )

  if config.migration_mode == MigrationMode.DRY_RUN:
    logger.info(
        "Dry run finished successfully.\nGenerated `CREATE TABLE` DDL at"
        f" '{config.create_target_table_ddl_filepath}'.\nGenerated copy rows"
        f" SQL at '{config.copy_rows_filepath}'."
    )
  elif config.migration_mode == MigrationMode.CREATE_TABLE:
    logger.info(
        "Table created successfully.\n"
        f"New table name is `{table_id}`.\n"
        f"Generated copy rows SQL at '{config.copy_rows_filepath}'."
    )
  else:
    logger.info(
        f"Migration finished successfully. New table name is `{table_id}`"
    )


def _add_stream_label(stream: Stream, datastream_api_endpoint_override: str):
  stream.labels[LABEL_KEY] = LABEL_VALUE
  execute_update_stream(
      stream=stream,
      datastream_api_endpoint_override=datastream_api_endpoint_override,
  )


def _verify_bigquery_table_not_exist(
    table_id: str, bigquery_client: bigquery.Client
):
  table: Table = execute_get_bigquery_table(
      table_id, bigquery_client=bigquery_client
  )

  # Table exists, exit to avoid data corruption.
  if table:
    logger.error(
        f"ERROR: Table {table_id} already exists. Drop the table and rerun the"
        " migration."
    )
    sys.exit(1)


def _wait_for_user_prompt_if_necessary(msg: str, force: bool):
  if force:
    logger.info(msg + ".")
  else:
    prompt = ""
    while WANTED_USER_PROMPT != prompt:
      prompt = input(f"{msg}. Type '{WANTED_USER_PROMPT}' to continue..  ")


if __name__ == "__main__":
  main()
