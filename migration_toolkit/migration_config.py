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
from argparse import RawTextHelpFormatter
import json
import logging
import sys
from common import argparse_arguments
from common import name_mapper
from common.logging_config import configure_logging
from common.output_names import *
from common.source_type import SourceType
from executors.get_stream import execute_get_stream
from google.cloud.datastream_v1.types import Stream

logger = logging.getLogger(__name__)


def get_config() -> argparse.Namespace:
  user_args = _get_user_args()
  configure_logging(user_args.verbose)

  stream: Stream = execute_get_stream(
      project_id=user_args.project_id,
      datastream_region=user_args.datastream_region,
      stream_id=user_args.stream_id,
      datastream_api_endpoint_override=user_args.datastream_api_endpoint_override,
  )
  args_from_stream = _get_args_from_stream(stream=stream, user_args=user_args)

  all_args = vars(user_args) | args_from_stream
  _get_filepaths(all_args)

  return argparse.Namespace(**all_args)


def _get_user_args():
  parser = argparse.ArgumentParser(
      description="Datastream BigQuery Migration Toolkit arguments",
      formatter_class=RawTextHelpFormatter,
  )

  argparse_arguments.migration_mode(parser)

  argparse_arguments.force(parser)
  argparse_arguments.verbose(parser)
  argparse_arguments.datastream_api_endpoint_override(parser)

  required_args_parser = parser.add_argument_group("required arguments")
  argparse_arguments.project_id(required_args_parser)
  argparse_arguments.stream_id(required_args_parser)
  argparse_arguments.datastream_region(required_args_parser)

  argparse_arguments.source_schema_name(required_args_parser)
  argparse_arguments.source_table_name(required_args_parser)

  argparse_arguments.bigquery_source_dataset_name(required_args_parser)
  argparse_arguments.bigquery_source_table_name(required_args_parser)

  return parser.parse_args()


def _get_args_from_stream(stream: Stream, user_args):
  args_from_stream = {}
  stream_name = stream.display_name
  if stream.state != Stream.State.PAUSED:
    logger.error(
        f"ERROR: Stream '{stream_name}' should be in state PAUSED, but it is in"
        f" state {stream.state.name}. Please pause the stream and run the"
        " migration again."
    )
    sys.exit(1)

  args_from_stream["stream"] = stream

  args_from_stream["connection_profile_name"] = (
      stream.source_config.source_connection_profile
  )

  args_from_stream["source_type"] = _source_config_to_source_type(
      stream.source_config
  )

  if not hasattr(stream.destination_config, "bigquery_destination_config"):
    logger.error(
        f"ERROR: Stream '{stream_name}' doesn't have BigQuery destination."
        " Recreate the stream with BigQuery destination and run the migration"
        " again."
    )
    sys.exit(1)

  args_from_stream["bigquery_max_staleness_seconds"] = (
      stream.destination_config.bigquery_destination_config.data_freshness.seconds
  )

  if hasattr(
      stream.destination_config.bigquery_destination_config,
      "source_hierarchy_datasets",
  ):
    logger.debug(f"Stream {stream_name} is a source hierarchy stream.")
    args_from_stream["bigquery_region"] = getattr(
        stream.destination_config.bigquery_destination_config.source_hierarchy_datasets.dataset_template,
        "location",
        None,
    )
    dataset_id_prefix = getattr(
        stream.destination_config.bigquery_destination_config.source_hierarchy_datasets.dataset_template,
        "dataset_id_prefix",
        None,
    )
    args_from_stream["bigquery_kms_key_name"] = getattr(
        stream.destination_config.bigquery_destination_config.source_hierarchy_datasets.dataset_template,
        "kms_key_name",
        None,
    )
    args_from_stream["bigquery_target_dataset_name"] = (
        name_mapper.dynamic_datasets_dataset_name(
            dataset_id_prefix=dataset_id_prefix,
            source_schema_name=user_args.source_schema_name,
        )
    )

    args_from_stream["bigquery_target_table_name"] = (
        name_mapper.dynamic_datasets_table_name(
            source_table_name=user_args.source_table_name
        )
    )
    args_from_stream["single_target_stream"] = False

  else:
    args_from_stream["bigquery_target_dataset_name"] = (
        stream.destination_config.bigquery_destination_config.single_target_dataset.dataset_id
    )
    args_from_stream["bigquery_target_table_name"] = (
        name_mapper.single_dataset_table_name(
            source_schema_name=user_args.source_schema_name,
            source_table_name=user_args.source_table_name,
        )
    )
    args_from_stream["single_target_stream"] = True

  return args_from_stream


def _source_config_to_source_type(source_config):
  source_config_json = json.loads(type(source_config).to_json(source_config))
  return (
      SourceType.MYSQL
      if source_config_json.get("mysqlSourceConfig") is not None
      else SourceType.ORACLE
  )


def _get_filepaths(args):
  _, connection_profile_simple_name = args["connection_profile_name"].split(
      "/connectionProfiles/"
  )

  args["discover_result_filepath"] = os.path.join(
      DATASTREAM_DISCOVER_RESULT_DIRECTORY,
      DATASTREAM_DISCOVER_RESULT_FILENAME_TEMPLATE.format(
          connection_profile_name=connection_profile_simple_name,
          schema_name=args["source_schema_name"],
          table_name=args["source_table_name"],
      ),
  )

  bigquery_target_table_fully_qualified_name = f"{args['project_id']}.{args['bigquery_target_dataset_name']}.{args['bigquery_target_table_name']}"
  bigquery_source_table_fully_qualified_name = f"{args['project_id']}.{args['bigquery_source_dataset_name']}.{args['bigquery_source_table_name']}"

  args["fetch_bigquery_source_table_ddl_filepath"] = os.path.join(
      FETCH_BIGQUERY_TABLE_DDL_DIRECTORY,
      FETCH_BIGQUERY_TABLE_DDL_FILENAME_TEMPLATE.format(
          table_name=bigquery_source_table_fully_qualified_name
      ),
  )

  args["create_target_table_ddl_filepath"] = os.path.join(
      CREATE_TARGET_TABLE_DDL_DIRECTORY,
      CREATE_TARGET_TABLE_DDL_FILENAME_TEMPLATE.format(
          table_name=bigquery_target_table_fully_qualified_name
      ),
  )

  args["create_source_table_ddl_filepath"] = os.path.join(
      SOURCE_TABLE_DDL_DIRECTORY,
      SOURCE_TABLE_DDL_FILENAME_TEMPLATE.format(
          table_name=bigquery_source_table_fully_qualified_name
      ),
  )

  args["copy_rows_filepath"] = os.path.join(
      COPY_ROWS_DIRECTORY,
      COPY_ROWS_FILENAME_TEMPLATE.format(
          source_table=bigquery_source_table_fully_qualified_name,
          destination_table=bigquery_target_table_fully_qualified_name,
      ),
  )
