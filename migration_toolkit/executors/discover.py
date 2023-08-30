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

import json
import logging
from typing import Dict
from common.file_writer import write_json
from common.monitoring_consts import USER_AGENT
from common.source_type import SourceType
from google.api_core.gapic_v1.client_info import ClientInfo
from google.cloud import datastream_v1

logger = logging.getLogger(__name__)

SOURCE_TYPE_TO_RDBMS: Dict[SourceType, str] = {
    SourceType.MYSQL: "mysql_rdbms",
    SourceType.ORACLE: "oracle_rdbms",
}
SOURCE_TYPE_TO_SCHEMAS: Dict[SourceType, str] = {
    SourceType.MYSQL: "mysql_databases",
    SourceType.ORACLE: "oracle_schemas",
}
SOURCE_TYPE_TO_SCHEMA: Dict[SourceType, str] = {
    SourceType.MYSQL: "database",
    SourceType.ORACLE: "schema",
}
SOURCE_TYPE_TO_TABLES: Dict[SourceType, str] = {
    SourceType.MYSQL: "mysql_tables",
    SourceType.ORACLE: "oracle_tables",
}


def _pb_to_json(pb):
  return json.loads(type(pb).to_json(pb))


def _build_data_object(source_type: SourceType, schema: str, table: str):
  return {
      SOURCE_TYPE_TO_RDBMS[source_type]: {
          SOURCE_TYPE_TO_SCHEMAS[source_type]: [{
              SOURCE_TYPE_TO_SCHEMA[source_type]: schema,
              SOURCE_TYPE_TO_TABLES[source_type]: [{"table": table}],
          }]
      }
  }


def execute_discover(
    connection_profile_name: str,
    source_type: SourceType,
    source_table_name: str,
    source_schema_name: str,
    datastream_api_endpoint_override: str,
    filepath: str,
):
  logger.info(
      f"Calling discover on connection profile '{connection_profile_name}'.."
  )

  client_options = (
      {"api_endpoint": datastream_api_endpoint_override}
      if datastream_api_endpoint_override
      else {}
  )

  client = datastream_v1.DatastreamClient(
      client_options=client_options,
      client_info=ClientInfo(user_agent=USER_AGENT),
  )

  parent, connection_profile_simple_name = connection_profile_name.split(
      "/connectionProfiles/"
  )

  request = datastream_v1.DiscoverConnectionProfileRequest(
      connection_profile_name=connection_profile_name,
      full_hierarchy=True,
      parent=parent,
      **_build_data_object(
          source_type=source_type,
          schema=source_schema_name,
          table=source_table_name,
      ),
  )

  resp = client.discover_connection_profile(request=request)
  resp = _pb_to_json(resp)
  write_json(filepath=filepath, data=resp)
