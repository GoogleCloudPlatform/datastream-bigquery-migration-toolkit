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
import sys
from common.monitoring_consts import USER_AGENT
from google.api_core.exceptions import NotFound
from google.api_core.gapic_v1.client_info import ClientInfo
from google.cloud import datastream_v1
from google.cloud.datastream_v1.types import Stream

logger = logging.getLogger(__name__)


def _pb_to_json(pb):
  return json.loads(type(pb).to_json(pb))


def execute_get_stream(
    project_id: str,
    datastream_region: str,
    stream_id: str,
    datastream_api_endpoint_override: str,
) -> Stream:
  client_options = (
      {"api_endpoint": datastream_api_endpoint_override}
      if datastream_api_endpoint_override
      else {}
  )
  logger.info(
      f"Calling get on stream '{stream_id}', region '{datastream_region}',"
      f" project '{project_id}', client options '{client_options}'.."
  )

  client = datastream_v1.DatastreamClient(
      client_options=client_options,
      client_info=ClientInfo(user_agent=USER_AGENT),
  )

  parent = f"projects/{project_id}/locations/{datastream_region}"
  fully_qualified_stream_id = f"{parent}/streams/{stream_id}"
  request = datastream_v1.GetStreamRequest(name=fully_qualified_stream_id)

  try:
    stream: Stream = client.get_stream(request=request)
  except NotFound:
    logger.error(
        f"ERROR: Stream '{stream_id}' not found. Make sure the stream exists"
        " before starting the migration."
    )
    sys.exit(1)

  return stream
