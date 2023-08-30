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
import sys
from common.monitoring_consts import USER_AGENT
from google.api_core.exceptions import NotFound
from google.api_core.gapic_v1.client_info import ClientInfo
from google.cloud import datastream_v1
from google.cloud.datastream_v1.types import Stream

logger = logging.getLogger(__name__)


def execute_update_stream(
    stream: Stream,
    datastream_api_endpoint_override: str,
) -> Stream:
  client_options = (
      {"api_endpoint": datastream_api_endpoint_override}
      if datastream_api_endpoint_override
      else {}
  )
  logger.info(
      f"Calling update on stream '{stream.display_name}', client options"
      f" '{client_options}'.."
  )

  client = datastream_v1.DatastreamClient(
      client_options=client_options,
      client_info=ClientInfo(user_agent=USER_AGENT),
  )

  request = datastream_v1.UpdateStreamRequest(stream=stream)

  try:
    operation = client.update_stream(request=request)
    res = operation.result()
  except NotFound:
    logger.error(
        f"ERROR: Stream '{stream.display_name}' not found. Make sure the stream"
        " exists before starting the migration."
    )
    sys.exit(1)

  logging.debug(f"Got result {res}")
  return res
