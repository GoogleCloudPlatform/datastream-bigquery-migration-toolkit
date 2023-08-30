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
import os
from typing import Any

logger = logging.getLogger(__name__)


def write(filepath: str, data: str):
  logger.info(f"Writing to file: '{filepath}'")
  logger.debug(f"Data: {data}")

  dirname = os.path.dirname(filepath)
  if not os.path.exists(dirname):
    os.makedirs(dirname)
  with open(filepath, "w") as f:
    f.writelines(data)


def write_json(filepath: str, data: Any):
  logger.info(f"Writing JSON to file: '{filepath}'")
  logger.debug(f"Data: {data}")

  dirname = os.path.dirname(filepath)
  if not os.path.exists(dirname):
    os.makedirs(dirname)
  with open(filepath, "w") as f:
    json.dump(data, f)
