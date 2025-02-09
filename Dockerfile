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

FROM google/cloud-sdk:slim

COPY migration_toolkit /migration
COPY LICENSE /
COPY README.md /
COPY CONTRIBUTING.md /

RUN apt -y install python3-venv
ENV PATH="/opt/venv/bin:$PATH"

RUN python3 -m venv /opt/venv && \
    . /opt/venv/bin/activate && \
    pip install -U pip && \
    pip install -U -r ./migration/requirements.txt

CMD ["source", "/opt/venv/bin/activate"]
