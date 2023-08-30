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

import re


def single_dataset_table_name(source_schema_name: str, source_table_name: str):
  return _clean_table_name(source_schema_name + "_" + source_table_name)


def dynamic_datasets_table_name(source_table_name: str):
  return _clean_table_name(source_table_name)


def dynamic_datasets_dataset_name(
    dataset_id_prefix: str, source_schema_name: str
):
  return _clean_dataset_name(dataset_id_prefix + source_schema_name)


def _clean_dataset_name(table_name):
  return str(re.sub("[^A-Za-z0-9_]", "_", table_name))


def _clean_table_name(table_name):
  return str(re.sub("[/$.@+]", "_", table_name))
