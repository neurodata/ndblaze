# Copyright 2014 Open Connectome Project (http://openconnecto.me)
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

#
# Code to load project paths
#

import os, sys

OCPBLAZE_BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), ".." ))
OCPBLAZE_OCPLIB_PATH = os.path.join(OCPBLAZE_BASE_PATH, "ocplib" )
OCPBLAZE_UTIL_PATH = os.path.join(OCPBLAZE_BASE_PATH, "util" )
OCPBLAZE_BLAZE_PATH = os.path.join(OCPBLAZE_BASE_PATH, "blaze" )

sys.path += [ OCPBLAZE_OCPLIB_PATH, OCPBLAZE_UTIL_PATH, OCPBLAZE_BLAZE_PATH ]
