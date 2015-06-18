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

from cubelist import CubeList

class CubeMap:

  def __init__(self, sparkContext):
    """Create a cube map"""
   
    self.sc = sparkContext
    self.cube_map = {}

  def getCubeRdd(self, token, channel_name, resolution):

    key = (token,channel_name,resolution)
    if key in self.cube_map:
      pass
    else:
      self.cube_map[key] = CubeList(self.sc, token, channel_name, resolution)

    return self.cube_map[key]
