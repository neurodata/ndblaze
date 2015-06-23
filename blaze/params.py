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


class Params:
  """Arguments Class"""
  
  def __init__ (self, ds, ch, res):
    self.token = ds.token
    self.resolution = res
    self.channels = [ch.getChannelName()]
    self.channel_type = ch.getChannelType()
    self.datatype = ch.getDataType()
    self.cubedim = ds.cubedim[res]

  def __call__ (self):

    return self
