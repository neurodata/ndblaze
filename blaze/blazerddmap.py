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

from blazerdd import BlazeRdd

class BlazeRddMap:

  def __init__(self, sparkContext):
    """Create a RDD map"""
   
    self.sc = sparkContext
    self.rdd_map = {}

  def getBlazeRdd(self, ds, ch, res):

    key = (ds.token, ch.getChannelName(), res)
    if key in self.rdd_map:
      pass
    else:
      self.rdd_map[key] = BlazeRdd(self.sc, ds, ch, res)

    return self.rdd_map[key]
