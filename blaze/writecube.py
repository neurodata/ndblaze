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

from util.urlmethods import postNPZ, postHDF5
from params import Params

class WriteCube:

  def __init__(self, token, channel):
    """Some parameters"""
    self.token = token
    self.channel = channel

  def writeCube():
    """Write a blob of data back sequentially"""

    # Get the rdd

    # iterate over the keys and merge the cubes together for sequential ones
    test.zipWithIndex().groupBy(lambda (x,y):x[0]-y).collect()


    # post the request
    p = Params()
    p.token = self.token
    p.channels = [self.channel]
    p.resolution = self.resolution
    p.args = (x1, x2, y1, y2, z1, z2)
    postHDF5(p, post_data)
