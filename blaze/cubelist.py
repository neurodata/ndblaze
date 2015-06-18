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

import ocplib
from params import Params
from urlmethods import postHDF5

class CubeList:

  def __init__(self, sparkContext, token, channel_name, res):
    """Create an empty rdd"""
    
    self.sc = sparkContext
    self.rdd = self.sc.parallelize("")
    self.p = Params()
    self.p.token = token
    self.p.channels = [channel_name]
    self.p.resolution = res
    self.p.channel_type = 'image'
    self.p.datatype = 'uint8'

  def insertData(self, cube_list):

    # Inserting data in the rdd
    self.rdd = self.rdd.union(self.sc.parallelize(cube_list))

    # trigger for inserting data in the background
    if self.rdd.count() >= 5:
      self.postData()

  def postData(self):
    """Write a blob of data sequentially"""

    import pdb; pdb.set_trace()
    sorted_list = self.rdd.sortByKey().zipWithIndex().groupBy(lambda ((x,y),z):x-z).collect()

    # KL TODO combine cubes together to form large cube
    
    for index,iterable in sorted_list:
      for ((zidx,post_data), index) in iterable:
        [x, y, z] = ocplib.MortonXYZ(zidx)

        # Caculate the args and post the request
        self.p.args = (x*128, (x+1)*128, y*128, (y+1)*128, z*16, (z+1)*16)
        postHDF5(self.p, post_data)
