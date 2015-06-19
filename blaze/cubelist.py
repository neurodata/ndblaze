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

from operator import itemgetter

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

    # KL TODO trigger for inserting data under memory pressure
    if self.rdd.count() >= 5:
      #self.postData()
      self.flushData()

  def postData(self):
    """Write a blob of data sequentially"""

    #sorted_list = self.rdd.sortByKey().zipWithIndex().groupBy(lambda ((x,y),z):x-z).collect()
    #sorted_list = self.rdd.sortByKey().zipWithIndex().groupBy(lambda ((x,y),z):x-z).values().map(lambda x: list(x)).max(key=len)
    self.rdd.sortByKey().zipWithIndex().groupBy(lambda ((x,y),z):x-z).values().map(lambda x: list(x)).sortBy(lambda x : len(x), ascending=False)
    print "Largest List", [i[0] for i in [i[0] for i in sorted_list]]
    self.rdd = self.rdd.subtractByKey(self.sc.parallelize(sorted_list.first()).map(itemgetter(0)))
    print "Keys in List", self.rdd.keys().collect()

    # KL TODO combine cubes together to form large cube
    
    for ((zidx,post_data), index) in sorted_list:
      [x, y, z] = ocplib.MortonXYZ(zidx)

      # Caculate the args and post the request
      self.p.args = (x*128, (x+1)*128, y*128, (y+1)*128, z*16, (z+1)*16)
      print "Posting", zidx
      postHDF5(self.p, post_data)

  def flushData(self):
    """Remove all the data from rdd, biggest list first"""

    iterator = self.rdd.sortByKey().zipWithIndex().groupBy(lambda ((x,y),z):x-z).values().map(lambda x: list(x)).toLocalIterator()

    for sorted_list in iterator:
      for ((zidx,post_data), index) in sorted_list:
        [x, y, z] = ocplib.MortonXYZ(zidx)

        # Caculate the args and post the request
        self.p.args = (x*128, (x+1)*128, y*128, (y+1)*128, z*16, (z+1)*16)
        print "Posting", zidx
        postHDF5(self.p, post_data)

    self.emptyRDD()

  def emptyRDD(self):
    """Make the RDD empty"""

    # KL TODO Better way to make an exisitng RDD empty. This seems to be the fastest for now
    self.rdd = self.sc.parallelize("")

  def getData(self, zidx_list):
    """Return cubes of data"""

    zidx_list = zidx_list
    def func1(x):
      if x[0] in zidx_list:
        return x

    return self.rdd.map(func1).toLocalIterator()
