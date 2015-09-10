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

import numpy as np
from operator import itemgetter

from ocplib import MortonXYZ, XYZMorton
from urlmethods import postHDF5, getHDF5, postBlosc
from params import Params

class BlazeRdd:

  def __init__(self, sparkContext, ds, ch, res):
    """Create an empty rdd"""
    
    self.sc = sparkContext
    self.rdd = self.sc.parallelize("")
    self.ds = ds
    self.ch = ch
    self.res = res

  def insertData(self, cube_list):

    # Inserting data in the rdd
    import time
    start = time.time()
    self.rdd = self.rdd.union(self.sc.parallelize(cube_list))
    print time.time()-start
    
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
      [x, y, z] = MortonXYZ(zidx)

      # Caculate the args and post the request
      self.p.args = (x*128, (x+1)*128, y*128, (y+1)*128, z*16, (z+1)*16)
      print "Posting", zidx
      postHDF5(self.p, post_data)

  def flushData(self):
    """Remove all the data from rdd."""

    import time
    p = Params(self.ds, self.ch, self.res)

    #start = time.time()
    #sorted_list2 = self.rdd.sortByKey().map(lambda x: (p,x)).collect()
    #print "2nd TIME:", time.time()-start
    
    # Sort the list according to sequential list
    #start = time.time()
    #sorted_list2 = self.rdd.sortByKey().zipWithIndex().groupBy(lambda ((x,y),z):x-z).values().map(lambda x: list(x)).keys().map(lambda x: (p,x[0])).collect()
    #print "1st TIME:", time.time()-start

    # Post the url
    start = time.time()
    sorted_list = self.rdd.sortByKey().combineByKey(lambda x: x, np.vectorize(lambda x,y: x if y == 0 else y), np.vectorize(lambda x,y: x if y == 0 else y)).map(lambda x: (p,x))
    sorted_list.map(postBlosc).collect()
    print "TIME:", time.time()-start

    # Clear out the RDD
    self.emptyRDD()

  def emptyRDD(self):
    """Make the RDD empty"""

    # KL TODO Better way to make an exisitng RDD empty. This seems to be the fastest for now
    self.rdd = self.sc.parallelize("")

  def getData(self, zidx_list):
    """Return cubes of data"""

    import time
    start = time.time()
    p = Params(self.ds, self.ch, self.res)
    zidx_rdd = self.sc.parallelize(zidx_list).map(lambda x : (x,p)).map(getHDF5)    
    test = self.rdd.union(zidx_rdd).sortByKey().combineByKey(lambda x: x, np.vectorize(lambda x,y: x if y == 0 else y), np.vectorize(lambda x,y: x if y == 0 else y))
    test.collect()
    print "TIME:",time.time()-start
    
    # KL TODO This is slow. Need to identify ways to make it go faster
    #return_list = zidx_rdd.leftOuterJoin(self.rdd).values().map(lambda x: (p,x)).map(getHDF5)
    return test.toLocalIterator()
