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

import blosc
import numpy as np
from operator import itemgetter, div, mul, add, sub, mod

from ocplib import MortonXYZ, XYZMorton
from urlmethods import postHDF5, getHDF5, postBlosc
from params import Params
from dataset import Dataset

class BlazeRdd:

  def __init__(self, sparkContext, ds, ch, res):
    """Create an empty rdd"""
    
    self.sc = sparkContext
    self.rdd = self.sc.parallelize("")
    self.ds = ds
    self.ch = ch
    self.res = res

  def insertData(self, cube_list):

    def breakCubes((token,channel_name,res,x1,x2,y1,y2,z1,z2), blosc_data):
      """break the cubes into smaller chunks"""

      voxarray = blosc.unpack_array(blosc_data)
      
      ds = Dataset(token)
      ch = ds.getChannelObj(channel_name)
      [zimagesz, yimagesz, ximagesz] = ds.imagesz[res]
      [xcubedim, ycubedim, zcubedim] = cubedim = ds.cubedim[res]
      [xcubedim,ycubedim,zcubedim] = cubedim = [512,512,16]
      [xoffset, yoffset, zoffset] = ds.offset[res]
      
      # Calculating the corner and dimension
      corner = [x1, y1, z1]
      dim = voxarray.shape[::-1][:-1]

      # Round to the nearest largest cube in all dimensions
      [xstart, ystart, zstart] = start = map(div, corner, cubedim)

      znumcubes = (corner[2]+dim[2]+zcubedim-1)/zcubedim - zstart
      ynumcubes = (corner[1]+dim[1]+ycubedim-1)/ycubedim - ystart
      xnumcubes = (corner[0]+dim[0]+xcubedim-1)/xcubedim - xstart
      numcubes = [xnumcubes, ynumcubes, znumcubes]
      offset = map(mod, corner, cubedim)

      data_buffer = np.zeros(map(mul, numcubes, cubedim)[::-1], dtype=voxarray.dtype)
      end = map(add, offset, dim)
      data_buffer[offset[2]:end[2], offset[1]:end[1], offset[0]:end[0]] = voxarray

      cube_list = []
      for z in range(znumcubes):
        for y in range(ynumcubes):
          for x in range(xnumcubes):
            zidx = XYZMorton(map(add, start, [x,y,z]))
           
            # Parameters in the cube slab
            index = map(mul, cubedim, [x,y,z])
            end = map(add, index, cubedim)

            cube_data = data_buffer[index[2]:end[2], index[1]:end[1], index[0]:end[0]]
            cube_list.append((zidx, blosc.pack_array(cube_data)))
      
      return cube_list[:]

    # Inserting data in the rdd
    import time
    start = time.time()
    temp_rdd = self.sc.parallelize(cube_list)
    temp_rdd = temp_rdd.map(lambda (k,v): breakCubes(k,v)[:]).flatMap(lambda k: k)
    
    self.rdd = self.rdd.union(temp_rdd)
    print time.time()-start
    
    # KL TODO trigger for inserting data under memory pressure
    #if self.rdd.count() >= 5:
      ##self.postData()
      #self.flushData()

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
