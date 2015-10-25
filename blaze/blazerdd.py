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
from operator import itemgetter, div, add, sub, mod, mul

from ocplib import MortonXYZ, XYZMorton
from urlmethods import postHDF5, getHDF5, postBlosc, getBlosc
from params import Params
from dataset import Dataset
from blazeredis import BlazeRedis
from blazecontext import BlazeContext
from tasks import asyncPostBlosc
blaze_context = BlazeContext()

class BlazeRdd:

  def __init__(self, ds, ch, res):
    """Create an empty rdd"""
   
    self.sc = blaze_context.sc
    self.rdd = self.sc.parallelize("")
    self.ds = ds
    self.ch = ch
    self.res = res
    self.br = BlazeRedis()

  def loadData(self, x1,x2,y1,y2,z1,z2):
    """Load data from the region"""
      
    [zimagesz, yimagesz, ximagesz] = self.ds.imagesz[self.res]
    #[xcubedim, ycubedim, zcubedim] = cubedim = self.ds.cubedim[self.res]
    [xcubedim,ycubedim,zcubedim] = cubedim = [512,512,16]
    [xoffset, yoffset, zoffset] = self.ds.offset[self.res]
    p = Params(self.ds, self.ch, self.res)
    
    # Calculating the corner and dimension
    corner = [x1, y1, z1]
    dim = [x2,y2,z2]

    # Round to the nearest largest cube in all dimensions
    [xstart, ystart, zstart] = start = map(div, corner, cubedim)

    znumcubes = (corner[2]+dim[2]+zcubedim-1)/zcubedim - zstart
    ynumcubes = (corner[1]+dim[1]+ycubedim-1)/ycubedim - ystart
    xnumcubes = (corner[0]+dim[0]+xcubedim-1)/xcubedim - xstart
    numcubes = [xnumcubes, ynumcubes, znumcubes]
    offset = map(mod, corner, cubedim)

    key_list = []
    for z in range(znumcubes):
      for y in range(ynumcubes):
        for x in range(xnumcubes):
          key_list.append(XYZMorton(map(add, start, [x,y,z])))
    
    def postBlosc2(((zidx,p), post_data)):
      """Calling the celery job from here"""
      asyncPostBlosc.delay(((zidx,p),post_data))

    import pdb; pdb.set_trace()
    temp_rdd = self.insertData(key_list)
    zidx_rdd = self.sc.parallelize(key_list).map(lambda x: (x,p)).map(getBlosc)
    middle_stage = zidx_rdd.union(temp_rdd).collect()
    self.sc.parallelize(middle_stage).sortByKey().combineByKey(lambda x: x, np.vectorize(lambda x,y: x if y == 0 else y), np.vectorize(lambda x,y: x if y == 0 else y)).map(lambda (k,v) : ((k,p),v)).map(postBlosc2).collect()
    #test = temp_rdd.collect() 

  def insertData(self, key_list):

    def breakCubes(key, blosc_data):
      """break the cubes into smaller chunks"""
      
      key_array = [token,channel_name,res,x1,x2,y1,y2,z1,z2] = key.split('_')
      [res,x1,x2,y1,y2,z1,z2] = [int(i) for i in key_array[2:]]
      voxarray = blosc.unpack_array(blosc_data)
      
      ds = Dataset(token)
      ch = ds.getChannelObj(channel_name)
      [zimagesz, yimagesz, ximagesz] = ds.imagesz[res]
      #[xcubedim, ycubedim, zcubedim] = cubedim = ds.cubedim[res]
      [xcubedim, ycubedim, zcubedim] = cubedim = [512,512,16]
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
            #cube_list.append((zidx, blosc.pack_array(cube_data)))
            cube_list.append((zidx, cube_data))
      
      return cube_list[:]
    
    def getBlock(key):
      """Get the block from Redis"""
      br = BlazeRedis()
      return (key,br.getBlock(key))

    data = self.br.readData(key_list)
    
    # Inserting data in the rdd
    temp_rdd = self.sc.parallelize(data)
    #test = temp_rdd.flatMap(lambda k : k.split(',')).distinct().map(lambda k: getBlock(k)).collect()
    temp_rdd = temp_rdd.flatMap(lambda k : k.split(',')).distinct().map(lambda k : getBlock(k)).filter(lambda (k,v) : k != '').map(lambda (k,v): breakCubes(k,v)[:]).flatMap(lambda k : k)
    return temp_rdd
