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
import time
import numpy as np
from operator import itemgetter, div, add, sub, mod, mul

from dataset import Dataset
from blazeredis import BlazeRedis
from ndlib import MortonXYZ, XYZMorton, overwriteMerge_ctype
from urlmethods import postBlosc, getBlosc
from params import Params
from blazecontext import BlazeContext
from tasks import asyncPostBlosc
blaze_context = BlazeContext()

CUBE_DIM = [512,512,16]

class BlazeRdd:

  def __init__(self, ds, ch, res):
    """Create an empty rdd"""
   
    self.ds = ds
    self.ch = ch
    self.res = res
    self.br = BlazeRedis(ds.token, ch.getChannelName(), res)


  def loadData(self, x1,x2,y1,y2,z1,z2):
    """Load data from the region"""
      
    [zimagesz, yimagesz, ximagesz] = self.ds.imagesz[self.res]
    #[xcubedim, ycubedim, zcubedim] = cubedim = self.ds.cubedim[self.res]
    [xcubedim,ycubedim,zcubedim] = cubedim = CUBE_DIM
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
          key_list.append( self.br.generateSIKey(XYZMorton(map(add, start, [x,y,z]))) )
    
    # Internal functions for Spark

    def breakCubes(key, blosc_data):
      """break the cubes into smaller chunks"""
      
      key_array = [token, channel_name, res, x1, x2, y1, y2, z1, z2, time_stamp] = key.split('_')
      [res, x1, x2, y1, y2, z1, z2] = [int(i) for i in key_array[2:][:-1]]
      if blosc_data is None:
        return
      voxarray = blosc.unpack_array(blosc_data)
      
      br = BlazeRedis(token, channel_name, res)

      ds = Dataset(token)
      ch = ds.getChannelObj(channel_name)
      [zimagesz, yimagesz, ximagesz] = ds.imagesz[res]
      #[xcubedim, ycubedim, zcubedim] = cubedim = ds.cubedim[res]
      [xcubedim, ycubedim, zcubedim] = cubedim = CUBE_DIM
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
            cube_list.append((br.generateSIKey(zidx), blosc.pack_array(cube_data.reshape((1,)+cube_data.shape))))
      
      return cube_list[:]
    
    def getBlock(key):
      """Get the block from Redis"""
      [token, ch, res] = key.split('_')[:3]
      br = BlazeRedis(token, ch, res)
      return key, br.getBlock(key)
    
    def postBlosc2((key, post_data)):
      """Calling the celery job from here"""
      asyncPostBlosc.delay((key,post_data))
      # asyncPostBlosc((key, post_data))
      return post_data


    # Get the block-keys for a given list of zindexes, for read trigger
    # blockkey_list = self.br.getBlockKeys(key_list)
    # Get all the block-keys, for memory trigger
    key_list, blockkey_list = self.br.getAllBlockKeys()
    # Sort the block_key list to order cubes on time
    blockkey_list.sort()
    # Creating a RDD for blocks in Redis
    temp_rdd = blaze_context.sc.parallelize(blockkey_list)
    
    # temp_rdd = temp_rdd.filter(lambda k : k is not None).map(lambda k : '_'.join(k.split('_')[:-1])).map(lambda k: breakCubes(*getBlock(k))).flatMap(lambda k : k)
    # temp_rdd = temp_rdd.filter(lambda k : k is not None).map(lambda k: breakCubes(*getBlock(k))).flatMap(lambda k : '_'.join(k.split('_')[:-1]))
    
    # Filter - removes None
    # Map - get block and break it into cubes
    # Filter - removee None
    # flatMap - maps keys and creates k,v pairs
    temp_rdd = temp_rdd.filter(lambda k : k is not None).map(lambda k: breakCubes(*getBlock(k))).filter(lambda k : k is not None).flatMap(lambda k : k)
    
    # Creating a RDD for blocks to be fetched
    # Read trigger
    # zidx_rdd = blaze_context.sc.parallelize(key_list).map(getBlosc)
    # Memory trigger
    zidx_rdd = blaze_context.sc.parallelize(key_list).map(getBlosc)
    
    def mergeCubes(data1, data2):
      """Merge Cubes"""
      
      import ndlib 
      data1 = blosc.unpack_array(data1)
      data2 = blosc.unpack_array(data2)
      # Call vectorize function
      # vec_func = np.vectorize(lambda x,y: x if y == 0 else y)
      # Call ctype function
      ndlib.overwriteMerge_ctype(data1, data2)
      return blosc.pack_array(data1)


    #zidx_rdd.union(temp_rdd).sortByKey().combineByKey(lambda x: x, np.vectorize(lambda x,y: x if y == 0 else y), np.vectorize(lambda x,y: x if y == 0 else y)).map(lambda (k,v) : ((k,p),v)).map(postBlosc2).collect()
    #zidx_rdd.union(temp_rdd).sortByKey().map(lambda (x,y):(x,blosc.unpack_array(y))).combineByKey(lambda x: x, np.vectorize(lambda x,y: x if y == 0 else y), np.vectorize(lambda x,y: x if y == 0 else y)).map(lambda (k,v) : ((k,p),blosc.pack_array(v))).map(postBlosc2).collect()
    #zidx_rdd.union(temp_rdd).map(lambda (x,y):(x,blosc.unpack_array(y))).combineByKey(lambda x: x, np.vectorize(lambda x,y: x if y == 0 else y), np.vectorize(lambda x,y: x if y == 0 else y)).map(lambda (k,v) : ((k,p),blosc.pack_array(v))).map(postBlosc2).collect()
    #zidx_rdd.union(temp_rdd).map(lambda (x,y):(x,blosc.unpack_array(y))).combineByKey(lambda x: x, np.vectorize(lambda x,y: x if y == 0 else y), np.vectorize(lambda x,y: x if y == 0 else y)).map(lambda (x,y):(x,blosc.pack_array(y))).map(postBlosc2).collect()
    
    # Union - union of the 2 lists of fetched cubes and cubes in redis
    # combineByKey - merge the cubes based on our merge function
    # sortByKey - so we can do sequential writes in the backend
    # map - call celery function to post in background
    merged_data = zidx_rdd.union(temp_rdd).combineByKey(lambda x: x, mergeCubes, mergeCubes).sortByKey().map(postBlosc2).collect()

    # return the merged cube back to reader
    return merged_data[0]
