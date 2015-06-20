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

import re
import h5py
import tempfile
from contextlib import closing
import numpy as np
from operator import div, mul, add, sub, mod

#from blazecontext import BlazeContext
from blaze import rdd_map
from ocplib import XYZMorton, MortonXYZ
from dataset import Dataset

def getData(webargs):
  """Return a region of cutout"""

  try:
    # arguments of format token/channel/service/resolution/x,x/y,y/z,z/
    m = re.match("(\w+)/(\w+)/(\w+)/(\d+)/(\d+),(\d+)/(\d+),(\d+)/(\d+),(\d+)/", webargs)
    [token, channel_name, service] = [i for i in m.groups()[:3]]
    [res, x1, x2, y1, y2, z1, z2] = [int(i) for i in m.groups()[3:]]
  except Exception, e:
    print "Wrong arguments"
    raise
  
  # Fetaching the info from OCP backend
  ds = Dataset(token)
  ds.getChannelObj(channel_name)
  [zimagesz, yimagesz, ximagesz] = ds.imagesz[res]
  [xcubedim, ycubedim, zcubedim] = cubedim = ds.cubedim[res]
  [xoffset, yoffset, zoffset] = ds.offset[res]

  # KL TODO Check the bounds here

  # Calculating the corner and dimension
  corner = [x1, y1, z1]
  dim = map(sub, [x2,y2,z2], corner)

  # Round to the nearest largest cube in all dimensions
  [xstart, ystart, zstart] = start = map(div, corner, cubedim)

  znumcubes = (corner[2]+dim[2]+zcubedim-1)/zcubedim - zstart
  ynumcubes = (corner[1]+dim[1]+ycubedim-1)/ycubedim - ystart
  xnumcubes = (corner[0]+dim[0]+xcubedim-1)/xcubedim - xstart
  numcubes = [xnumcubes, ynumcubes, znumcubes]

  voxarray = np.empty(map(mul, numcubes[::-1], cubedim[::-1]), dtype=np.uint8)

  # Generate a list of zindex to cut
  zidx_list = []
  for z in range(znumcubes):
    for y in range(ynumcubes):
      for x in range(xnumcubes):
        zidx_list.append(XYZMorton(map(add, [x,y,z], start)))
  
  zidx_list.sort()
  lowxyz = MortonXYZ(zidx_list[0])

  channel_rdd = rdd_map.getBlazeRdd(token, channel_name, res)
  for zidx,cube_data in channel_rdd.getData(zidx_list):
    curxyz = MortonXYZ(zidx)
    offset = map(mul, map(sub, curxyz, lowxyz), cube_data.shape[::-1])
    end = map(add, offset, cube_data.shape[::-1])
    # Add the data to a larger voxarray
    voxarray[offset[2]:end[2], offset[1]:end[1], offset[0]:end[0]] = cube_data[:]

  # trim the data
  if map(mod, dim, cubedim) == [0,0,0] and map(mod, corner, cubedim) == [0,0,0]:
    pass
  else:
    offset = (map(mod, corner, cubedim))
    end = map(add, offset, dim)
    voxarray = voxarray[offset[2]:end[2], offset[1]:end[2], offset[0]:end[2]]

  # Construct an HDF5 file
  try:
    tmpfile = tempfile.NamedTemporaryFile()
    f5out = h5py.File(tmpfile.name, driver='core', backing_store=True)

    for channel_name in channel_name.split(','):
      changrp = f5out.create_group("{}".format(channel_name))
      changrp.create_dataset("CUTOUT", tuple(voxarray.shape), voxarray.dtype, compression='gzip', data=voxarray)
      changrp.create_dataset("CHANNEL_TYPE", (1,), dtype=h5py.special_dtype(vlen=str), data='image')
      changrp.create_dataset("DATATYPE", (1,), dtype=h5py.special_dtype(vlen=str), data='uint8')
  except Exception, e:
    fh5out.close()
    tmpfile.close()
    raise

  f5out.close()
  tmpfile.seek(0)

  return tmpfile.read()
      

def postData(webargs, post_data):
  """Accept a posted region of cutout"""

  try:
    # arguments of format token/channel/service/resolution/x,x/y,y/z,z/
    m = re.match("(\w+)/(\w+)/(\w+)/(\d+)/(\d+),(\d+)/(\d+),(\d+)/(\d+),(\d+)/", webargs)
    [token, channel_name, service] = [i for i in m.groups()[:3]]
    [res, x1, x2, y1, y2, z1, z2] = [int(i) for i in m.groups()[3:]]
  except Exception, e:
    print "Wrong arguments"
    raise

  with closing (tempfile.NamedTemporaryFile()) as tmpfile:
    
    try:
      # Opening the hdf5 file
      tmpfile.write(post_data)
      tmpfile.seek(0)
      h5f = h5py.File(tmpfile.name, driver='core', backing_store=False)
    
    except Exception, e:
      print "Error opening HDF5 file"
      raise

    # KL TODO Make so that we take in multiple channels
    voxarray = h5f.get(channel_name)['CUTOUT'].value
    # KL TODO check if this matches with backend
    h5_datatype = h5f.get(channel_name)['DATATYPE'].value[0]
    h5_channeltype = h5f.get(channel_name)['CHANNELTYPE'].value[0]
    
    # Fetaching the info from OCP backend
    ds = Dataset(token)
    ds.getChannelObj(channel_name)
    [zimagesz, yimagesz, ximagesz] = ds.imagesz[res]
    [xcubedim, ycubedim, zcubedim] = cubedim = ds.cubedim[res]
    [xoffset, yoffset, zoffset] = ds.offset[res]
    
    # KL TODO Check the bounds here
    
    import pdb; pdb.set_trace()
    # Calculating the corner and dimension
    corner = [x1, y1, z1]
    dim = voxarray.shape[::-1]

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
          cube_list.append((zidx, cube_data))
   
    channel_rdd = rdd_map.getBlazeRdd(token, channel_name, res)
    channel_rdd.insertData(cube_list)
