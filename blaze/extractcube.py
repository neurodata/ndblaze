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

#from blazecontext import BlazeContext
from blaze import cube_map
import ocplib

def postData(webargs, post_data):
  """Parse the arguments"""

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
    # Not used for now
    h5_datatype = h5f.get(channel_name)['DATATYPE'].value[0]
    h5_channeltype = h5f.get(channel_name)['CHANNELTYPE'].value[0]
    
    # KL TODO Get this via projinfo from OCP
    [zimagesz, yimagesz, ximagesz] = [10000, 10000, 100]
    [zvoxarray, yvoxarray, xvoxarray] = voxarray.shape
    [xcubedim, ycubedim, zcubedim] = cubedim = [128, 128, 16]
    [xoffset, yoffset, zoffset] = [0, 0, 0]

    cube_list = []
    for z in range(z1, z2, zcubedim):
      for y in range(y1, y2, ycubedim):
        for x in range(x1, x2, xcubedim):
          zidx = ocplib.XYZMorton([(x-xoffset)/xcubedim, (y-yoffset)/ycubedim, (z-zoffset)/zcubedim])
         
          # Parameters in the cube slab
          xmin = x-x1 
          ymin = y-y1
          zmin = z-z1
          xmax = min(xvoxarray, xmin+xcubedim)
          ymax = min(yvoxarray, ymin+ycubedim)
          zmax = min(zvoxarray, zmin+zcubedim)

          print xmin, xmax, ymin, ymax, zmin, zmax
          cube_data = voxarray[zmin:zmax, ymin:ymax, xmin:xmax]
          cube_list.append((zidx,cube_data))
    
    cube_rdd = cube_map.getList(token, channel_name)
    cube_rdd.insertData(cube_list)

    import pdb; pdb.set_trace()
    print "Testing"
