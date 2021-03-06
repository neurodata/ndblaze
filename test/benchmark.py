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

import os
import sys
import random
import argparse
import numpy as np
import tempfile
import h5py
import urllib2
import zlib
import cStringIO

sys.path += [os.path.abspath('../')]
import ndblaze.settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'ndblaze.settings'

import blosc
from ndlib import MortonXYZ
from params import Params

p = Params()
p.token = "blaze1"
p.resolution = 0
p.channels = ['image']
p.window = [0,0]
p.channel_type = "image"
p.datatype = "uint8"
SIZE = 128
ZSIZE = 16

def Benchmark(zidx):
  """Run the Benchmark."""

  i = zidx
  [x,y,z] = MortonXYZ(i)
  p.args = (x*SIZE, (x+1)*SIZE, y*SIZE, (y+1)*SIZE, z*ZSIZE, (z+1)*ZSIZE)
  image_data = np.ones([1,16,SIZE,SIZE], dtype=np.uint8) * random.randint(0,255)
  response = PostBlosc(p, image_data)

def PostHDF5 (p, post_data):
  """Post data using the hdf5 interface"""

  # Build the url and then create a hdf5 object
  url = 'http://{}/{}/{}/hdf5/{}/{},{}/{},{}/{},{}/'.format(SITE_HOST, p.token, ','.join(p.channels), p.resolution, *p.args)

  tmpfile = tempfile.NamedTemporaryFile ()
  fh5out = h5py.File ( tmpfile.name )
  for idx, channel_name in enumerate(p.channels):
    chan_grp = fh5out.create_group(channel_name)
    chan_grp.create_dataset("CUTOUT", tuple(post_data[idx,:].shape), post_data[idx,:].dtype, compression='gzip', data=post_data[idx,:])
    chan_grp.create_dataset("CHANNELTYPE", (1,), dtype=h5py.special_dtype(vlen=str), data=p.channel_type)
    chan_grp.create_dataset("DATATYPE", (1,), dtype=h5py.special_dtype(vlen=str), data=p.datatype)
  fh5out.close()
  tmpfile.seek(0)
  
  try:
    # Build a post request
    req = urllib2.Request(url, tmpfile.read())
    import time
    start = time.time()
    response = urllib2.urlopen(req)
    print time.time()-start
    tmpfile.close()
    return response
  except urllib2.HTTPError,e:
    return e

def PostBlosc (p, post_data):
  """Post data using the npz interface"""
  
  # Build the url and then create a npz object
  url = 'http://{}/blaze/{}/{}/blosc/{}/{},{}/{},{}/{},{}/'.format(SITE_HOST, p.token, ','.join(p.channels), p.resolution, *p.args)

  try:
    # Build a post request
    req = urllib2.Request(url, blosc.pack_array(post_data))
    response = urllib2.urlopen(req)
    return response
  except urllib2.HTTPError,e:
    return e

def main():
  """Take in the arguments"""

  parser = argparse.ArgumentParser(description='Run the Benchmark script')
  parser.add_argument('host', action="store", help='HostName')
  parser.add_argument('number_iterations', action="store", type=int, help='Number of iterations')
  parser.add_argument('number_processes', action="store", type=int, help='Number of processes')

  result = parser.parse_args()

  global SITE_HOST
  SITE_HOST = result.host
  zidx_list = range(result.number_iterations)
  random.shuffle(zidx_list)
  import time
  from multiprocessing import Pool
  pool = Pool(result.number_processes)
  start = time.time()
  pool.map(Benchmark, zidx_list)
  print time.time() - start

if __name__ == '__main__':
  main()
