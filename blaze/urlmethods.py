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

import tempfile
import h5py
import urllib2
import cStringIO
import zlib
import numpy as np
from django.conf import settings 

SITE_HOST = settings.SITE_HOST

def postNPZ(p, post_data):
  """Post the data using npz format"""
  
  # Construct the url and create a npz object
  # KL TODO Support for timeseries data
  url = 'http://{}/ca/{}/{}/npz/{}/{},{}/{},{}/{},{}/'.format(SITE_HOST, p.token, ','.join(p.channels), p.resolution, *p.args)
  fileobj = cStringIO.StringIO()
  np.save(fileobj, post_data)
  cdz = zlib.compress(fileobj.getvalue())

  # Building the post request and checking it posts correctly
  try:
    req = urllib2.Request(url, cdz)
    response = urllib2.urlopen(req)
  except urllib2.HTTPError, e:
    print "Error. {}".format(e)
    raise 

def postHDF5 (p, post_data):
  """Post data using the hdf5"""

  # Build the url and then create a hdf5 object
  url = 'http://{}/ca/{}/{}/hdf5/{}/{},{}/{},{}/{},{}/'.format ( SITE_HOST, p.token, ','.join(p.channels), p.resolution, *p.args )

  tmpfile = tempfile.NamedTemporaryFile ()
  fh5out = h5py.File ( tmpfile.name )
  for idx, channel_name in enumerate(p.channels):
    chan_grp = fh5out.create_group(channel_name)
    chan_grp.create_dataset("CUTOUT", tuple(post_data.shape), post_data.dtype, compression='gzip', data=post_data)
    chan_grp.create_dataset("CHANNELTYPE", (1,), dtype=h5py.special_dtype(vlen=str), data=p.channel_type)
    chan_grp.create_dataset("DATATYPE", (1,), dtype=h5py.special_dtype(vlen=str), data=p.datatype)
  fh5out.close()
  tmpfile.seek(0)
  
  try:
    # Build a post request
    req = urllib2.Request(url,tmpfile.read())
    response = urllib2.urlopen(req)
    return response
  except urllib2.HTTPError,e:
    return e

def getHDF5 (p):
  """Get data using npz. Returns a hdf5 file"""
  
  # Build the url and then create a hdf5 object
  url = 'http://{}/ca/{}/{}/hdf5/{}/{},{}/{},{}/{},{}/'.format(SITE_HOST, p.token, ','.join(p.channels), p.resolution, *p.args)

  # Get the image back
  f = urllib2.urlopen (url)
  tmpfile = tempfile.NamedTemporaryFile()
  tmpfile.write(f.read())
  tmpfile.seek(0)
  h5f = h5py.File(tmpfile.name, driver='core', backing_store=False)
  
  return h5f.get(p.channels[0])['CUTOUT'].value

def postURL ( url, f ):

  req = urllib2.Request(url, f.read())
  response = urllib2.urlopen(req)

  return response

def getURL ( url ):
  """Post the url"""

  try:
    req = urllib2.Request ( url )
    f = urllib2.urlopen ( url )
  except urllib2.HTTPError, e:
    return e.code

  return f
