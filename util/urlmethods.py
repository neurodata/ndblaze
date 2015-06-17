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

import urllib2
import cStringIO
import zlib

# KL TODO Read from django
SITE_HOST = "localhost:8080"

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
    req = urllib.Request(url, cdz)
    response = urllib2.urlopen(req)
  except urllib2.HTTPError, e:
    print "Error. {}".format(e)
    raise 
