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

import redis
import time
import blosc

class BlazeRedis:

  def __init__(self):
    """Create a connection"""
    
    try:
      self.client = redis.StrictRedis(host='localhost', port=6379, db=0)
      self.pipe = self.client.pipeline(transaction=False)
    except Exception as e:
      raise

  def writeData(self, ds, ch, voxarray):

    key = "{}_{}".format(ds.token, ch.getChannelName()) 
    self.pipe.set( key, voxarray )

    start = time.time()
    self.pipe.execute()
    print "Insertion:",time.time()-start

  def readData(self, key_list):

    cube_list = []
    for zidx in zidx_list:
      key = "{}_{}".format(ds.token,ch.getChannelName(), zidx)
      cube_list.append(self.client.get(key))

    return cube_list
