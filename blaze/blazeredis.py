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

  def writeData(self, ds, ch, cube_list):

    serial_time = 0
    for zidx,cube_data in cube_list:
      key = "{}_{}_{}".format(ds.token, ch.getChannelName(), zidx) 
      start = time.time()
      serial_time += time.time()-start
      self.pipe.set( key, blosc.pack_array(cube_data) )

    print "Serialization:", serial_time
    start = time.time()
    self.pipe.execute()
    print "Inserttion:",time.time()-start

  def readData(self, key_list):

    cube_list = []
    for zidx in zidx_list:
      key = "{}_{}".format(ds.token,ch.getChannelName(), zidx)
      cube_list.append(self.client.get(key))

    return cube_list
