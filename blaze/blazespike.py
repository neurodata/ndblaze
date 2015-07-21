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

import aerospike
import time

NAME_SPACE = 'test'

class BlazeSpike:

  def __init__(self):
    """Create a connection"""
    
    try:
      config = { 'hosts': [('127.0.0.1', 3000)], 'policies':{'timeout':1000} }
      self.client = aerospike.client(config)
      self.client.connect()
    except Exception as e:
      raise

  def writeData(self, ds, ch, cube_list):

    start = time.time()
    key = (NAME_SPACE, "{}_{}".format(ds.token, ch.getChannelName()), 1)
    self.client.put(key, {'data':cube_list})
    print time.time()-start
    
    #for zidx,cube_data in cube_list:
      #key = (NAME_SPACE, "{}_{}".format(ds.token, ch.getChannelName()), zidx) 
      #data = { 'data' : cube_data }
      #start = time.time()
      #self.client.put(key, data)
      #print time.time()-start

  def readData(self, key_list):

    cube_list = []
    for zidx in zidx_list:
      key = (NAME_SPACE, "{}_{}".format(ds.token,ch.getChannelName()), zidx)
      cube_list.append(self.client.get(key))

    return cube_list
