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

  def flushDB(self):
    """Clear the database"""
    self.client.flushdb()

  def executePipe(self):
    """Execute the pipe"""
    return self.pipe.execute()

  def writeKeys(self, main_key, key_list):
    """Write the key table"""
   
    for key in key_list:
      self.pipe.append(key, ","+main_key)
  
  def generateKey(self, ds, ch, res, (x1,x2,y1,y2,z1,z2)):
    """Generate the key"""

    return '{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(ds.token, ch.getChannelName(), res, x1, x2, y1, y2, z1, z2)

  def writeData(self, ds, ch, res, (x1,x2,y1,y2,z1,z2), voxarray, key_list):
    """Insert the data"""
    
    self.flushDB()
    key = self.generateKey(ds, ch, res, (x1,x2,y1,y2,z1,z2))
    self.pipe.set( key, voxarray )
    
    self.writeKeys(key, key_list)

    start = time.time()
    self.executePipe()
    print "Insertion:",time.time()-start

  def getKeys(self, key_list):
    """Return the value for this data"""
    
    for key in key_list:
      self.pipe.get(key)
      self.pipe.delete(key)
  
  def getBlock(self, key):
    """Get a single block of data"""
    data = self.client.get(key)
    self.client.delete(key)
    
    return data

  def readData(self, key_list):
    """Read the data"""
    
    self.getKeys(key_list)
    return self.executePipe()[0::2]
