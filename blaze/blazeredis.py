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

#import redlock
import time
import blosc
import redis

class BlazeRedis:

  def __init__(self, ds, ch, res):
    """Create a connection"""
    
    try:
      #self.client = redis.StrictRedis(host='localhost', port=6379, db=0)
      from ndblaze.settings import REDIS_SERVER
      self.client = redis.StrictRedis(host=REDIS_SERVER, port=6379, db=0)
      #self.dlm = redlock.Redlock([{"host": "localhost", "port": 6379, "db": 0}, ])
      self.pipe = self.client.pipeline(transaction=False)
      self.ch = ch
      self.ds = ds
      self.res = res
    except Exception as e:
      raise
  
  # Helper Functions
  # Wrapper Functions for redis interfaces

  def flushDB(self):
    """Clear the database"""
    self.client.flushdb()

  def executePipe(self):
    """Execute the pipe"""
    return self.pipe.execute()

  # Secondary Index GET/PUT/DELETE
  # Secondary Index points which zindex is contained in which Primary Index
    
  def generateSIKey(self, zindex):
    """Genarate the secondary index key"""
    return '{}_{}_{}_{}'.format(self.ds, self.ch, self.res, zindex)

  def getSIKeys(self, key_list):
    """Return the value for this data"""
    for key in key_list:
      #SIkey = self.generateSIKey(key)
      print "get:", key
      #self.dlm.lock(key,1000)
      self.pipe.smembers(key)
  
  def putSIKeys(self, main_key, key_list):
    """Write the key table"""
    for key in key_list:
      #SIkey = self.generateSIKey(key)
      print "Adding:", key, " to:", main_key
      self.pipe.sadd(key, main_key)

  def deleteSIKeys(self, main_key, key_list):
    """Removing the secondary key"""
    self.pipe.srem(main_key, *key_list)
    #self.dlm.unlock(main_key)

  # Primary Index GET/PUT/DELETE
  # Primary Index points to the actual block

  def generatePIKey(self, x1, x2, y1, y2, z1, z2):
    """Generate the primary index key"""
    import time
    return '{}_{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(self.ds, self.ch, self.res, x1, x2, y1, y2, z1, z2, time.clock())
  
  def putBlock(self, region, voxarray):
    """Insert a single block"""
    key = self.generatePIKey(*region)
    self.pipe.set(key, voxarray)
    return key
  
  # Used by Spark and so does not have pipe
  def getBlock(self, key):
    """Get a single block of data"""
    data = self.client.get(key)
    return data

  def deleteBlock(self, key):
    """Delete a single block"""
    self.pipe.delete(key)

  # Data PUT/GET/DELETE
  # Spark exposed interfaces for reading and writing Data

  def putData(self, region, voxarray, key_list):
    """Insert the data"""
    # self.flushDB()
    # Insert the block
    key = self.putBlock(region, voxarray)
    # write the secondary index 
    self.putSIKeys(key, key_list)
    self.executePipe()

  def getBlockKeys(self, key_list):
    """Read the block keys"""
    
    self.getSIKeys(key_list)
    SIkey_list = self.executePipe()
    # chekcing in case of empty sets
    return [i for i in iter(SIkey_list[0]) if i is not None]
    # return [i.pop() if i else None for i in SIkey_list]

  def deleteData(self, SIkey):
    """Delete the data"""
    
    key_list = self.getBlockKeys([SIkey])
    print key_list
    for key in key_list:
      self.deleteBlock(key)
    self.deleteSIKeys(SIkey, key_list)

    self.executePipe()
