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
    
  def generateSIKey(self, ds, ch, res, zindex):
    """Genarate the secondary index key"""
    return '{}_{}_{}_{}'.format(ds.token, ch.getChannelName(), res, zindex)

  def getSIKeys(self, ds, ch, res, key_list):
    """Return the value for this data"""
    for key in key_list:
      SIkey = self.generateSIKey(ds, ch, res, key)
      self.pipe.smembers(SIkey)
  
  def putSIKeys(self, ds, ch, res, main_key, key_list):
    """Write the key table"""
    for key in key_list:
      SIkey = self.generateSIKey(ds, ch, res, key)
      self.pipe.sadd(SIkey, main_key)

  def deleteSIKeys(self, main_key, key_list):
    """Removing the secondary key"""
    temp_key = str(main_key) + '_temp'
    self.putSIKeys(temp_key, key_list)
    self.pipe.sdiffstore(main_key, main_key, temp_key)
    self.pipe.delete(temp_key)

  # Primary Index GET/PUT/DELETE
  # Primary Index points to the actual block

  def generatePIKey(self, ds, ch, res, (x1,x2,y1,y2,z1,z2)):
    """Generate the primary index key"""
    return '{}_{}_{}_{}_{}_{}_{}_{}_{}'.format(ds.token, ch.getChannelName(), res, x1, x2, y1, y2, z1, z2)
  
  def putBlock(self, key, voxarray):
    """Insert a single block"""
    self.pipe.set(key, voxarray)
  
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

  def putData(self, ds, ch, res, (x1,x2,y1,y2,z1,z2), voxarray, key_list):
    """Insert the data"""
    self.flushDB()
    key = self.generatePIKey(ds, ch, res, (x1,x2,y1,y2,z1,z2))
    # Insert the block
    self.putBlock(key, voxarray)
    # write the secondary index 
    self.putSIKeys(ds, ch, res, key, key_list)
    self.executePipe()

  def getBlockKeys(self, ds, ch, res, key_list):
    """Read the block keys"""
    self.getSIKeys(key_list)
    return [i.pop() for i in self.executePipe()]

  def deleteData(self, ds, ch, res, SIkey):
    """Delete the data"""
    
    key_list = self.getSIKeys(SIkey)
    for key in key_list:
      self.deleteBlock(key)
    self.deleteSIKeys(SIkey, key_list)

    self.executePipe()
