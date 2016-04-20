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

from memsql.common import database
import time

class BlazeMem:

  def __init__(self):
    """Create a connection"""
    
    try:
      self.client = database.connect(host='localhost', port=3307, user='brain', password="88brain88" ,database='test')
    except Exception as e:
      raise

  def writeData(self, ds, ch, cube_list):

    zlist = []
    datalist = []
    for zidx,cube_data in cube_list:
      zlist.append(zidx)
      datalist.append(cube_data)
    
    import pdb; pdb.set_trace()
    sql = "INSERT INTO {} (zidx,value) VALUES {}".format('key_value', ",".join(str(i) for i in zip(zlist,datalist)))
    start = time.time()
    self.client.execute(sql)
    print "Inserttion:",time.time()-start

  def readData(self, key_list):

    cube_list = []
    for zidx in zidx_list:
      key = "{}_{}".format(ds.token,ch.getChannelName(), zidx)
      cube_list.append(self.client.get(key))

    return cube_list
