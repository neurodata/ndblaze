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
import MySQLdb
from django.conf import settings

class BlazeDB():
  """MySQL interface for metdata management"""

  def __init__(self):
    """Intialize the database connection"""

    try:
      self.conn = MySQLdb.connect(host = '', user = settings.DATABASES['default']['USER'], passwd = settings.DATABASES['default']['PASSWORD'], db = settings.DBNAME)
    except MySQLdb.Error, e:
      raise

  def getDataset(self, ds):
    """Get the dataset from the database"""

    cursor = self.conn.cursor()
    
    sql = "SELECT datasetid, ximagesz, yimagesz, zimagesz, xoffset, yoffset, zoffset, xvoxelres, yvoxelres, zvoxelres, scalingoption, scalinglevels, starttime, endtime FROM datasets WHERE dataset = '{}';".format(ds.token)
    try:
      cursor.execute(sql)
      r = cursor.fetchone()
    except MySQLdb.Error, e:
      raise
      #logger.warning ("Failed to fetch dataset {}: {}. sql={}".format(e.args[0], e.args[1], sql))
      #raise OCPTILECACHEError("Failed to fetch dataset {}: {}. sql={}".format(e.args[0], e.args[1], sql))
    finally:
      cursor.close()
    
    if r is not None:
      (ds.dsid, ds.ximagesz, ds.yimagesz, ds.zimagesz, ds.xoffset, ds.yoffset, ds.zoffset, ds.xvoxelres, ds.yvoxelres, ds.zvoxelres, ds.scalingoption, ds.scalinglevels, ds.starttime, ds.endtime) = r
    else:
      raise Exception("Dataset not found")
  
  def addDataset (self, ds):
    """Add a dataset to the database"""
    
    cursor = self.conn.cursor()

    try:
      sql = "INSERT INTO datasets (dataset, ximagesz, yimagesz, zimagesz, xoffset, yoffset, zoffset, xvoxelres, yvoxelres, zvoxelres, scalingoption, scalinglevels, starttime, endtime) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}', '{}', '{}');".format(ds.token, ds.ximagesz, ds.yimagesz, ds.zimagesz, ds.xoffset, ds.yoffset, ds.zoffset, ds.xvoxelres, ds.yvoxelres, ds.zvoxelres, ds.scalingoption, ds.scalinglevels, ds.starttime, ds.endtime)
      cursor.execute (sql)

      for ch in ds.channel_list:
        sql = "INSERT INTO channels (channel_name, dataset, channel_type, channel_datatype, startwindow, endwindow) VALUES ('{}','{}','{}','{}','{}','{}');".format(ch.channel_name, ch.dataset, ch.channel_type, ch.channel_datatype, ch.startwindow, ch.endwindow)
        cursor.execute (sql)

      self.conn.commit()
    
    except MySQLdb.Error, e:
      self.conn.rollback()
      raise 
      #logger.warning ("Failed to insert dataset/channel {}: {}. sql={}".format(e.args[0], e.args[1], sql))
      #raise OCPTILECACHEError("Failed to insert dataset/channel {}: {}. sql={}".format(e.args[0], e.args[1], sql))
    finally:
      cursor.close()
  
  def addChannel(self, ch):
    """Add a channel to the channels table"""

    cursor = self.conn.cursor()
    sql = "INSERT INTO channels (channel_name, dataset, channel_type, channel_datatype, startwindow, endwindow) VALUES ('{}','{}','{}','{}','{}','{}');".format(ch.channel_name, ch.dataset, ch.channel_type, ch.channel_datatype, ch.startwindow, ch.endwindow)
    
    try:
      cursor.execute (sql)
      self.conn.commit()
    except MySQLdb.Error, e:
      logger.warning ("Failed to insert channel {}: {}. sql={}".format(e.args[0], e.args[1], sql))
      raise OCPTILECACHEError("Failed to insert channel {}: {}. sql={}".format(e.args[0], e.args[1], sql))
    finally:
      cursor.close()

  def getChannel(self, ds):
    """Get a channel from the channels table"""

    cursor = self.conn.cursor()
    sql = "SELECT channel_name, dataset, channel_type, channel_datatype, startwindow, endwindow FROM channels where dataset='{}';".format(ds.token)

    try:
      cursor.execute (sql)
      from dataset import Channel
      for row in cursor:
        ds.channel_list.append(Channel(*row))
    except MySQLdb.Error, e:
      logger.warning ("Failed to fetch channel. {}: {}. sql={}".format(e.args[0], e.args[1], sql))
      raise OCPTILECACHEError("Failed to fetch channel. {}: {}. sql={}".format(e.args[0], e.args[1], sql))
    finally:
      cursor.close()
