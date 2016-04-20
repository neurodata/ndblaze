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

import pip
import os
import sys
import MySQLdb

sys.path += [os.path.abspath('../')]
os.environ['DJANGO_SETTINGS_MODULE'] = 'ndblaze.settings'
from django.conf import settings

class Installer:

  def __init__ (self):
    pass

  def createDatabase(self):
    """Creating the database for ndblaze"""
    
    try:
      self.conn = MySQLdb.connect (host = settings.DATABASES['default']['HOST'], user = settings.DATABASES['default']['USER'], passwd = settings.DATABASES['default']['PASSWORD'])
    except MySQLdb.Error, e:
      raise Exception("Cannot connect to the MySQL server.")

    try:
      cursor = self.conn.cursor()
      cursor.execute("CREATE DATABASE {};".format(settings.DBNAME))
    except MySQLdb.Error, e:
      raise Exception("Cannot create the database {}. {}".format(settings.DBNAME, e))
    finally:
      self.conn.close()

    try:
      self.conn = MySQLdb.connect (host = settings.DATABASES['default']['HOST'], user = settings.DATABASES['default']['USER'], passwd = settings.DATABASES['default']['PASSWORD'], db = settings.DBNAME)
    except MySQLdb.Error, e:
      raise Exception("Cannot connect to the database {}. {}".format(settings.DBNAME, e))

    try:
      cursor = self.conn.cursor()
      # creating the datasets table
      cursor.execute("CREATE TABLE datasets (dataset VARCHAR(255) PRIMARY KEY, datasetid INT UNIQUE AUTO_INCREMENT, ximagesz INT, yimagesz INT, zimagesz INT, xoffset INT, yoffset INT, zoffset INT, xvoxelres DOUBLE, yvoxelres DOUBLE, zvoxelres DOUBLE, scalingoption INT, scalinglevels INT, starttime INT, endtime INT);")
      cursor.execute("CREATE TABLE channels (channel_name VARCHAR(255) ,dataset VARCHAR(255) REFERENCES datasets(dataset), PRIMARY KEY (channel_name,dataset), channel_type VARCHAR(255), channel_datatype VARCHAR(255), startwindow INT, endwindow INT);")
      self.conn.commit()
    except MySQLdb.Error, e:
      raise Exception("Cannot create the tables for the database {}, {}".format(settings.DBNAME, e))
    finally:
      self.conn.close()
    
    def pipInstall(self):
      """Installing all the pip packages"""

      print "Does Nothing"

    def restart(self):
      """Restart all the services"""

def main():
  inst = Installer()
  inst.createDatabase()

if __name__ == '__main__':
  main()
