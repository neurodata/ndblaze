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
import zipfile

from pyspark import SparkContext, SparkConf
from pyspark.serializers import MarshalSerializer, PickleSerializer

class BlazeContext:

  def __init__(self):
    """Create a spark context"""
    
    conf = SparkConf().setAppName('blaze')
    # self.sc = SparkContext(conf=conf, serializer=MarshalSerializer(), batchSize=16)
    self.sc = SparkContext(conf=conf, serializer=PickleSerializer(), batchSize=16)
    # self.sc.addPyFile("ndlib/ndlib.py")
    # self.sc.addPyFile("ndlib/ndtype.py")
    # self.sc.addPyFile("blaze/urlmethods.py")
    # self.sc.addPyFile("blaze/dataset.py")
    # self.sc.addPyFile("blaze/blazeredis.py")
    # self.sc.addPyFile("blaze/blazedb.py")
    zip_path = self.addBlazeZip()
    self.sc.addPyFile(zip_path)
    os.remove(zip_path)
    zip_path = self.addNdLibZip()
    self.sc.addPyFile(zip_path)
    os.remove(zip_path)
    zip_path = self.addDjangoZip()
    self.sc.addPyFile(zip_path)
    os.remove(zip_path)

  def addBlazeZip(self):
    """Create a zipped executable of blaze"""
    
    lib_path = os.path.dirname('/home/ubuntu/ndblaze/blaze/')
    zip_path = '/tmp/blaze.zip'
    zip_file = zipfile.PyZipFile(zip_path, mode='w')
    try:
      zip_file.debug = 3
      zip_file.writepy(lib_path)
      return zip_path
    finally:
      zip_file.close()
  
  def addNdLibZip(self):
    """Create a zipped executable of blaze"""
    
    lib_path = os.path.dirname('/home/ubuntu/ndblaze/ndlib/')
    zip_path = '/tmp/ndlib.zip'
    zip_file = zipfile.PyZipFile(zip_path, mode='w')
    try:
      zip_file.debug = 3
      zip_file.writepy(lib_path)
      return zip_path
    finally:
      zip_file.close()
  
  def addDjangoZip(self):
    """Create a zipped executable of blaze"""
    
    lib_path = os.path.dirname('/home/ubuntu/ndblaze/ndblaze/')
    zip_path = '/tmp/ndblaze.zip'
    zip_file = zipfile.PyZipFile(zip_path, mode='w')
    try:
      zip_file.debug = 3
      zip_file.writepy(lib_path)
      return zip_path
    finally:
      zip_file.close()
