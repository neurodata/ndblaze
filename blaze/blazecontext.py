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

from pyspark import SparkContext, SparkConf
from pyspark.serializers import MarshalSerializer, PickleSerializer

class BlazeContext:

  def __init__(self):
    """Create a spark context"""

    conf = SparkConf().setAppName('blaze')
    #self.sc = SparkContext(conf=conf, serializer=MarshalSerializer(), batchSize=16)
    self.sc = SparkContext(conf=conf, serializer=PickleSerializer(), batchSize=16)
