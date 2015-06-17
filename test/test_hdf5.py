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

import random
import numpy as np
from params import Params
from postmethods import postHDF5

p = Params()
p.token = "unittest_rw"
p.resolution = 0
p.channels = ['IMAGE1']
p.window = [0,0]
p.channel_type = "image"
p.datatype = "uint8"

class Test_Hdf5:

  def test_simple(self):
    """Test a simple post"""
  
  p.args = (0,128,0,128,0,16)
  image_data = np.ones([1,16,128,128], dtype=np.uint8) * random.randint(0,255)
  response = postHDF5(p, image_data)

  p.args = (128,384,128,384,0,16)
  image_data = np.ones([1,16,256,256], dtype=np.uint8) * random.randint(0,255)
  response = postHDF5(p, image_data)
  
  p.args = (128,384,128,384,16,32)
  image_data = np.ones([1,16,256,256], dtype=np.uint8) * random.randint(0,255)
  response = postHDF5(p, image_data)
