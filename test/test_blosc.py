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
import sys
import random
import numpy as np
import random

sys.path += [os.path.abspath('..')]
import ndblaze.settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'ndblaze.settings'

# import settings
import ndlib
from params import Params
from postmethods import postBlosc, getBlosc

p = Params()
p.token = "blaze1"
p.resolution = 0
p.channels = ['image2']
p.window = [0,0]
p.channel_type = "image"
p.datatype = "uint32"

class Test_Blosc:

  def test_simple(self):
    """Test a simple post"""

  # # Posting zindex 0
  # [x,y,z] = ndlib.MortonXYZ(0)
  # p.args = (x*512, (x+1)*512, y*512, (y+1)*512, z*16, (z+1)*16)
  # image_data = np.ones([1,16,512,512], dtype=np.uint32) * 50
  # response = postBlosc(p, image_data)
  # # Posting zindex 0
  # [x,y,z] = ndlib.MortonXYZ(0)
  # p.args = (x*512, (x+1)*512, y*512, (y+1)*512, z*16, (z+1)*16)
  # image_data = np.zeros([1,16,512,512], dtype=np.uint32)
  # image_data[0,0,100,100] = 2
  # response = postBlosc(p, image_data)
  # # Posting zindex 0
  # [x,y,z] = ndlib.MortonXYZ(0)
  # p.args = (x*512, (x+1)*512, y*512, (y+1)*512, z*16, (z+1)*16)
  # image_data = np.zeros([1,16,512,512], dtype=np.uint32)
  # image_data[0,0,100,100] = 1
  # response = postBlosc(p, image_data)
  
  zidx_list = range(0,1000)
  random.shuffle(zidx_list)
  # zidx_list = [random.randint(0,1000) for i in zidx_list]
  for i in zidx_list:
    [x,y,z] = ndlib.MortonXYZ(i)
    p.args = (x*512, (x+1)*512, y*512, (y+1)*512, z*16, (z+1)*16)
    image_data = np.ones([1,16,512,512], dtype=np.uint32) * random.randint(0,255)
    response = postBlosc(p, image_data)
  
  [x,y,z] = ndlib.MortonXYZ(0)
  p.args = (x*512, (x+1)*512, y*512, (y+1)*512, z*16, (z+1)*16)
  data = getBlosc(p)

  # assert (data[0,0,100,100] == 1)
  # pass
  # # Posting zindex 0
  # [x,y,z] = ndlib.MortonXYZ(0)
  # p.args = (x*128, (x+1)*128, y*128, (y+1)*128, z*16, (z+1)*16)
  # image_data = np.ones([1,16,128,128], dtype=np.uint8) * random.randint(0,255)
  # response = postBlosc(p, image_data)
  
  # # Posting zindex 6
  # [x,y,z] = ndlib.MortonXYZ(6)
  # p.args = (x*128, (x+1)*128, y*128, (y+1)*128, z*16, (z+1)*16)
  # image_data = np.ones([1,16,128,128], dtype=np.uint8) * random.randint(0,255)
  # response = postBlosc(p, image_data)

  # # Posting zindex 8
  # [x,y,z] = ndlib.MortonXYZ(8)
  # p.args = (x*128, (x+1)*128, y*128, (y+1)*128, z*16, (z+1)*16)
  # image_data = np.ones([1,16,128,128], dtype=np.uint8) * random.randint(0,255)
  # response = postBlosc(p, image_data)
  
  # # Posting zindex 5
  # [x,y,z] = ndlib.MortonXYZ(5)
  # p.args = (x*128, (x+1)*128, y*128, (y+1)*128, z*16, (z+1)*16)
  # image_data = np.ones([1,16,128,128], dtype=np.uint8) * random.randint(0,255)
  # response = postBlosc(p, image_data)
  
  # # Posting zindex 1
  # [x,y,z] = ndlib.MortonXYZ(1)
  # p.args = (x*128, (x+1)*128, y*128, (y+1)*128, z*16, (z+1)*16)
  # image_data = np.ones([1,16,128,128], dtype=np.uint8) * random.randint(0,255)
  # response = postBlosc(p, image_data)
  
  # # Posting zindex 2
  # [x,y,z] = ndlib.MortonXYZ(2)
  # p.args = (x*128, (x+1)*128, y*128, (y+1)*128, z*16, (z+1)*16)
  # image_data = np.ones([1,16,128,128], dtype=np.uint8) * random.randint(0,255)
  # response = postBlosc(p, image_data)

  # # Posting zindex 3
  # [x,y,z] = ndlib.MortonXYZ(3)
  # p.args = (x*128, (x+1)*128, y*128, (y+1)*128, z*16, (z+1)*16)
  # image_data = np.ones([1,16,128,128], dtype=np.uint8) * random.randint(0,255)
  # response = postBlosc(p, image_data)
  
  # # Posting zindex 7
  # [x,y,z] = ndlib.MortonXYZ(7)
  # p.args = (x*128, (x+1)*128, y*128, (y+1)*128, z*16, (z+1)*16)
  # image_data = np.ones([1,16,128,128], dtype=np.uint8) * random.randint(0,255)
  # response = postBlosc(p, image_data)
