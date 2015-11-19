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

from __future__ import absolute_import

from celery import task
from django.conf import settings

from blaze.urlmethods import postBlosc
from blaze.blazeredis import BlazeRedis

#@task(queue='post')
def asyncPostBlosc((key, post_data)):
  """Post the data asynchronously"""
  
  postBlosc((key, post_data))
  [token, channel, res, zindex] = key.split('_')
  br = BlazeRedis(token, channel, res)
  #br.deleteData(key)
