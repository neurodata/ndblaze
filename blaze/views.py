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

import re
import logging
from django.shortcuts import render
from django.http import HttpResponse, HttpResponseNotFound, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import View

from extractcube import postData, getData

class BlazeView(View):

  def get(self, request, webargs):

    try:
      return HttpResponse(getData(webargs), content_type="product/hdf5")
    except Exception, e:
      return HttpResponseBadRequest()

  def post(self, request, webargs):
    """RESTful URL for posting data"""

    try:
      #m = re.match(r"(\w+)/(?P<channel>[\w+,/-]+)?/?hdf5/([\w,/-]+)$", webargs)
      #[token, channel, service, cutout_args] = [i for i in m.groups()]
      postData(webargs, request.body)
      return HttpResponse("Successful", content_type="text/html")

    except Exception, e:
      return HttpResponseBadRequest()
      #logger.warning("Incorrect format for arguments. {}".format(e))
