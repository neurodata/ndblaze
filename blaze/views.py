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

from blazerest import postBloscData, getBloscData
from sparkdir import blaze_context

import logging
logger=logging.getLogger("ndblaze")

class BlazeView(View):

  def get(self, request, webargs):

    try:
      return HttpResponse(getBloscData(webargs, blaze_context), content_type="product/blosc")
    except Exception, e:
      logger.error(e)
      # logger.warning("{}".format(e))
      return HttpResponseBadRequest()

  def post(self, request, webargs):
    """RESTful URL for posting data"""

    try:
      postBloscData(webargs, request.body)
      return HttpResponse("Successful", content_type="text/html")

    except Exception, e:
      # logger.warning("{}".format(e))
      return HttpResponseBadRequest()
