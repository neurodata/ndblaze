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

from datetime import timedelta

BROKER_URL = 'amqp://guest@localhost'
#CELERY_RESULT_BACKEND = 'rpc://'

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT=['json']
CELERY_TIMEZONE = 'America/New_York'
CELERY_ENABLE_UTC = True

CELERYD_CONCURRENCY = 1

CELERYBEAT_SCHEDULE = {
  'add-every-30-seconds': {
  'task': 'tasks.flushData',
  'schedule': timedelta(seconds=60),
  'args': ()
  },
}