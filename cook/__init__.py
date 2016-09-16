#
# __init__.py
#

import json
import requests
import uuid
import time
from requests_kerberos import HTTPKerberosAuth
from requests.auth import HTTPBasicAuth

class JobClient:
  class Status:
    OK = 0
    ERROR = 1
    UNKNOWN = 2

  __required_keys_list = ['url', 'auth']

  def __init__(self, **kwargs):
    for k in self.__required_keys_list:
      if k in kwargs:
        setattr(self, k, kwargs[k])
      else:
        raise ValueError("{0} parameter is required".format(k))

    if self.auth == "http_basic":
      if 'http_user' not in kwargs:
        raise ValueError("HTTP user is required when authentication is HTTP basic")

      if 'http_password' not in kwargs:
        raise ValueError("HTTP password is required when authentication is HTTP basic")

      self.auth = (kwargs['http_user'], kwargs['http_password'])
    elif auth == "kerberos":
      self.auth = HTTPKerberosAuth()
    else:
      raise ValueError("Authentication type {0} not supported".format(kwargs['auth']))

    if 'batch_request_size' in kwargs:
      self.batch_request_size = kwargs['batch_request_size']
    else:
      self.batch_request_size = 32

    if 'status_update_interval_seconds' in kwargs:
      self.status_update_interval_seconds = kwargs['status_update_interval_seconds']
    else:
      self.status_update_interval_seconds = 10

    if 'request_timeout_seconds' in kwargs:
      self.request_timeout_seconds = kwargs['request_timeout_seconds']
    else:
      self.request_timeout_seconds = 60

    self.scheduler_api_endpoint = "/rawscheduler"
    self.jobs = []

  def __api_get(self, query):
    if not isinstance(query, list):
      query = [query]

    req = []
    for q in query:
      req.append(requests.get(self.url + q, headers = {"Content-Type": "application/json", "Accept": "application/json"}, auth = self.auth, timeout = self.request_timeout_seconds))
    return req

  def __api_delete(self, query):
    if not isinstance(query, list):
      query = [query]

    req = []
    for q in query:
      req.append(requests.delete(self.url + q, headers = {"Content-Type": "application/json", "Accept": "application/json"}, auth = self.auth, timeout = self.request_timeout_seconds))
    return req

  def __api_post(self, query, data):
    return requests.post(self.url + query, headers = {"Content-Type": "application/json", "Accept": "application/json"}, auth = self.auth, data = json.dumps(data), timeout = self.request_timeout_seconds)

  def __batch_request(self, jobs):
    # build a single request
    batch = []
    for i in range(0, len(jobs), self.batch_request_size):
      chunk = jobs[i:i+self.batch_request_size]
      batch.append(["job={0}".format(uuid) for uuid in chunk])

    return batch

  def delete(self, **kwargs):
    if 'jobs' not in kwargs:
      raise ValueError("No jobs given")

    if not isinstance(kwargs['jobs'], list):
      raise ValueError("Jobs UUIDs must be a list")

    return self.__delete(kwargs['jobs'])

  def __delete(self, jobs):
    if len(jobs) > 1:
      req = []
      for r in self.__batch_request(jobs):
        req.append(''.join([self.scheduler_api_endpoint, '?', '&'.join(r)]))
    else:
      req = "{0}?job={1}".format(self.scheduler_api_endpoint, jobs[0])

    ret = []
    for resp in self.__api_delete(req):
      reply = {}
      if resp.status_code == 204:
        status = JobClient.Status.OK
      else:
        # Malformed
        if resp.status_code == 400:
          status = JobClient.Status.ERROR
        # Fobidden
        elif resp.status_code == 403:
          status = JobClient.Status.ERROR
        else:
          status = JobClient.Status.UNKNOWN

        reply['reason'] = resp.content

      reply['status'] = status
      reply['http_code'] = resp.status_code

      ret.append(reply)

    return ret

  def query(self, **kwargs):
    if 'jobs' not in kwargs:
      raise ValueError("No jobs given")

    if not isinstance(kwargs['jobs'], list):
      raise ValueError("Jobs UUIDs must be a list")

    return self.__query(kwargs['jobs'])

  def __query(self, jobs):
    req = None

    if len(jobs) > 1:
      req = []
      for r in self.__batch_request(jobs):
        req.append(''.join([self.scheduler_api_endpoint, '?', '&'.join(r)]))
    else:
      req = "{0}?job={1}".format(self.scheduler_api_endpoint, jobs[0])

    ret = []
    for resp in self.__api_get(req):
      reply = {}
      if resp.status_code == 200:
        status = JobClient.Status.OK

        reply['data'] = resp.json()
      else:
        # Malformed
        if resp.status_code == 400:
          status = JobClient.Status.ERROR
        # Fobidden
        elif resp.status_code == 403:
          status = JobClient.Status.ERROR
        # Not found
        elif resp.status_code == 404:
          status = JobClient.Status.ERROR
        else:
          status = JobClient.Status.UNKNOWN

        reply['reason'] = resp.content

      reply['status'] = status
      reply['http_code'] = resp.status_code

      ret.append(reply)

    return ret

  def submit(self, **kwargs):
    if 'jobs' not in kwargs:
      raise ValueError("No jobs given")

    if not isinstance(kwargs['jobs'], list):
      raise ValueError("Jobs definitions must be a list")

    return self.__submit(kwargs['jobs'])

  def __submit(self, jobs):
    data = {'jobs': jobs}
    for j in data['jobs']:
      # generate a random UUID if absent
      if 'uuid' not in j:
        j['uuid'] = str(uuid.uuid1())

    reply = {}
    resp = self.__api_post(self.scheduler_api_endpoint, data)
    if resp.status_code == 201:
      status = JobClient.Status.OK
      reply['data'] = [j['uuid'] for j in data['jobs']]
    else:
      if resp.status_code == 400:
        status = JobClient.Status.ERROR
      elif resp.status_code == 401:
        status = JobClient.Status.ERROR
      elif resp.status_code == 422:
        status = JobClient.Status.ERROR
      else:
        status = JobClient.Status.UNKNOWN

      reply['reason'] = resp.content

    reply['status'] = status
    reply['http_code'] = resp.status_code

    return reply

  def wait(self, jobs):
    while True:
      # check the status of the job if it's completed

      for resp in self.query(jobs = jobs):
        if resp['status'] == JobClient.Status.OK:
          for job in resp['data']:
            if job['status'] == 'completed':
              yield(job)
              jobs.remove(job['uuid'])

      if len(jobs) > 0:
        time.sleep(self.status_update_interval_seconds)
      else:
        break
