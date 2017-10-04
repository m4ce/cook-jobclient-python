import getpass
import json
import logging
import time
from uuid import UUID, uuid1
from datetime import datetime

import requests
from requests import HTTPError
from schema import Schema, And, Or, Use, Optional

from .utils import generate_batch_request
from .exceptions import JobClientError

logger = logging.getLogger(__name__)


class JobClient(object):
    _job_schema = Schema([{
        Optional('name'): And(basestring, lambda s: len(s) > 0),
        Optional('uuid'): And(basestring, lambda s: len(s) > 0 and UUID(s)),
        Optional('executor'): And(basestring, lambda s: s in ('mesos', 'cook')),
        Optional('priority'): And(Use(int), lambda n: (0 <= n <= 100)),
        'max_retries': And(Use(int), lambda n: n > 0),
        Optional('max_runtime'): And(Use(long), lambda n: n > 0),
        Optional('expected_runtime'): And(Use(long), lambda n: n > 0),
        Optional('cpus'): And(Or(int, float), lambda n: n > 0),
        Optional('mem'): And(Or(int, float), lambda n: n > 0),
        Optional('gpus'): And(Use(int), lambda n: n >= 0),
        Optional('ports'): And(int, lambda n: n >= 0),
        Optional('uris'): list,
        Optional('env'): dict,
        Optional('constraints'): list,
        Optional('disable_mea_culpa_retries'): bool,
        Optional('container'): dict,
        Optional('command'): basestring
    }])
    """Schema: Validation schema for submitting jobs"""

    _job_states = list(['success', 'running', 'failed', 'completed', 'waiting'])
    """list: list of possible states a job can be in"""

    # API endpoints
    _scheduler_endpoint = '/rawscheduler'
    """str: the API endpoint for scheduling jobs"""

    _list_endpoint = '/list'
    """str: the API endpoint for listing scheduled jobs"""

    _retry_endpoint = '/retry'
    """str: the API endpoint for retrying jobs"""

    def __init__(self, url, auth='http_basic', http_user=None, http_password=None, batch_request_size=32,
                 status_update_interval_secs=10, request_timeout_secs=60, default_job_settings={'max_retries': 1}):
        """Initialize Cook Job Client

        Args:
            url (str): Cook Scheduler REST API URL
            auth (str): Authentication method, can be http_basic or kerberos
            http_user (str or None): Username for HTTP basic authentication
            http_password (str or None): Password for HTTP basic authentication
            batch_request_size (int): Request size when performing batch requests
            status_update_interval_secs (int): Polling interval to wait on job's status updates
            request_timeout_secs (int): HTTP request timeout
            default_job_settings (dict): Default parameters for submitted jobs
        """
        self._auth = None

        if auth == 'http_basic':
            assert http_user is not None, 'HTTP user is required when authentication is HTTP basic'
            assert http_password is not None, 'HTTP password is required when authentication is HTTP basic'

            self._auth = (http_user, http_password)
        elif auth == 'kerberos':
            from requests_kerberos import HTTPKerberosAuth
            self._auth = HTTPKerberosAuth()
        else:
            raise ValueError(
                "Authentication type {} not supported".format(auth))

        self._url = url
        self._batch_request_size = batch_request_size
        self._status_update_interval_secs = status_update_interval_secs
        self._request_timeout_secs = request_timeout_secs
        self._default_job_settings = default_job_settings

    def get_url(self):
        """Returns the Cook API URL
        
        Returns:
            str: The URL
        """
        return self._url

    def get_auth(self):
        """Returns the authentication method

        Returns:
            str: The auth method
        """
        return self._auth

    def get_default_job_settings(self):
        """Returns the default job settings

        Returns:
            dict: The job settings
        """
        return self._default_job_settings

    def _api_get(self, query):
        """Perform a HTTP GET request

        Args:
            query (list or str): HTTP query to execute

        Returns:
            list: One or more HTTP result(s)
        """
        if not isinstance(query, list):
            query = [query]

        req = list()
        with requests.Session() as session:
            for q in query:
                r = session.get(self._url + q, headers={'Content-Type': 'application/json',
                                                        'Accept': 'application/json'}, auth=self._auth,
                                timeout=self._request_timeout_secs)
                r.raise_for_status()
                req.append(r)
        return req

    def _api_delete(self, query):
        """Perform a HTTP DELETE request

        Args:
            query (list or str): HTTP query to execute

        Returns:
            list: One or more HTTP result(s)
        """
        if not isinstance(query, list):
            query = [query]

        req = list()
        for q in query:
            r = requests.delete(self._url + q, headers={'Content-Type': 'application/json',
                                                        'Accept': 'application/json'}, auth=self._auth,
                                timeout=self._request_timeout_secs)
            r.raise_for_status()
            req.append(r)
        return req

    def _api_post(self, query, data):
        """Perform a HTTP POST request

        Args:
            query (list or str): HTTP query to execute
            data (dict): Data to post

        Returns:
            requests.Response: the HTTP response
        """
        r = requests.post(self._url + query,
                          headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
                          auth=self._auth, data=json.dumps(data), timeout=self._request_timeout_secs)
        r.raise_for_status()
        return r

    def _batch_request(self, jobs):
        """Create a batch request by slicing up a given list of jobs

        Args:
            jobs (list): List of jobs

        Returns:
            list: The generated batch request
        """
        return generate_batch_request(jobs, self._batch_request_size)

    def delete(self, jobs):
        """Delete one or more jobs

        Args:
            jobs (list): Jobs to delete

        Raises:
            AssertionError, JobClientError
        """
        assert isinstance(jobs, list), 'Jobs must be a list'
        assert len(jobs) > 0, 'One or more jobs required'

        req = list()
        if len(jobs) > 1:
            for r in self._batch_request(jobs):
                req.append(
                    ''.join([self._scheduler_endpoint, '?', '&'.join(r)]))
        else:
            req = "{}?job={}".format(
                self._scheduler_endpoint, jobs[0])

        try:
            self._api_delete(req)
        except HTTPError as e:
            raise JobClientError(e.message)

    def query(self, jobs):
        """Query one or more jobs

        Args:
            jobs (list): Jobs to query

        Returns:
            list: Jobs information

        Raises:
            AssertionError, JobClientError
        """
        assert isinstance(jobs, list), 'Jobs must be type list'
        assert len(jobs) > 0, 'One or more jobs required'

        req = list()
        if len(jobs) > 1:
            for r in self._batch_request(jobs):
                req.append(
                    ''.join([self._scheduler_endpoint, '?', '&'.join(r)]))
        else:
            req = "{}?job={}".format(
                self._scheduler_endpoint, jobs[0])

        try:
            ret = list()
            for resp in self._api_get(req):
                ret.extend(resp.json())
            return ret
        except HTTPError as e:
            raise JobClientError(e.message)

    def submit(self, jobs):
        """Submit one or more jobs

        Args:
            jobs (list): Jobs to submit

        Raises:
            AssertionError, JobClientError
        """
        assert isinstance(jobs, list), 'Jobs must be type list'
        assert len(jobs) > 0, 'One or more jobs required'

        data = {'jobs': jobs}
        for j in data['jobs']:
            # generate a random UUID if absent
            if 'uuid' not in j:
                j['uuid'] = str(uuid1())

            # default missing fields
            j.update(dict(self._default_job_settings.items() + j.items()))

        self._job_schema.validate(jobs)

        try:
            self._api_post(self._scheduler_endpoint, data)
            return [j['uuid'] for j in data['jobs']]
        except HTTPError as e:
            raise JobClientError(e.message)

    def retry(self, jobs, retries):
        """Retry a job

        Args:
            jobs (list): Job UUIDs
            retries (int): Number of retries

        Raises:
            JobClientError
        """
        assert isinstance(jobs, list), 'Jobs must be type list'
        assert len(jobs) > 0, 'One or more jobs required'
        assert retries >= 0, 'Retries must be greater than 0'

        try:
            for job in jobs:
                self._api_post("{}?job={}&retries={}".format(self._retry_endpoint, job, retries), {})
        except HTTPError as e:
            raise JobClientError(e.message)

    def list(self, user=getpass.getuser(), state=['success', 'running', 'failed', 'completed', 'waiting'],
             start_time=None, stop_time=None,
             limit=None):
        """List jobs run by a given user over a specific time range.

        Args:
            user (str): Username of user who ran the jobs
            state (str or list): One or more states to query for. Valid states are 'success', 'running', 'failed',
                                 'completed', 'waiting'.
            start_time (datetime or None): Considers all jobs submitted after this time
            stop_time (datetime or None): Considers all jobs submitted before this time
            limit (int or None): Limit the number of jobs returned

        Raises:
            AssertionError, JobClientError
        """
        r = list(["user={}".format(user)])
        if state:
            r.append("state={}".format('%2B'.join(state) if isinstance(state, list) else state))

        if start_time:
            assert isinstance(start_time, datetime), "start time must be a datetime object"
            r.append("start_ms={}".format((start_time - datetime.utcfromtimestamp(0)).total_seconds() * 1000.0))

        if stop_time:
            assert isinstance(stop_time, datetime), "stop time must be a datetime object"
            r.append("stop_ms={}".format((stop_time - datetime.utcfromtimestamp(0)).total_seconds() * 1000.0))

        if limit:
            r.append("limit={}".format(limit))

        try:
            resp = self._api_get(''.join([self._list_endpoint, '?', '&'.join(r)]))[0]
            return resp.json()
        except HTTPError as e:
            raise JobClientError(e.message)

    def wait(self, jobs):
        """Wait for jobs to complete

        Args:
            jobs (list): List of jobs to wait for

        Yields:
            dict: The job information
        """
        while True:
            try:
                for job in self.query(jobs=jobs):
                    if job['status'] == 'completed':
                        jobs.remove(job['uuid'])
                        yield (job)
            except JobClientError as e:
                logger.error(e.message)

            if len(jobs) > 0:
                time.sleep(self._status_update_interval_secs)
            else:
                break
