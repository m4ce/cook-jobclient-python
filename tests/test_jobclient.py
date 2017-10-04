import os
import unittest
import json
from mock import patch, Mock
from requests import HTTPError
from schema import SchemaError
from uuid import UUID
from cook.jobclient import JobClient, JobClientError
from cook.utils import generate_batch_request
from requests_kerberos import HTTPKerberosAuth


class JobClientTests(unittest.TestCase):
    def setUp(self):
        self.client = JobClient(url='http://localhost:12310', http_user='foo', http_password='secret', default_job_settings={'max_retries': 10})

        with open("{}/test_jobs.json".format(os.path.dirname(__file__)), 'r') as f:
            self._jobs = json.loads(f.read())

    def test_auth(self):
        with self.assertRaises(AssertionError):
            JobClient(url='http://localhost:12310')

        with self.assertRaises(AssertionError):
            JobClient(url='http://localhost:12310', http_user='foo')

        client = JobClient(url='http://localhost:12310', auth='http_basic', http_user='foo', http_password='secret')
        self.assertEquals(client.get_auth(), ('foo', 'secret'))

        client = JobClient(url='http://localhost:12310', auth='kerberos')
        self.assertIs(type(client.get_auth()), HTTPKerberosAuth)

    def test_url(self):
        self.assertEquals(self.client.get_url(), 'http://localhost:12310')

    def test_default_job_settings(self):
        self.assertDictEqual(self.client.get_default_job_settings(), {'max_retries': 10})

    def test_batch_request(self):
        expected = [
            ['job=15dd97d6-a628-11e7-b27b-3cfdfea21a98', 'job=15dd95b0-a628-11e7-b27b-3cfdfea21a98',
             'job=15dd9380-a628-11e7-b27b-3cfdfea21a98', 'job=15dd915a-a628-11e7-b27b-3cfdfea21a98'],
            ['job=15dd8f2a-a628-11e7-b27b-3cfdfea21a98', 'job=15dd8cf0-a628-11e7-b27b-3cfdfea21a98',
             'job=15dd8ab6-a628-11e7-b27b-3cfdfea21a98', 'job=15dd8732-a628-11e7-b27b-3cfdfea21a98'],
            ['job=15dd83a4-a628-11e7-b27b-3cfdfea21a98', 'job=15dd7ddc-a628-11e7-b27b-3cfdfea21a98',
             'job=8a396b9c-a55f-11e7-b57c-3cfdfea21a98', 'job=8a39691c-a55f-11e7-b57c-3cfdfea21a98'],
            ['job=8a39669c-a55f-11e7-b57c-3cfdfea21a98', 'job=8a396412-a55f-11e7-b57c-3cfdfea21a98',
             'job=8a396192-a55f-11e7-b57c-3cfdfea21a98', 'job=8a395efe-a55f-11e7-b57c-3cfdfea21a98'],
            ['job=8a395c60-a55f-11e7-b57c-3cfdfea21a98', 'job=8a3959a4-a55f-11e7-b57c-3cfdfea21a98',
             'job=8a395602-a55f-11e7-b57c-3cfdfea21a98', 'job=8a395076-a55f-11e7-b57c-3cfdfea21a98'],
            ['job=55c4f298-a4a9-11e7-bedf-3cfdfea21a98', 'job=55c4efe6-a4a9-11e7-bedf-3cfdfea21a98',
             'job=55c4ed3e-a4a9-11e7-bedf-3cfdfea21a98', 'job=55c4ea8c-a4a9-11e7-bedf-3cfdfea21a98'],
            ['job=55c4e7e4-a4a9-11e7-bedf-3cfdfea21a98', 'job=55c4e528-a4a9-11e7-bedf-3cfdfea21a98',
             'job=55c4e26c-a4a9-11e7-bedf-3cfdfea21a98', 'job=55c4df7e-a4a9-11e7-bedf-3cfdfea21a98'],
            ['job=55c4dc0e-a4a9-11e7-bedf-3cfdfea21a98', 'job=55c4d510-a4a9-11e7-bedf-3cfdfea21a98']
        ]

        self.assertEquals(generate_batch_request([job['uuid'] for job in self._jobs], 4), expected)

    @staticmethod
    def _mock_response(status_code=200, json_data=None):
        mock_resp = Mock()
        mock_resp.status_code = status_code

        if json_data:
            mock_resp.json = Mock(
                return_value=json_data
            )

        if status_code >= 300:
            mock_resp.raise_for_status = Mock()
            mock_resp.raise_for_status.side_effect = HTTPError()

        return mock_resp

    @patch('requests.Session.get')
    def test_query(self, mock_get):
        mock_resp = self._mock_response(json_data=self._jobs)
        mock_get.return_value = mock_resp

        jobs = self.client.query([job['uuid'] for job in self._jobs])
        self.assertSequenceEqual(jobs, self._jobs)

        job = [j for j in self._jobs if j['uuid'] == '15dd9380-a628-11e7-b27b-3cfdfea21a98']
        mock_resp = self._mock_response(json_data=job)
        mock_get.return_value = mock_resp

        # should work with a list parameter
        jobs = self.client.query(["15dd9380-a628-11e7-b27b-3cfdfea21a98"])
        self.assertSequenceEqual(jobs, job)

        # test if we can pass a string
        with self.assertRaises(AssertionError):
            self.client.query("15dd9380-a629-11e7-b27b-3cfdfea21a98G")

        # test if we can pass an empty list
        with self.assertRaises(AssertionError):
            self.client.query([])

        # test a number of failed requests
        for code in [400, 401, 403, 404]:
            mock_resp = self._mock_response(status_code=code)
            mock_get.return_value = mock_resp

            with self.assertRaises(JobClientError):
                self.client.query(['2413bf75-1587-4a69-82e2-63cc4b0d656d'])

    @patch('requests.post')
    def test_submit(self, mock_post):
        expected = ['15dd97d6-a628-11e7-b27b-3cfdfea21a98']
        mock_resp = self._mock_response(status_code=201)
        mock_post.return_value = mock_resp
        self.assertSequenceEqual(self.client.submit([{
            'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
            'name': 'cookjob_20170925_1',
            'ports': 0,
            'gpus': 0,
            'constraints': [],
            'env': {},
            'disable_mea_culpa_retries': False,
            'mem': 128,
            'command': 'echo "hello world"',
            'priority': 1,
            'max_retries': 1,
            'max_runtime': 3600,
            'cpus': 1
        }]), expected)

        # make sure uuid is a valid UUID
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'uuid': 'foobar',
                'name': 'cookjob_20170925_1',
                'command': 'echo "hello world"'
            }])

        # test that an UUID is generated when not specified
        jobs = self.client.submit([{
            'name': 'cookjob_20170925_1',
            'command': 'echo "hello world"'
        }])
        self.assertIs(type(UUID(jobs[0])), UUID)

        # cpu can be a float too
        self.assertSequenceEqual(self.client.submit([{
            'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
            'cpus': 1.0
        }]), expected)

        with self.assertRaises(SchemaError):
            self.client.submit([{
                'cpus': 'should break'
            }])

        # should break if 0
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'cpus': 0
            }])

        # mem can be a float too
        self.assertSequenceEqual(self.client.submit([{
            'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
            'mem': 1.0
        }]), expected)

        with self.assertRaises(SchemaError):
            self.client.submit([{
                'mem': 'should break'
            }])

        # should break if 0
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'mem': 0
            }])

        # should break if empty
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'name': ''
            }])

        # should break if non-string
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'name': 1234
            }])

        # should break if empty
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'uuid': ''
            }])

        # should break if non-string
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'uuid': 1234
            }])

        # test executor
        self.assertSequenceEqual(self.client.submit([{
            'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
            'executor': 'mesos'
        }]), expected)
        self.assertSequenceEqual(self.client.submit([{
            'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
            'executor': 'cook'
        }]), expected)

        # should break if executor is not mesos/cook
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'executor': 'foo'
            }])

        # test priority
        self.assertSequenceEqual(self.client.submit([{
            'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
            'priority': 100
        }]), expected)
        self.assertSequenceEqual(self.client.submit([{
            'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
            'priority': 0
        }]), expected)

        # should break if priority < 0
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'priority': -1
            }])

        # should break if priority > 100
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'priority': 101
            }])

        # test max_retries
        self.assertSequenceEqual(self.client.submit([{
            'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
            'max_retries': 10
        }]), expected)

        # should break if max_retries <= 0
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'max_retries': 0
            }])

        # test max_runtime and expected_runtime
        for opt in ['max_runtime', 'expected_runtime']:
            self.assertSequenceEqual(self.client.submit([{
                'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
                opt: 1
            }]), expected)

            # should break if <= 0
            with self.assertRaises(SchemaError):
                self.client.submit([{
                    opt: 0
                }])

        # test gpus
        self.assertSequenceEqual(self.client.submit([{
            'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
            'gpus': 0
        }]), expected)

        # should break if gpus < 0
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'gpus': -1
            }])

        # test ports
        self.assertSequenceEqual(self.client.submit([{
            'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
            'ports': 0
        }]), expected)

        # should break if ports < 0
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'ports': -1
            }])

        # test URIs
        self.assertSequenceEqual(self.client.submit([{
            'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
            'uris': [
                {
                    'value': 'http://localhost/executor.tar.gz',
                    'extract': True
                }
            ]
        }]), expected)

        # should break if URIs is not a list
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'uris': 'foo'
            }])

        # test env
        self.assertSequenceEqual(self.client.submit([{
            'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
            'env': {'foo': 'bar'}
        }]), expected)

        # should break if env is not a dictionary
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'env': 'foo'
            }])

        # test constraints
        self.assertSequenceEqual(self.client.submit([{
            'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
            'constraints': [['instance_type', 'EQUALS', 'beefybox']]
        }]), expected)

        # should break if constraints is not a list
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'constraints': 'foo'
            }])

        # test disable_mea_culpa_retries
        for value in [True, False]:
            self.assertSequenceEqual(self.client.submit([{
                'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
                'disable_mea_culpa_retries': value
            }]), expected)

        # should break if disable_mea_culpa_retries is not a bool
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'disable_mea_culpa_retries': 'foo'
            }])

        # test container
        self.assertSequenceEqual(self.client.submit([{
            'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
            'container': {
                'type': 'DOCKER',
                'docker': {
                    'image': 'centos:latest',
                    'network': 'HOST',
                    'force-pull-image': True
                },
                'volumes':
                    [
                        {
                            'container-path': '/data',
                            'host-path': '/data',
                            'mode': 'ro'
                        }
                    ]

            }
        }]), expected)

        # should break if container is not a dict
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'container': 'foo'
            }])

        # test command
        self.assertSequenceEqual(self.client.submit([{
            'uuid': '15dd97d6-a628-11e7-b27b-3cfdfea21a98',
            'command': 'echo hello world'
        }]), expected)

        # should break if command is not a string
        with self.assertRaises(SchemaError):
            self.client.submit([{
                'command': 123
            }])

        # test a number of failed requests
        for code in [400, 401, 409, 500]:
            mock_resp = self._mock_response(status_code=code)
            mock_post.return_value = mock_resp

            with self.assertRaises(JobClientError):
                self.client.submit([
                    {
                        'command': 'echo hello world'
                    }
                ])

    @patch('requests.delete')
    def test_delete(self, mock_delete):
        mock_resp = self._mock_response(status_code=204)
        mock_delete.return_value = mock_resp
        self.assertIsNone(self.client.delete([job['uuid'] for job in self._jobs]))

        # should work with a list parameter
        self.assertIsNone(self.client.delete(['15dd9380-a628-11e7-b27b-3cfdfea21a98']))

        # should fail with a string
        with self.assertRaises(AssertionError):
            self.client.delete("15dd9380-a629-11e7-b27b-3cfdfea21a98G")

        # should fail if we pass an empty string
        with self.assertRaises(AssertionError):
            self.client.delete([])

        # test failures
        for code in [400, 403]:
            mock_resp = self._mock_response(status_code=code)
            mock_delete.return_value = mock_resp

            with self.assertRaises(JobClientError):
                self.client.delete(["15dd9380-a629-11e7-b27b-3cfdfea21a98G"])

    @patch('requests.post')
    def test_delete(self, mock_post):
        mock_resp = self._mock_response(status_code=204)
        mock_post.return_value = mock_resp
        self.assertIsNone(self.client.retry([job['uuid'] for job in self._jobs], retries=10))

        # should work with a list parameter
        self.assertIsNone(self.client.retry(['15dd9380-a628-11e7-b27b-3cfdfea21a98'], retries=10))

        # should fail with a string
        with self.assertRaises(AssertionError):
            self.client.retry("15dd9380-a629-11e7-b27b-3cfdfea21a98G", retries=10)

        # should fail if we pass an empty string
        with self.assertRaises(AssertionError):
            self.client.retry([], retries=10)

        # should fail if jobs is not passed in
        with self.assertRaises(TypeError):
            self.client.retry(retries=10)

        # should fail if retries is not passed in
        with self.assertRaises(TypeError):
            self.client.retry(['15dd9380-a628-11e7-b27b-3cfdfea21a98'])

        # should fail if retries is less than 0
        with self.assertRaises(AssertionError):
            self.client.retry(['15dd9380-a628-11e7-b27b-3cfdfea21a98'], retries=-1)

        # test failures
        for code in [400, 403]:
            mock_resp = self._mock_response(status_code=code)
            mock_post.return_value = mock_resp

            with self.assertRaises(JobClientError):
                self.client.retry(["15dd9380-a629-11e7-b27b-3cfdfea21a98G"], retries=10)

    @patch('requests.Session.get')
    def test_list(self, mock_get):
        mock_resp = self._mock_response(status_code=200, json_data=self._jobs)
        mock_get.return_value = mock_resp

        self.assertSequenceEqual(self.client.list(), self._jobs)

        # test failures
        for code in [400, 403]:
            mock_resp = self._mock_response(status_code=code)
            mock_get.return_value = mock_resp

            with self.assertRaises(JobClientError):
                self.client.list()

    @patch('requests.Session.get')
    def test_wait(self, mock_get):
        mock_resp = self._mock_response(status_code=200, json_data=self._jobs)
        mock_get.return_value = mock_resp

        self.assertSequenceEqual(list(self.client.wait([job['uuid'] for job in self._jobs])), self._jobs)


if __name__ == "__main__":
    unittest.main()
