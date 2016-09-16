# Python library for Cook Scheduler REST API
This is a simple Python library that allows to interact with the Cook Scheduler REST API.

Pull requests to add additional API features are very welcome. I only implemented what I needed.

## Install
To install it simply issue the following command:

```
pip install cook-jobclient
```

## Usage
```
from cook import JobClient
jobclient = JobClient(url = "http://cook", auth = "http_basic", http_user = "foo", http_password = "bar")
```

Valid authentication methods are: http_basic, kerberos

Launch job(s)
```
resp = jobclient.submit(jobs = [{'uuid': "da75efdc-7c01-11e6-beaa-000c295e64ae", 'max_retries': 1, 'max_runtime': 86400000, 'mem': 1000, 'cpus': 1.5, 'command': 'id'}])
if resp['status'] == JobClient.Status.OK:
  print("Jobs successfully submitted.")
  print("Jobs UUIDs: {0}".format(resp['data']))
else:
  print("Jobs submission failed (reason: {0}".format(resp['reason']))
```

If UUID is absent, one will be automatically generated in the submit method and returned as part of the 'data' attribute.

Query job(s)
```
resp = jobclient.query(jobs = ["da75efdc-7c01-11e6-beaa-000c295e64ae"])
for r in resp:
  if r['status'] == JobClient.Status.OK:
    print(r['data'])
  else:
    print("Jobs query failed (reason: {0})".format(r['reason']))
```

Delete job(s)
```
resp = jobclient.delete(jobs = ["da75efdc-7c01-11e6-beaa-000c295e64ae"])
for r in resp:
  if r['status'] == JobClient.Status.OK:
    print("Jobs marked for deletion")
  else:
    print("Jobs deletion failed (reason: {0})".format(r['reason']))
```

Wait for job(s) to complete
```
for job in jobclient.wait(jobs = ["da75efdc-7c01-11e6-beaa-000c295e64ae", "da75efdc-7c01-11e7-beaa-000c295e64ae"]):
  print("Job {0} completed, status {1}".format(job['status'], job['instances'][0]['status']))
```

## Contact
Matteo Cerutti - matteo.cerutti@hotmail.co.uk
