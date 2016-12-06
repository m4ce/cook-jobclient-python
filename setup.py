from distutils.core import setup

version = '0.0.7'

setup(
  name = 'cook-jobclient',
  packages = ['cook'],
  version = version,
  description = 'Python library for Cook Scheduler REST API',
  author = 'Matteo Cerutti',
  author_email = 'matteo.cerutti@hotmail.co.uk',
  url = 'https://github.com/m4ce/cook-jobclient-python',
  download_url = 'https://github.com/m4ce/cook-jobclient-python/tarball/%s' % (version,),
  keywords = ['cook', 'mesos', 'jobclient'],
  classifiers = [],
  install_requires = ["requests", "requests-kerberos"]
)
