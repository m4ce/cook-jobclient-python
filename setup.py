from distutils.core import setup

version = '0.0.1'

setup(
  name = 'cook-api',
  packages = ['cook'],
  version = version,
  description = 'Python library for Cook Mesos Framework API',
  author = 'Matteo Cerutti',
  author_email = 'matteo.cerutti@hotmail.co.uk',
  url = 'https://github.com/m4ce/cook-api-python',
  download_url = 'https://github.com/m4ce/cook-api-python/tarball/%s' % (version,),
  keywords = ['cook', 'mesos'],
  classifiers = [],
  install_requires = ["requests", "requests-kerberos"]
)
