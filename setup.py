from setuptools import setup, find_packages
import os

version = '0.0'
with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as fp:
    long_description = fp.read()

setup(name='MongoMultiMaster',
      version=version,
      description="Multimaster replication for MongoDB",
      long_description=long_description,
      classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Utilities' ],
      keywords='mongodb,replication',
      author='Rick Copeland',
      author_email='rick@arborian.com',
      url='',
      license='Apache License, Version 2.0',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
        'gevent',
        'pymongo',
        'pyyaml',
      ],
      scripts=['scripts/mmm'],
      entry_points="")
