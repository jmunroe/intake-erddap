#!/usr/bin/env python

from setuptools import setup
import versioneer

requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-erddap',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='ERDDAP plugin for Intake',
    url='https://github.com/jmunroe/intake-erddap',
    maintainer='James Munroe',
    maintainer_email='jmunroe@mun.ca',
    license='BSD',
    py_modules=['intake_erddap'],
    packages=['intake_erddap'],
    package_data={'': ['*.csv', '*.yml', '*.html']},
    entry_points={
        'intake.drivers': [
            'erddap = intake_erddap.intake_erddap:ERDDAPSource',
            'erddap_cat = intake_erddap.erddap_cat:ERDDAPCatalog',
            'erddap_auto = intake_erddap.intake_erddap:ERDDAPSourceAutoPartition',
            'erddap_manual = intake_erddap.intake_erddap:ERDDAPSourceManualPartition',
        ]
    },
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.rst').read(),
    zip_safe=False,
)
