from setuptools import find_packages
from setuptools import setup

import os

loc = os.path.dirname(os.path.realpath(__file__))
requirementPath = loc + '/requirements.txt'
install_requires = []

if os.path.isfile(requirementPath):
    with open(requirementPath, encoding="utf8") as f:
        install_requires = f.read().splitlines()
    
setup(
          name="LizardTools", 
          version='1.0.1',
          description='this package contains useful tools for data exchange with the Lizard api',
          author='',
          packages=find_packages(),
          install_requires=install_requires
          
          )