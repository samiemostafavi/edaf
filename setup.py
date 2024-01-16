#  Copyright (c) 2024 Samie Mostafavi
#
#  Licensed under the Apache License, Version 2.0 (the 'License');
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an 'AS IS' BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#   limitations under the License.

import setuptools

import edaf

with open('./README.md', 'r') as fp:
    long_description = fp.read()

with open('./requirements.txt', 'r') as fp:
    reqs = fp.readlines()

setuptools.setup(
    name='edaf',
    version=edaf.__version__,
    author='Samie Mostafavi',
    author_email='samiemostafavi@gmail.com',
    description='The EDAF (an End-to-end Delay Analytics Framework for 5G-and-beyond networks) '
                'framework to decompose packets` end-to-end delays and determine'
                'each component`s significance for optimizing the delay in 5G network.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/samiemostafavi/edaf',
    packages=setuptools.find_packages(),
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Topic :: System :: Wireless',
        'Topic :: System :: Networking',
        'Topic :: System :: Delay'
    ],
    install_requires=reqs,
    extras_require={},
    entry_points={
        'console_scripts': ['edaf=edaf.api.server:serve'],
    },
    python_requires='>=3.8',
    license='Apache v2'
)