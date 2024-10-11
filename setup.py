from setuptools import setup, find_packages

setup(
    name='cassandracrud',
    version='0.1.0',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    install_requires=[
        'cassandra-driver>=3.29.1',
        'pandas>=1.0.0',
    ],
    author="Baris Genc",
    author_email="info@gencbaris.com",
    description='A simple CRUD library for Apache Cassandra',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/cervantes79/cassandracrud",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)