from setuptools import setup


setup(
    name="aiosolr",
    version="0.1.1",
    description="Asyncio lightweight python wrapper for Apache Solr.",
    author='Igor Tokarev',
    author_email='TigorC@gmail.com',
    long_description='',
    py_modules=[
        'aiosolr'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
        'Programming Language :: Python :: 3',
    ],
    url='http://github.com/tigorc/aiosolr/',
    license='BSD',
    install_requires=[
        'aiohttp>=0.21.2'
    ]
)
