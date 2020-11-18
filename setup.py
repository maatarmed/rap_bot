from setuptools import setup

with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='rapy_genius',
    version='0.0.4',
    description='a python api to collect data from genius.com using their API',
    py_modules=["data_manager","genius_api"],
    package_dir={'':'src'},
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent"
    ],
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=[
        "beautifulsoup4==4.9.3",
        "bs4==0.0.1",
        "pymongo==3.11.0",
        "requests==2.24.0",
        "urllib3==1.25.11",
    ],
    extras_require={
        "dev": [
            "pytest>=3.7",
        ],
    },
    url='https://github.com/maatarmed/rapy_genius',
    author='Maatar M',
    author_email='maatar93@gmail.com',
)