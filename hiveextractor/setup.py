from os.path import join
from setuptools import setup

def requirements():
    try:
        with open(join('hiveextractor', 'requirements.txt'), 'r') as f:
            return f.read().splitlines()
    except Exception as e:
        print(e)
        quit(1)

setup(
    name='hiveextractor',
    version='1.0.0',
    license='MIT',
    zip_safe=False,
    author='schnekke',
    author_email='schnekke@ya.ru',
    packages=['gcputils', 'hiveextractor', 'pyhs2', 'pyhs2/cloudera', 'pyhs2/TCLIService'],
    python_requires='>=2.7,<3',
    url='https://github.com/BradRuderman/pyhs2',
    description='Python Hive Server 2 Client Driver & Exporter',
    install_requires=requirements(),
    entry_points={
        'console_scripts': [
            'extract_start=hiveextractor:extract',
            'export_start=hiveextractor:export'
        ]
    },
    test_suite='pyhs2.test',
    tests_require=['mock']
)