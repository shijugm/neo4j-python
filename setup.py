
from setuptools import setup ,find_packages


REQUIRED_PACKAGES = [
    'neo4j',
    'sqlalchemy'
]

setup(    
        name='dataflow-neo4j',
        version='0.0.1',
        description='Dataflow example for neo4j',
        packages=find_packages(),
        install_requires=REQUIRED_PACKAGES,
        include_package_data=True,
        classifiers=[
            "Operating System :: OS Independent",
        ],

)


