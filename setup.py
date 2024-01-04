from setuptools import setup, find_packages

base_requirements = {"openmetadata-ingestion~=1.2.0"}

setup(
    name="spacial-connector",
    version="0.0.1",
    url="https://open-metadata.org/",
    author="Daniel Chenari",
    license="MIT license",
    description="Ingestion Framework for OpenMetadata",
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    install_requires=list(base_requirements),
    packages=find_packages(include=["connector", "connector.*"]),
)
