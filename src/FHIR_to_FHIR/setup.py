from setuptools import setup, find_packages

setup(
    name="pubsub_to_fhir_pipeline",
    version="0.1",
    description="Apache Beam pipeline to process Pub/Sub FHIR messages and write to GCP FHIR store",
    install_requires=[
        "apache-beam[gcp]",
        "google-auth",
        "requests",
    ],
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
)
