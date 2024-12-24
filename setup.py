from setuptools import setup, find_packages

setup(
    name="atomicdb",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "cryptography>=41.0.0",
    ],
    python_requires=">=3.8",
    author="Robert Kinser",
    author_email="icakinser@gmail.com",
    description="A lightweight, thread-safe document database with SQL-like queries",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    keywords="database, document-store, json, thread-safe, atomic",
    url="",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
