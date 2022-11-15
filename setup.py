import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="infoworks-data-validator",
    version="1.0.0",
    author="Abhishek",
    author_email="abhishek.raviprasad@infoworks.io",
    description="Data Validation Scripts",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/abhr1994/InfoworksDataValidator",
    packages=setuptools.find_packages(),
    install_requires=[
        'requests', 'click', 'colorama', 'sqlparse'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points="""
        [console_scripts]
        iwx-datavalidation-tool=iwx_data_validator:main
    """
)
