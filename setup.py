from setuptools import setup, find_packages

setup(
    name='iwx-data-validation-tool',
    version='0.0.0',
    packages=find_packages(),
    install_requires=['click'],
    entry_points="""
        [console_scripts]
        iwx-datavalidation-tool=iwx_data_validator:welcome
    """
)