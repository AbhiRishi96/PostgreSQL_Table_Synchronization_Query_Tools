from setuptools import setup, find_packages

setup(
    name='cxr_db',                            # Name of the package
    version='0.1.1',                          # Version of the package
    packages=find_packages(),                 # Automatically find all sub-packages
    # install_requires=[
    #     'psycopg2',                           # Add other dependencies here
    #     'pandas',
    #     'PyYAML'
    # ],      
    entry_points={
        'console_scripts': [
            'run_cxr_db = cxr_db.main:main',  # Create a command-line tool 'run_cxr_db'
        ],
    },
    include_package_data=True,                # Include non-Python files like config.yaml
    description='A package for syncing PostgreSQL tables, creating views, -and executing queries.',
    long_description=open('README.md').read(),  # Optional: load README for PyPI description
    long_description_content_type='text/markdown',
    author='Abhishek_Rai',
    author_email='abhishek.rai@qure.ai',
    url='https://github.com/yourrepo/my_sync_package',  # Optional: link to the project
)