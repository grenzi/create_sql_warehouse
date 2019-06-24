from setuptools import setup

setup(
    name="Create Sql Warehouse",
    version="0.1",
    py_modules=["hello"],
    install_requires=["Click"],
    entry_points="""
        [console_scripts]
        create_sql_warehouse=create_sql_warehouse:main
    """,
)
