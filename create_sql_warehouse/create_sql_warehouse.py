import os
from settings import get_settings
from dbutil import (
    get_transactional_tables_selection,
    get_staging_ddl,
    get_temporal_ddl,
    get_dimension_ddl,
)
import subprocess
import glob


def saveOutputFile(schema, otype, name, sql):
    with open(
        os.path.join(settings["outputdir"], schema, otype, name + ".sql"),
        "w",
        encoding="utf-8",
    ) as fp:
        fp.write(sql.replace("\r", ""))


def ensure_dir_exists(path):
    try:
        os.mkdir(path)
    except FileExistsError:
        pass


print("MakeDw starting")
# get / change basic settings & scopes
print("...Loading settings")
settings = get_settings()

ensure_dir_exists(os.path.join(settings["outputdir"], settings["dimension_schema"]))
ensure_dir_exists(
    os.path.join(settings["outputdir"], settings["dimension_schema"], "Tables")
)
ensure_dir_exists(
    os.path.join(
        settings["outputdir"], settings["dimension_schema"], "Stored Procedures"
    )
)

ensure_dir_exists(os.path.join(settings["outputdir"], settings["staging_schema"]))
ensure_dir_exists(
    os.path.join(settings["outputdir"], settings["staging_schema"], "Tables")
)
ensure_dir_exists(
    os.path.join(settings["outputdir"], settings["staging_schema"], "Stored Procedures")
)

ensure_dir_exists(os.path.join(settings["outputdir"], settings["temporal_schema"]))
ensure_dir_exists(
    os.path.join(settings["outputdir"], settings["temporal_schema"], "Tables")
)
ensure_dir_exists(
    os.path.join(
        settings["outputdir"], settings["temporal_schema"], "Stored Procedures"
    )
)

# select tables from source
tablesinscope = get_transactional_tables_selection(settings)
print("...Processing Tables")
for table in tablesinscope:
    print(f"......Starting on {table}")
    # todo for wh for each table:
    # * given input table ->
    # * create staging table
    stagingtablesql, stagingloadprocsql = get_staging_ddl(table, settings)
    saveOutputFile(settings["staging_schema"], "Tables", table, stagingtablesql)
    # * create load to staging
    saveOutputFile(
        settings["staging_schema"],
        "Stored Procedures",
        f"Populate{table}",
        stagingloadprocsql,
    )

    # * create temporal table
    temporaltablesql, temporalloadprocsql = get_temporal_ddl(table, settings)
    saveOutputFile(settings["temporal_schema"], "Tables", table, temporaltablesql)
    # * create load to temporal table
    saveOutputFile(
        settings["temporal_schema"],
        "Stored Procedures",
        f'Populate{table.replace(" ", "")}',
        temporalloadprocsql,
    )

    # * create scd1or2 dim table
    dimensionsql, dimensionloadsql = get_dimension_ddl(table, settings)
    saveOutputFile(settings["dimension_schema"], "Tables", table, dimensionsql)
    # * create load to dim
    saveOutputFile(
        settings["dimension_schema"],
        "Stored Procedures",
        f'Populate{table.replace(" ", "")}',
        dimensionloadsql,
    )

# format output nicely
print("...formatting output")
me = os.path.abspath(__file__)
binpath = os.path.realpath(os.path.join(me, os.pardir, os.pardir, "bin"))
sqlformatter = os.path.join(binpath, "SqlFormatter.exe")
process = subprocess.Popen(
    [sqlformatter, settings["outputdir"], "/ae", "/b-", "/r", "/sk"]
)
process.wait()
print("...tidying up")
# poorsql doesn't like some of the temporal table stuff, so remove these lines
# --WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!
for filepath in glob.iglob(f'{settings["outputdir"]}/**/*.sql', recursive=True):
    with open(filepath, "r") as file:
        s = file.read()
    with open(filepath, "w") as file:
        file.write(
            s.replace("--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!", "")
            .lstrip()
            .rstrip()
        )

print("All done!")
