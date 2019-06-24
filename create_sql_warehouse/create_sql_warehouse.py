import os
import click
from settings import Settings
from dbutil import DbUtil
import subprocess
import logging
import glob

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s,%(name)s,%(levelname)s,"%(message)s"',
    datefmt="%m-%d %H:%M",
    filename="create_sql_warehouse.log",
    filemode="w",
)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)
logger = logging.getLogger()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)
def saveOutputFile(outputdir, schema, otype, name, sql):
    with open(
        os.path.join(outputdir, schema, otype, name + ".sql"), "w", encoding="utf-8"
    ) as fp:
        fp.write(sql.replace("\r", ""))


def ensure_dir_exists(path):
    try:
        os.mkdir(path)
    except FileExistsError:
        pass


@click.command()
@click.option(
    "--config",
    type=click.Path(exists=True),
    default=None,
    help="JSON file to read parameters from",
)
def main(config):
    # get / change basic settings & scopes
    print("...Loading settings")
    settings = Settings(config_path=config)
    dbutil = DbUtil(settingsinstance=settings)
    outdir = settings.get("outputdir")
    print(outdir)

    logger.debug(f'setting up directories under {settings.get("outputdir")}')
    ensure_dir_exists(
        os.path.join(settings.get("outputdir"), settings.get("dimension_schema"))
    )
    ensure_dir_exists(
        os.path.join(
            settings.get("outputdir"), settings.get("dimension_schema"), "Tables"
        )
    )
    ensure_dir_exists(
        os.path.join(
            settings.get("outputdir"),
            settings.get("dimension_schema"),
            "Stored Procedures",
        )
    )

    ensure_dir_exists(
        os.path.join(settings.get("outputdir"), settings.get("staging_schema"))
    )
    ensure_dir_exists(
        os.path.join(
            settings.get("outputdir"), settings.get("staging_schema"), "Tables"
        )
    )
    ensure_dir_exists(
        os.path.join(
            settings.get("outputdir"),
            settings.get("staging_schema"),
            "Stored Procedures",
        )
    )

    ensure_dir_exists(
        os.path.join(settings.get("outputdir"), settings.get("temporal_schema"))
    )
    ensure_dir_exists(
        os.path.join(
            settings.get("outputdir"), settings.get("temporal_schema"), "Tables"
        )
    )
    ensure_dir_exists(
        os.path.join(
            settings.get("outputdir"),
            settings.get("temporal_schema"),
            "Stored Procedures",
        )
    )

    # select tables from source
    table = dbutil.get_source_table()
    logger.info(f"...Processing {table}")
    # todo for wh for table:
    # * given input table ->
    # * create staging table
    stagingtablesql, stagingloadprocsql = dbutil.get_staging_ddl(table)
    saveOutputFile(
        settings.get("outputdir"),
        settings.get("staging_schema"),
        "Tables",
        table,
        stagingtablesql,
    )
    # * create load to staging
    saveOutputFile(
        settings.get("outputdir"),
        settings.get("staging_schema"),
        "Stored Procedures",
        f"Populate{table}",
        stagingloadprocsql,
    )

    # * create temporal table
    temporaltablesql, temporalloadprocsql = dbutil.get_temporal_ddl(table)
    saveOutputFile(
        settings.get("outputdir"),
        settings.get("temporal_schema"),
        "Tables",
        table,
        temporaltablesql,
    )
    # * create load to temporal table
    saveOutputFile(
        settings.get("outputdir"),
        settings.get("temporal_schema"),
        "Stored Procedures",
        f'Populate{table.replace(" ", "")}',
        temporalloadprocsql,
    )

    # * create scd1or2 dim table
    dimensionsql, dimensionloadsql = dbutil.get_dimension_ddl(table)
    saveOutputFile(
        settings.get("outputdir"),
        settings.get("dimension_schema"),
        "Tables",
        table,
        dimensionsql,
    )
    # * create load to dim
    saveOutputFile(
        settings.get("outputdir"),
        settings.get("dimension_schema"),
        "Stored Procedures",
        f'Populate{table.replace(" ", "")}',
        dimensionloadsql,
    )

    # format output nicely
    logger.info("...formatting output")
    me = os.path.abspath(__file__)
    binpath = os.path.realpath(os.path.join(me, os.pardir, os.pardir, "bin"))
    sqlformatter = os.path.join(binpath, "SqlFormatter.exe")
    process = subprocess.Popen(
        [sqlformatter, settings.get("outputdir"), "/ae", "/b-", "/r", "/sk"]
    )
    process.wait()
    logger.info("...tidying up")
    # poorsql doesn't like some of the temporal table stuff, so remove these
    # lines
    # --WARNING!  ERRORS ENCOUNTERED DURING SQL PARSING!
    for filepath in glob.iglob(f'{settings.get("outputdir")}/**/*.sql', recursive=True):
        with open(filepath, "r") as file:
            s = file.read()
        with open(filepath, "w") as file:
            file.write(
                s.replace("--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!", "")
                .lstrip()
                .rstrip()
            )

    logger.info("All done!")


if __name__ == "__main__":
    main()
