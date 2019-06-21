import PySimpleGUI as sg
from dbtemplates import *
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    MetaData,
    ForeignKey,
    inspect,
)
from sqlalchemy.dialects import mssql
from sqlalchemy.schema import CreateTable
import sys

this = sys.modules[__name__]
this.source_table = None
this.staging_table = None
this.temporal_table = None


def _get_engine(server, database):
    return create_engine(
        f"mssql+pyodbc://{server}/{database}?driver=SQL+Server+Native+Client+11.0"
    )


def _get_table_ddl(table, engine):
    ddl = str(CreateTable(table).compile(engine))
    ddl = (
        ddl.replace(" COLLATE SQL_Latin1_General_CP1_CI_AS", "")
        .replace("\n", " ")
        .replace("\t", " ")
    )
    return ddl


def get_transactional_tables_selection(settings):
    source_engine = _get_engine(settings["source_server"], settings["source_db"])

    # select source tables
    sql = select_source_tables_template.render(**settings)
    result = source_engine.execute(sql)
    tablelist = list([x[0] for x in result.fetchall()])
    layout = [
        [
            sg.Listbox(
                values=tablelist,
                select_mode=sg.LISTBOX_SELECT_MODE_MULTIPLE,
                size=(40, min(len(tablelist), 40)),
            )
        ],
        [sg.OK()],
    ]

    window = sg.Window("Select Input Tables", layout)
    event, values = window.Read()
    window.Close()
    return values[0]


def get_staging_columns(columns):
    # select source tables
    layout = [
        [
            sg.Listbox(
                values=columns,
                default_values=columns,
                select_mode=sg.LISTBOX_SELECT_MODE_MULTIPLE,
                size=(40, min(len(columns), 40)),
            )
        ],
        [sg.OK()],
    ]

    window = sg.Window("Select Columns to Include in Staging", layout)
    event, values = window.Read()
    window.Close()
    return values[0]


def get_staging_ddl(table, settings):
    source_engine = _get_engine(settings["source_server"], settings["source_db"])

    # ddl for staging table
    meta = MetaData()

    this.source_table = Table(
        table,
        meta,
        schema=settings["source_schema"],
        autoload=True,
        autoload_with=source_engine,
    )
    this.source_table.schema = settings["staging_schema"]

    keys = [x.name for x in this.source_table.columns if x.primary_key]
    if len(keys) == 0:
        keys = _get_primary_keys(this.source_table.columns)
    columns = get_staging_columns([c.name for c in this.source_table.columns])
    staging_columns = [
        Column(
            x.name, x.type, primary_key=(x.name in keys), nullable=_getnullable(x, keys)
        )
        for x in this.source_table.columns
        if x.name in columns
    ]
    this.staging_table = Table(
        f"{table}", meta, *staging_columns, schema=settings["staging_schema"]
    )

    staging_table_sql = table_creation_template.render(
        dropfirst=settings["dropfirst"],
        table=table,
        schema=settings["staging_schema"],
        create=_get_table_ddl(this.staging_table, source_engine),
    )

    staging_table = f'[{settings["staging_schema"]}].[{table}]'
    staging_columns = list(["[" + c.name + "]" for c in this.staging_table.columns])

    procname = "Populate" + table.replace(" ", "")

    proc_sql = staging_loadproc_template.render(
        dropfirst=settings["dropfirst"],
        procedurename=procname,
        staging_schema=settings["staging_schema"],
        source_schema=settings["source_schema"],
        source_db=settings["source_db"],
        source_table=table,
        staging_table=staging_table,
        staging_columns=staging_columns,
    )

    return staging_table_sql, proc_sql


def _get_primary_keys(columns):
    tablelist = list([x.name for x in columns])
    layout = [
        [
            sg.Listbox(
                values=tablelist,
                select_mode=sg.LISTBOX_SELECT_MODE_MULTIPLE,
                size=(40, min(len(tablelist), 40)),
            )
        ],
        [sg.OK()],
    ]
    window = sg.Window("No PK Found. Select Primary Keys", layout)
    event, values = window.Read()
    window.Close()
    return values[0]


def _getnullable(column, keys):
    if column in keys:
        return False
    return column.nullable


def get_temporal_ddl(table, settings):
    target_engine = _get_engine(settings["target_server"], settings["target_db"])
    target_meta = MetaData(bind=target_engine)

    keys = [x.name for x in this.staging_table.columns if x.primary_key]
    if len(keys) == 0:
        keys = _get_primary_keys(this.staging_table.columns)
    temporal_columns = [
        Column(
            x.name, x.type, primary_key=(x.name in keys), nullable=_getnullable(x, keys)
        )
        for x in this.staging_table.columns
    ]
    this.temporal_table = Table(
        f"{table}", target_meta, *temporal_columns, schema=settings["temporal_schema"]
    )
    temporal_table_ddl = _get_table_ddl(this.temporal_table, target_engine)
    temporal_table_ddl = (
        temporal_table_ddl[:-3]
        + ",[ValidFrom] [datetime2](0) GENERATED ALWAYS AS ROW START NOT NULL, [ValidTo] [datetime2](0) GENERATED ALWAYS AS ROW END NOT NULL,"
    )
    temporal_table_ddl += " PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]) "
    temporal_table_ddl += " ) "
    temporal_table_ddl += f" WITH (SYSTEM_VERSIONING = ON ( HISTORY_TABLE = [{settings['temporal_schema']}].[{table}History] ))  "

    temporal_table_sql = temporal_table_creation_template.render(
        dropfirst=settings["dropfirst"],
        table=table,
        schema=settings["temporal_schema"],
        create=temporal_table_ddl,
    )

    procedurename = f"Populate{table.replace(' ', '')}"
    merge_columns = list(
        [
            "[" + c.name + "]"
            for c in this.temporal_table.columns
            if c.name not in ["ValidFrom", "ValidTo"]
        ]
    )
    col_equality_list = [f"target.{c}=source.{c}" for c in merge_columns]
    pk_col_equality_list = [
        f"target.[{c.name}]=source.[{c.name}]"
        for c in this.temporal_table.primary_key.columns
    ]

    temporal_proc_sql = temporal_loadproc_template.render(
        dropfirst=settings["dropfirst"],
        procedurename=procedurename,
        temporal_schema=settings["temporal_schema"],
        source_table=table,
        merge_columns=merge_columns,
        staging_schema=settings["staging_schema"],
        pk_col_equality_list=pk_col_equality_list,
        col_equality_list=col_equality_list,
    )

    return temporal_table_sql, temporal_proc_sql


def _get_scd_type():
    layout = [[sg.Listbox(values=["Type 1", "Type 2"], size=(40, 15))], [sg.OK()]]
    window = sg.Window("Select Slowly Changing Dimention Type", layout)
    event, values = window.Read()
    window.Close()
    return values[0][0]


def _get_scd_columns():
    choosefrom = list(
        [
            c.name
            for c in this.temporal_table.columns
            if c.name not in ["ValidFrom", "ValidTo"]
        ]
    )
    layout = [
        [
            sg.Listbox(
                values=choosefrom,
                default_values=choosefrom,
                select_mode=sg.LISTBOX_SELECT_MODE_MULTIPLE,
                size=(40, min(len(choosefrom), 40)),
            )
        ],
        [sg.OK()],
    ]
    window = sg.Window("Select Columns to include in slowly changing dimension", layout)
    event, values = window.Read()
    window.Close()
    return list([v for v in values[0]])


def get_dimension_scd1_ddl(table, settings, columns):
    target_engine = _get_engine(settings["target_server"], settings["target_db"])
    target_meta = MetaData(bind=target_engine)
    dim_columns = [
        Column(
            settings["dimension_id_column_name"],
            Integer,
            primary_key=True,
            autoincrement=True,
        )
    ]
    dim_columns += list(
        [
            Column(x.name, x.type, primary_key=False, nullable=x.nullable)
            for x in this.temporal_table.columns
            if x.name in columns
        ]
    )
    dim_table = Table(
        table, target_meta, *dim_columns, schema=settings["dimension_schema"]
    )
    dim_table_ddl = _get_table_ddl(dim_table, target_engine)

    scd1_sql = "--Type 1 SCD\n" + table_creation_template.render(
        dropfirst=settings["dropfirst"],
        table=table,
        schema=settings["dimension_schema"],
        create=dim_table_ddl,
    )

    proc_name = f'BuildDim{table.replace(" ", "")}'

    sc1_proc_sql = scd1_load_template.render(
        dropfirst=settings["dropfirst"],
        procedurename=proc_name,
        dimension_schema=settings["dimension_schema"],
        dimension_table=table,
        selected_columns=columns,
        temporal_schema=settings["temporal_schema"],
        temporal_table=table,
    )
    return scd1_sql, sc1_proc_sql


def get_dimension_scd2_ddl(table, settings, columns):
    target_engine = _get_engine(settings["target_server"], settings["target_db"])
    target_meta = MetaData(bind=target_engine)
    dim_columns = [
        Column(
            settings["dimension_id_column_name"],
            Integer,
            primary_key=True,
            autoincrement=True,
        )
    ]
    dim_columns += list(
        [
            Column(x.name, x.type, primary_key=False, nullable=x.nullable)
            for x in this.temporal_table.columns
            if x.name in columns
        ]
    )
    dim_columns.append(Column("ValidFrom", mssql.DATETIME2(0), nullable=False))
    dim_columns.append(Column("ValidTo", mssql.DATETIME2(0), nullable=False))
    dim_table = Table(
        table, target_meta, *dim_columns, schema=settings["dimension_schema"]
    )
    dim_table_ddl = _get_table_ddl(dim_table, target_engine)

    scd2_sql = "--Type 2 SCD\n" + table_creation_template.render(
        dropfirst=settings["dropfirst"],
        table=table,
        schema=settings["dimension_schema"],
        create=dim_table_ddl,
    )

    proc_name = f'BuildDim{table.replace(" ", "")}'

    sc2_proc_sql = scd2_load_template.render(
        dropfirst=settings["dropfirst"],
        procedurename=proc_name,
        dimension_schema=settings["dimension_schema"],
        dimension_table=table,
        selected_columns=columns,
        temporal_schema=settings["temporal_schema"],
        temporal_table=table,
        dimension_id_column_name=settings['dimension_id_column_name']
    )
    return scd2_sql, sc2_proc_sql


def get_dimension_ddl(table, settings):
    scdtype = _get_scd_type()
    columns = _get_scd_columns()

    if scdtype == "Type 1":
        return get_dimension_scd1_ddl(table, settings, columns)
    return get_dimension_scd2_ddl(table, settings, columns)
