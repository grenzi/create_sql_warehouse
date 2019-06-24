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
import logging

logger = logging.getLogger(__file__)


class DbUtil:
    source_table = Table()
    staging_table = Table()
    temporal_table = Table()
    settings = None

    def __init__(self, *args, **kwargs):
        self.settings = kwargs["settingsinstance"]

    def _get_engine(self, server, database):
        return create_engine(
            f"mssql+pyodbc://{server}/{database}?driver=SQL+Server+Native+Client+11.0"
        )

    def _get_table_ddl(self, table, engine):
        ddl = str(CreateTable(table).compile(engine))
        ddl = (
            ddl.replace(" COLLATE SQL_Latin1_General_CP1_CI_AS", "")
            .replace("\n", " ")
            .replace("\t", " ")
        )
        return ddl

    def get_source_table(self):
        sourcedb = self.settings.get("source_db")
        sourceserver = self.settings.get("source_server")
        sourceschema = self.settings.get("source_schema")

        source_engine = self._get_engine(sourceserver, sourcedb)
        # select source tables
        sql = select_source_tables_template.render(
            source_db=sourcedb, source_schema=sourceschema
        )
        result = source_engine.execute(sql)
        tablelist = list([x[0] for x in result.fetchall()])

        return self.settings.get("source_table", tablelist=tablelist)

    def get_staging_ddl(self, table):
        sourcedb = self.settings.get("source_db")
        sourceserver = self.settings.get("source_server")
        sourceschema = self.settings.get("source_schema")
        source_engine = self._get_engine(sourceserver, sourcedb)

        # ddl for staging table
        meta = MetaData()

        self.source_table = Table(
            table,
            meta,
            schema=self.settings.get("source_schema"),
            autoload=True,
            autoload_with=source_engine,
        )
        self.source_table.schema = self.settings.get("staging_schema")

        keys = [x.name for x in self.source_table.columns if x.primary_key]
        if len(keys) == 0:
            keys = self.settings.get(
                "source_primary_keys", columns=self.source_table.columns
            )

        columns = self.settings.get(
            "staging_columns", columns=list([c.name for c in self.source_table.columns])
        )
        staging_columns = [
            Column(
                x.name,
                x.type,
                primary_key=(x.name in keys),
                nullable=self._getnullable(x, keys),
            )
            for x in self.source_table.columns
            if x.name in columns
        ]
        self.staging_table = Table(
            f"{table}",
            meta,
            *staging_columns,
            schema=self.settings.get("staging_schema"),
        )

        staging_table_sql = table_creation_template.render(
            dropfirst=self.settings.get("dropfirst"),
            table=table,
            schema=self.settings.get("staging_schema"),
            create=self._get_table_ddl(self.staging_table, source_engine),
        )

        staging_table = f'[{self.settings.get("staging_schema")}].[{table}]'
        staging_columns = list(["[" + c.name + "]" for c in self.staging_table.columns])

        procname = "Populate" + table.replace(" ", "")

        proc_sql = staging_loadproc_template.render(
            dropfirst=self.settings.get("dropfirst"),
            procedurename=procname,
            staging_schema=self.settings.get("staging_schema"),
            source_schema=self.settings.get("source_schema"),
            source_db=self.settings.get("source_db"),
            source_table=table,
            staging_table=staging_table,
            staging_columns=staging_columns,
        )

        return staging_table_sql, proc_sql

    def _getnullable(self, column, keys):
        if column in keys:
            return False
        return column.nullable

    def get_temporal_ddl(self, table):
        target_engine = self._get_engine(
            self.settings.get("target_server"), self.settings.get("target_db")
        )
        target_meta = MetaData(bind=target_engine)

        keys = [x.name for x in self.staging_table.columns if x.primary_key]
        if len(keys) == 0:
            logger.error(
                "No keys defined in staging table. this should not ever happen"
            )
            exit(-1)

        temporal_columns = [
            Column(
                x.name,
                x.type,
                primary_key=(x.name in keys),
                nullable=self._getnullable(x, keys),
            )
            for x in self.staging_table.columns
        ]
        self.temporal_table = Table(
            f"{table}",
            target_meta,
            *temporal_columns,
            schema=self.settings.get("temporal_schema"),
        )
        temporal_table_ddl = self._get_table_ddl(self.temporal_table, target_engine)
        temporal_table_ddl = (
            temporal_table_ddl[:-3]
            + ",[ValidFrom] [datetime2](0) GENERATED ALWAYS AS ROW START NOT NULL, [ValidTo] [datetime2](0) GENERATED ALWAYS AS ROW END NOT NULL,"
        )
        temporal_table_ddl += " PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]) "
        temporal_table_ddl += " ) "
        temporal_table_ddl += f" WITH (SYSTEM_VERSIONING = ON ( HISTORY_TABLE = [{self.settings.get('temporal_schema')}].[{table}History] ))  "

        temporal_table_sql = temporal_table_creation_template.render(
            dropfirst=self.settings.get("dropfirst"),
            table=table,
            staging_schema=self.settings.get("staging_schema"),
            schema=self.settings.get("temporal_schema"),
            create=temporal_table_ddl,
        )

        procedurename = f"Populate{table.replace(' ', '')}"
        merge_columns = list(
            [
                "[" + c.name + "]"
                for c in self.temporal_table.columns
                if c.name not in ["ValidFrom", "ValidTo"]
            ]
        )
        col_equality_list = [f"target.{c}=source.{c}" for c in merge_columns]
        pk_col_equality_list = [
            f"target.[{c.name}]=source.[{c.name}]"
            for c in self.temporal_table.primary_key.columns
        ]

        temporal_proc_sql = temporal_loadproc_template.render(
            dropfirst=self.settings.get("dropfirst"),
            procedurename=procedurename,
            temporal_schema=self.settings.get("temporal_schema"),
            source_table=table,
            merge_columns=merge_columns,
            staging_schema=self.settings.get("staging_schema"),
            backdate_hist_to=self.settings.get("backdate_hist_to"),
            pk_col_equality_list=pk_col_equality_list,
            col_equality_list=col_equality_list,
        )

        return temporal_table_sql, temporal_proc_sql

    def get_dimension_scd1_ddl(self, table, columns):
        target_engine = self._get_engine(
            self.settings.get("target_server"), self.settings.get("target_db")
        )
        target_meta = MetaData(bind=target_engine)
        dim_columns = [
            Column(
                self.settings.get("dimension_id_column_name"),
                Integer,
                primary_key=True,
                autoincrement=True,
            )
        ]
        dim_columns += list(
            [
                Column(x.name, x.type, primary_key=False, nullable=x.nullable)
                for x in self.temporal_table.columns
                if x.name in columns
            ]
        )
        dim_table = Table(
            table,
            target_meta,
            *dim_columns,
            schema=self.settings.get("dimension_schema"),
        )
        dim_table_ddl = self._get_table_ddl(dim_table, target_engine)

        scd1_sql = "--Type 1 SCD\n" + table_creation_template.render(
            dropfirst=self.settings.get("dropfirst"),
            table=table,
            schema=self.settings.get("dimension_schema"),
            create=dim_table_ddl,
        )

        proc_name = f'BuildDim{table.replace(" ", "")}'

        sc1_proc_sql = scd1_load_template.render(
            dropfirst=self.settings.get("dropfirst"),
            procedurename=proc_name,
            dimension_schema=self.settings.get("dimension_schema"),
            dimension_table=table,
            selected_columns=columns,
            temporal_schema=self.settings.get("temporal_schema"),
            temporal_table=table,
        )
        return scd1_sql, sc1_proc_sql

    def get_dimension_scd2_ddl(self, table, columns):
        target_engine = self._get_engine(
            self.settings.get("target_server"), self.settings.get("target_db")
        )
        target_meta = MetaData(bind=target_engine)
        dim_columns = [
            Column(
                self.settings.get("dimension_id_column_name"),
                Integer,
                primary_key=True,
                autoincrement=True,
            )
        ]
        dim_columns += list(
            [
                Column(x.name, x.type, primary_key=False, nullable=x.nullable)
                for x in self.temporal_table.columns
                if x.name in columns
            ]
        )
        dim_columns.append(Column("ValidFrom", mssql.DATETIME2(0), nullable=False))
        dim_columns.append(Column("ValidTo", mssql.DATETIME2(0), nullable=False))
        dim_table = Table(
            table,
            target_meta,
            *dim_columns,
            schema=self.settings.get("dimension_schema"),
        )
        dim_table_ddl = self._get_table_ddl(dim_table, target_engine)

        scd2_sql = "--Type 2 SCD\n" + table_creation_template.render(
            dropfirst=self.settings.get("dropfirst"),
            table=table,
            schema=self.settings.get("dimension_schema"),
            create=dim_table_ddl,
        )

        proc_name = f'BuildDim{table.replace(" ", "")}'

        sc2_proc_sql = scd2_load_template.render(
            dropfirst=self.settings.get("dropfirst"),
            procedurename=proc_name,
            dimension_schema=self.settings.get("dimension_schema"),
            dimension_table=table,
            selected_columns=columns,
            temporal_schema=self.settings.get("temporal_schema"),
            temporal_table=table,
            dimension_id_column_name=self.settings.get("dimension_id_column_name"),
        )
        return scd2_sql, sc2_proc_sql

    def get_dimension_ddl(self, table):
        scdtype = self.settings.get("scd_type")
        columns = self.settings.get(
            "scd_columns",
            columns=list(
                [
                    c.name
                    for c in self.temporal_table.columns
                    if c.name not in ["ValidFrom", "ValidTo"]
                ]
            ),
        )

        if scdtype == "Type 1":
            return self.get_dimension_scd1_ddl(table, columns)
        return self.get_dimension_scd2_ddl(table, columns)
