from jinja2 import Template

select_source_tables_template = Template(
    """
select 
    TABLE_NAME
from INFORMATION_SCHEMA.TABLES
where TABLE_CATALOG='{{source_db}}'
and TABLE_SCHEMA='{{source_schema}}'
order by TABLE_TYPE, TABLE_NAME
"""
)

drop_procedure_template = Template(
    """
{% if dropfirst %}
IF EXISTS (
    SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
    WHERE routine_type = 'PROCEDURE' 
    and SPECIFIC_NAME = '{{procedurename}}' 
    AND SPECIFIC_SCHEMA = '{{schema}}')
DROP PROCEDURE [{{schema}}].[{{procedurename}}];
GO
{% endif %}
"""
)

table_creation_template = Template(
    """
{% if dropfirst %}    
if exists (
    select * 
    from INFORMATION_SCHEMA.TABLES 
    where TABLE_NAME = '{{table}}' 
    AND TABLE_SCHEMA = '{{schema}}')  
drop table [{{schema}}].[{{table}}];
{% endif%}

{{create}};    
"""
)


temporal_table_creation_template = Template(
    """
{% if dropfirst %}    
BEGIN TRY
    ALTER TABLE [{{schema}}].[{{table}}]  SET ( SYSTEM_VERSIONING = OFF  )
END TRY
BEGIN CATCH
    --Do nothing
END CATCH
GO

BEGIN TRY
    drop table [{{schema}}].[{{table}}] 
END TRY
BEGIN CATCH
    --Do nothing
END CATCH
GO

BEGIN TRY
    drop table [{{schema}}].[{{table}}History]
END TRY
BEGIN CATCH
    --Do nothing
END CATCH
GO
{% endif %}    
{{create}};

"""
)

staging_loadproc_template = Template(
    """
{% if dropfirst %}
IF EXISTS (
        SELECT * 
        FROM INFORMATION_SCHEMA.ROUTINES 
        WHERE routine_type = 'PROCEDURE' 
        and SPECIFIC_NAME = '{{procedurename}}' 
        AND SPECIFIC_SCHEMA = '{{staging_schema}}')
    DROP PROCEDURE [{{staging_schema}}].[{{procedurename}}];
GO
{% endif %}
CREATE PROCEDURE [{{staging_schema}}].[{{procedurename}}] AS 
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE {{staging_table}};
    INSERT INTO {{staging_table}} (
    {% for col in staging_columns %}
    {{col}}{{ "," if not loop.last }}
    {% endfor %}
    )
    SELECT 
    {% for col in staging_columns %}{{col}}{{ "," if not loop.last }}{% endfor %}
    FROM [{{source_db}}].[{{source_schema}}].[{{source_table}}];
END
GO  
"""
)

temporal_loadproc_template = Template(
    """
{% if dropfirst %}
IF EXISTS (
        SELECT * 
        FROM INFORMATION_SCHEMA.ROUTINES 
        WHERE routine_type = 'PROCEDURE' 
        and SPECIFIC_NAME = '{{procedurename}}' 
        AND SPECIFIC_SCHEMA = '{{temporal_schema}}')
    DROP PROCEDURE [{{temporal_schema}}].[{{procedurename}}];
GO;
{% endif %}
CREATE PROCEDURE [{{temporal_schema}}].[{{procedurename}}] AS
BEGIN
    declare @old_ansi_null as sql_variant = sessionproperty('ANSI_NULLS')
    SET ANSI_NULLS OFF;
    SET NOCOUNT ON;
    MERGE [{{temporal_schema}}].[{{source_table}}] AS target
    USING (
        SELECT {% for col in merge_columns %}{{col}}{{ "," if not loop.last }}{% endfor %} FROM [{{staging_schema}}].[{{source_table}}]
        except
        SELECT {% for col in merge_columns %}{{col}}{{ "," if not loop.last }}{% endfor %} FROM [{{temporal_schema}}].[{{source_table}}]
    ) as source ({% for col in merge_columns %}{{col}}{{ "," if not loop.last }}{% endfor %})
    ON ({% for col in pk_col_equality_list %}{{col}}{{ " AND " if not loop.last }}{% endfor %})    
    WHEN MATCHED THEN UPDATE
        SET {% for col in col_equality_list %}{{col}}{{ "," if not loop.last }}{% endfor %}
    WHEN NOT MATCHED BY TARGET THEN INSERT
        ({% for col in merge_columns %}{{col}}{{ "," if not loop.last }}{% endfor %})
        VALUES ({% for col in merge_columns %}{{col}}{{ "," if not loop.last }}{% endfor %});

    MERGE [{{temporal_schema}}].[{{source_table}}] AS target
    USING (
        SELECT {% for col in merge_columns %}{{col}}{{ "," if not loop.last }}{% endfor %} FROM [{{staging_schema}}].[{{source_table}}]
    ) as source ( {% for col in merge_columns %}{{col}}{{ "," if not loop.last }}{% endfor %})
    ON ({% for col in pk_col_equality_list %}{{col}}{{ " AND " if not loop.last }}{% endfor %})
    WHEN NOT MATCHED BY SOURCE THEN DELETE;

    if @old_ansi_null = 1 
    SET ANSI_NULLS ON

    {% if backdate_hist_to %}   
    --this will only be run on the first load, since after that min(validfrom) will equal backfill date
    declare @oldest datetime2(7) = (SELECT MIN([ValidFrom]) FROM [{{temporal_schema}}].[{{source_table}}])
    IF @oldest <> '{{backdate_hist_to}}'
    BEGIN
        ALTER TABLE [{{temporal_schema}}].[{{source_table}}] SET (system_versioning = off);
        ALTER TABLE [{{temporal_schema}}].[{{source_table}}] DROP PERIOD FOR SYSTEM_TIME;
        UPDATE [{{temporal_schema}}].[{{source_table}}] SET ValidFrom={{backdate_hist_to}};
        ALTER TABLE [{{temporal_schema}}].[{{source_table}}] ADD PERIOD FOR SYSTEM_TIME (ValidFrom,ValidTo);
        ALTER TABLE [{{temporal_schema}}].[{{source_table}}] set (system_versioning = on (HISTORY_TABLE=[{{temporal_schema}}].[{{source_table}}History]));        
    END
    {% endif %}
END
GO;
"""
)

scd1_load_template = Template(
    """
{% if dropfirst %}    
IF EXISTS (
        SELECT * 
        FROM INFORMATION_SCHEMA.ROUTINES 
        WHERE routine_type = 'PROCEDURE' 
        and SPECIFIC_NAME = '{{procedurename}}' 
        AND SPECIFIC_SCHEMA = '{{dimension_schema}}')
    DROP PROCEDURE [{{dimension_schema}}].[{{procedurename}}];
GO;
{% endif %}
CREATE PROCEDURE [{{dimension_schema}}].[{{procedurename}}] AS
BEGIN
    SET NOCOUNT ON;
    declare @d as DATE = (select GETDATE())
    truncate table [{{dimension_schema}}].[{{dimension_table}}];        
    DBCC CHECKIDENT ('[{{dimension_schema}}].[{{dimension_table}}]', RESEED, 1);
    INSERT INTO [{{dimension_schema}}].[{{dimension_table}}]
    (
        {% for col in selected_columns %}[{{col}}]{{ "," if not loop.last }}{% endfor %}
    )
    SELECT {% for col in selected_columns %}[{{col}}]{{ "," if not loop.last }}{% endfor %}
    FROM [{{temporal_schema}}].[{{temporal_table}}]
    FOR SYSTEM_TIME AS OF @d
    SET NOCOUNT OFF;
END;
GO;
"""
)

scd2_load_template = Template(
    """
{% if dropfirst %}
IF EXISTS (
        SELECT * 
        FROM INFORMATION_SCHEMA.ROUTINES 
        WHERE routine_type = 'PROCEDURE' 
        and SPECIFIC_NAME = '{{procedurename}}' 
        AND SPECIFIC_SCHEMA = '{{dimension_schema}}')
    DROP PROCEDURE [{{dimension_schema}}].[{{procedurename}}];
GO;
{% endif %}
CREATE PROCEDURE [{{dimension_schema}}].[{{procedurename}}] AS
BEGIN
    SET NOCOUNT ON;
    truncate table [{{dimension_schema}}].[{{dimension_table}}];  

    WITH rawdim as ( SELECT {% for col in selected_columns %}[{{col}}]{{ "," if not loop.last }}{% endfor %},MIN([ValidFrom]) as ValidFrom ,MAX([ValidTo]) as ValidTo
    FROM [{{temporal_schema}}].[{{temporal_table}}] FOR SYSTEM_TIME ALL
    GROUP BY {% for col in selected_columns %}[{{col}}]{{ "," if not loop.last }}{% endfor %}
    ), thedim as (SELECT row_number() over (order by ValidTo) as dimId,
    {% for col in selected_columns %}[{{col}}]{{ "," if not loop.last }}{% endfor %},[ValidFrom],[ValidTo] FROM rawdim)
    INSERT INTO [{{dimension_schema}}].[{{dimension_table}}] (
    [{{dimension_id_column_name}}],{% for col in selected_columns %}[{{col}}]{{ "," if not loop.last }}{% endfor %},[ValidFrom],[ValidTo])
    select [{{dimension_id_column_name}}],{% for col in selected_columns %}[{{col}}]{{ "," if not loop.last }}{% endfor %},[ValidFrom],[ValidTo] FROM thedim;
    
    SET NOCOUNT OFF;
END
GO;
"""
)
