DECLARE 
	@constraint_name sysname,
	@table_name sysname,
	@create_sql nvarchar(max),
	@drop_sql nvarchar(max),
	@validate_sql nvarchar(max),
	@where_clause nvarchar(max);

DECLARE constraints_cursor CURSOR STATIC FOR
SELECT 
	cc.name,
	'[' + schema_name(cc.schema_id) + '].[' + object_name(cc.parent_object_id) + ']',
	cc.definition
FROM sys.check_constraints as cc
WHERE cc.is_disabled = 0 AND cc.is_ms_shipped = 0
AND SCHEMA_NAME() = OBJECT_SCHEMA_NAME(cc.parent_object_id);

OPEN constraints_cursor;

FETCH NEXT FROM constraints_cursor 
INTO @constraint_name, @table_name, @where_clause;

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @drop_sql = N'ALTER TABLE ' + @table_name + ' NOCHECK CONSTRAINT ' + @constraint_name + ';';
	EXEC sp_sqlexec @create_sql;
	
	SET @create_sql = N'ALTER TABLE ' + @table_name + ' WITH CHECK CHECK CONSTRAINT ' + @constraint_name + ';';

	SET @validate_sql = N'DELETE FROM ' + @table_name + ' WHERE NOT ' + @where_clause + ';';
	
	INSERT INTO dbo.constraints_info(table_name, constraint_name, constraint_type, create_sql, drop_sql, validate_sql)
	SELECT @table_name, @constraint_name, 'C', @create_sql, @drop_sql, @validate_sql;	

	FETCH NEXT FROM constraints_cursor 
	INTO @constraint_name, @table_name, @where_clause;
END

CLOSE constraints_cursor;
DEALLOCATE constraints_cursor;