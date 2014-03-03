-- Disable all DDL triggers
DISABLE TRIGGER ALL ON DATABASE;

-- Disable all DML triggers
DECLARE @table_name sysname;
DECLARE @disable_sql nvarchar(max);

DECLARE triggers_cursor CURSOR STATIC FOR
SELECT DISTINCT '[' + OBJECT_SCHEMA_NAME (t.parent_id) + '].[' + OBJECT_NAME(t.parent_id) + ']'
FROM sys.triggers as t
WHERE t.is_disabled = 0 and t.is_ms_shipped = 0
	AND SCHEMA_NAME() = OBJECT_SCHEMA_NAME(t.parent_id);

OPEN triggers_cursor;

FETCH NEXT FROM triggers_cursor 
INTO @table_name;

WHILE @@FETCH_STATUS = 0
BEGIN
	BEGIN TRY
		SET @disable_sql = N'ALTER TABLE ' + @table_name + ' DISABLE TRIGGER ALL';
		EXEC sp_sqlexec @disable_sql;
	END TRY
	BEGIN CATCH
		PRINT('Error disabling triggers on table ' + @table_name);
		PRINT(ERROR_MESSAGE());
	END CATCH
	
	FETCH NEXT FROM triggers_cursor 
	INTO @table_name;
END

CLOSE triggers_cursor;
DEALLOCATE triggers_cursor;