DECLARE	@validate_sql nvarchar(max);
DECLARE @table_name sysname;
DECLARE @constraint_name sysname;
DECLARE @constraint_id int;
DECLARE @rowcount int;

DECLARE constraints_cursor CURSOR STATIC FOR
SELECT table_name, constraint_name, validate_sql, constraint_id
FROM dbo.constraints_info
ORDER BY CASE
	WHEN constraint_type = 'PK' THEN 1
	WHEN constraint_type = 'UK' THEN 2
	WHEN constraint_type = 'F' THEN 3
	ELSE 4
END, constraint_id DESC;

OPEN constraints_cursor;

FETCH NEXT FROM constraints_cursor 
INTO @table_name, @constraint_name, @validate_sql, @constraint_id;

WHILE @@FETCH_STATUS = 0
BEGIN
	BEGIN TRY
		EXEC sp_sqlexec @validate_sql;
		SET @rowcount = @@ROWCOUNT;
		
		IF (@rowcount > 0)
		BEGIN
			PRINT(@table_name + ': ' + CAST(@rowcount as nvarchar(10)) + ' rows deleted (' + @constraint_name + ')');
		END
	END TRY
	BEGIN CATCH
		PRINT('Error executing statement: ' + @validate_sql);
		PRINT(ERROR_MESSAGE());
	END CATCH
	
	FETCH NEXT FROM constraints_cursor 
	INTO @table_name, @constraint_name, @validate_sql, @constraint_id;
END

CLOSE constraints_cursor;
DEALLOCATE constraints_cursor;