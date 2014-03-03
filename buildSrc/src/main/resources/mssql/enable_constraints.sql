DECLARE	@create_sql nvarchar(max);
DECLARE @constraint_id int;

DECLARE constraints_cursor CURSOR STATIC FOR
SELECT create_sql, constraint_id
FROM dbo.constraints_info
ORDER BY CASE
	WHEN constraint_type = 'PK' THEN 1
	WHEN constraint_type = 'UK' THEN 2
	WHEN constraint_type = 'F' THEN 3
	ELSE 4
END, constraint_id DESC;

OPEN constraints_cursor;

FETCH NEXT FROM constraints_cursor 
INTO @create_sql, @constraint_id;

WHILE @@FETCH_STATUS = 0
BEGIN
	BEGIN TRY
		EXEC sp_sqlexec @create_sql;
		
		DELETE FROM dbo.constraints_info
		WHERE constraint_id = @constraint_id;
	END TRY
	BEGIN CATCH
		PRINT('Error executing statement: ' + @create_sql);
		PRINT(ERROR_MESSAGE());
	END CATCH
	
	FETCH NEXT FROM constraints_cursor 
	INTO @create_sql, @constraint_id;
END

CLOSE constraints_cursor;
DEALLOCATE constraints_cursor;