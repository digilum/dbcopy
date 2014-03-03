DECLARE 
	@constraint_name sysname,
	@constraint_type char(2),
	@index_type varchar(20),
	@table_name sysname,
	@key_columns sysname,
	@create_sql nvarchar(max),
	@drop_sql nvarchar(max),
	@validate_sql nvarchar(max);

DECLARE constraints_cursor CURSOR STATIC FOR
SELECT 
	kc.name AS constraint_name,
	'[' + SCHEMA_NAME(kc.schema_id) + '].[' + OBJECT_NAME(kc.parent_object_id) + ']' AS table_name,  
	kc.type,
	i.type_desc,
	STUFF((
		SELECT   
			', ' + col_name(kc.parent_object_id, ic.column_id)
		FROM sys.index_columns ic
		WHERE i.index_id = ic.index_id
			AND i.object_id = ic.object_id
			AND ic.is_included_column = 0
		ORDER BY ic.key_ordinal
	FOR XML PATH('')), 1, 2, '')
FROM sys.key_constraints AS kc
	INNER JOIN sys.indexes AS i
		ON kc.unique_index_id = i.index_id AND kc.parent_object_id = i.object_id
WHERE kc.is_ms_shipped = 0
	AND SCHEMA_NAME() = OBJECT_SCHEMA_NAME(kc.parent_object_id)
ORDER BY kc.parent_object_id;

OPEN constraints_cursor;

FETCH NEXT FROM constraints_cursor 
INTO @constraint_name, @table_name, @constraint_type, @index_type, @key_columns;

WHILE @@fetch_status = 0
BEGIN
	
	SET @drop_sql = N'ALTER TABLE ' + @table_name + ' DROP CONSTRAINT ' + @constraint_name + ';';
	EXEC sp_sqlexec @drop_sql;
	
	SET @create_sql = N'ALTER TABLE ' + @table_name + ' WITH CHECK ADD CONSTRAINT ' + @constraint_name + ' ' +
		CASE
			WHEN @constraint_type = 'UQ' THEN 'UNIQUE '
			WHEN @constraint_type = 'PK' THEN 'PRIMARY KEY '
			ELSE ''
		END + @index_type + ' (' + @key_columns + ');';
	
	SET @validate_sql = N'DELETE FROM d1 FROM (SELECT row_number() OVER(ORDER BY ' + @key_columns + 
		') AS row_num FROM ' + @table_name + ') AS d1 INNER JOIN (SELECT d.row_num FROM( ' +
		'SELECT row_number() OVER(PARTITION BY ' + @key_columns + ' ORDER BY  ' + 
		@key_columns + ') AS dup_num, row_number() OVER(ORDER BY ' + @key_columns + 
		' ) AS row_num FROM ' + @table_name + ' ) AS d WHERE d.dup_num <> 1 ' +
		') AS d2 ON d1.row_num = d2.row_num '
		
	INSERT INTO dbo.constraints_info(table_name, constraint_name, constraint_type, create_sql, drop_sql, validate_sql)
	SELECT @table_name, @constraint_name, @constraint_type, @create_sql, @drop_sql, @validate_sql;


	FETCH NEXT FROM constraints_cursor 
	INTO @constraint_name, @table_name, @constraint_type, @index_type, @key_columns;
END

CLOSE constraints_cursor;
DEALLOCATE constraints_cursor;
