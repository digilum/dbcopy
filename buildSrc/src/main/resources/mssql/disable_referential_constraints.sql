DECLARE 
	@constraint_name sysname,
	@table_name sysname,
	@key_columns sysname,
	@referenced_columns sysname,
	@referenced_table_name sysname,
	@delete_referential_action varchar(20),
	@update_referential_action varchar(20),
	@create_sql nvarchar(max),
	@drop_sql nvarchar(max),
	@validate_sql nvarchar(max),
	@join_clause nvarchar(max),
	@where_clause nvarchar(max);

DECLARE @referential_actions TABLE(
	action_id tinyint,
	action_text nvarchar(20)
);

INSERT INTO @referential_actions
VALUES(0, N'NO ACTION'), (1, N'CASCADE'), (2, N'SET NULL'), (3, N'SET DEFAULT');


DECLARE constraints_cursor CURSOR STATIC FOR
SELECT
	f.name,
	'[' + OBJECT_SCHEMA_NAME(f.parent_object_id) + '].[' + OBJECT_NAME(f.parent_object_id) + ']',  
	'[' + OBJECT_SCHEMA_NAME(f.referenced_object_id) + '].[' + OBJECT_NAME(f.referenced_object_id) + ']',
	STUFF((
		SELECT ', ' + COL_NAME(f.parent_object_id, fc.parent_column_id)
		FROM sys.foreign_key_columns AS fc
		WHERE fc.constraint_object_id = f.object_id
		ORDER BY fc.constraint_column_id FOR XML PATH('')), 1, 2, ''),
	STUFF((
		SELECT ', ' + COL_NAME(f.referenced_object_id, fc.referenced_column_id)
		FROM sys.foreign_key_columns AS fc
		WHERE fc.constraint_object_id = f.object_id 
		ORDER BY fc.constraint_column_id FOR XML PATH('')), 1, 2, ''), 
	STUFF((
		SELECT ' AND p.' + COL_NAME(f.parent_object_id, fc.parent_column_id) + 
			' = r.' + COL_NAME(f.referenced_object_id, fc.referenced_column_id)  
		FROM sys.foreign_key_columns AS fc
		WHERE fc.constraint_object_id = f.object_id 
		ORDER BY fc.constraint_column_id FOR XML PATH('')), 1, 5,''),
	STUFF((SELECT ' AND p.' + COL_NAME(f.parent_object_id, fc.parent_column_id) + ' IS NOT NULL ' 
		FROM sys.foreign_key_columns AS fc
		WHERE fc.constraint_object_id = f.object_id 
		ORDER BY fc.constraint_column_id FOR XML PATH('')), 1, 0,''),
	d.action_text,
	u.action_text
FROM sys.foreign_keys AS f
INNER JOIN @referential_actions as d on f.delete_referential_action = d.action_id
INNER JOIN @referential_actions as u on f.update_referential_action = u.action_id
WHERE SCHEMA_NAME() IN (OBJECT_SCHEMA_NAME(f.parent_object_id), OBJECT_SCHEMA_NAME(f.referenced_object_id));


OPEN constraints_cursor;

FETCH NEXT FROM constraints_cursor 
INTO @constraint_name, @table_name, @referenced_table_name, @key_columns, @referenced_columns,
	@join_clause, @where_clause, @delete_referential_action, @update_referential_action;

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @drop_sql = N'ALTER TABLE ' + @table_name + ' DROP CONSTRAINT ' + @constraint_name + ';';
	EXEC sp_sqlexec @drop_sql;
	
	SET @create_sql = N'ALTER TABLE ' + @table_name + ' ADD CONSTRAINT ' + @constraint_name + 
		' FOREIGN KEY (' + @key_columns + ') REFERENCES ' + 
		@referenced_table_name + ' (' + @referenced_columns + ')' +
		' ON UPDATE ' + @update_referential_action + ' ON DELETE ' + @delete_referential_action + ';';
	
	SET @validate_sql = N'DELETE FROM p FROM ' + @table_name + ' AS p WHERE NOT EXISTS (' +
		' SELECT 1 FROM ' + @referenced_table_name + ' AS r WHERE ' + @join_clause + ')' + @where_clause + ';';
	
	INSERT INTO dbo.constraints_info(table_name, constraint_name, constraint_type, create_sql, drop_sql, validate_sql)
	SELECT @table_name, @constraint_name, 'F', @create_sql, @drop_sql, @validate_sql;

	FETCH NEXT FROM constraints_cursor 
	INTO @constraint_name, @table_name, @referenced_table_name, @key_columns, @referenced_columns,
		@join_clause, @where_clause, @delete_referential_action, @update_referential_action;
END

CLOSE constraints_cursor;
DEALLOCATE constraints_cursor;