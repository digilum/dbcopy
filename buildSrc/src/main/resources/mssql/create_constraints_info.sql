if (object_id(N'dbo.constraints_info', 'U') is null)
	create table dbo.constraints_info (
		constraint_id int identity primary key,
		table_name sysname not null,
		constraint_name sysname not null,
		constraint_type char(2) not null,
		create_sql nvarchar(max),
		drop_sql nvarchar(max),
		validate_sql nvarchar(max)
	);
