if (object_id(N'dbo.constraints_info', 'U') is not null)
	if not exists (
	  select 1 from dbo.constraints_info
	)
		drop table dbo.constraints_info;
