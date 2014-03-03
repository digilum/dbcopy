declare cur cursor 
for 
	select name
	from sysobjects 
	where type = 'U'
go

declare @table varchar(500)
declare @sql varchar(500)

open cur
fetch cur into @table 
while (@@sqlstatus = 0)
begin
	select @sql = 'alter table ' + @table + ' disable trigger'
	exec( @sql)
	fetch cur into @table 
end
close cur

deallocate cursor cur
