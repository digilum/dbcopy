begin
	for c in (
		select t.trigger_name, t.table_name
		from user_triggers t
		where t.status = 'DISABLED'
		and t.table_name not like 'BIN$%'
		order by t.table_name
	) loop
		begin
			dbms_utility.exec_ddl_statement( 'alter trigger "' || c.trigger_name || '" enable' );
		exception
			when others then 
				dbms_output.put_line( lpad( c.table_name, 30 ) || ' : ' || sqlerrm );
		end;
	end loop;
end;