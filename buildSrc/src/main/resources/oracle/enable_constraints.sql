begin
	for c in (
		select c.table_name, c.constraint_name
		from user_constraints c
		where c.status = 'DISABLED'
		and c.table_name not like 'BIN$%'
		order by case c.constraint_type when 'R' then 1 else 2 end desc, c.table_name
	) loop
		begin
			dbms_utility.exec_ddl_statement( 'alter table ' || c.table_name 
				|| ' enable constraint ' || c.constraint_name );
		exception
			when others then
				dbms_output.put_line( lpad( c.table_name, 30 ) || ' : ' || sqlerrm );
		end;
	end loop;
end;