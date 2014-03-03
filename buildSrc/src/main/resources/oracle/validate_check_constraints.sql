begin
	for c in (
		select c.table_name, c.constraint_name, c.search_condition
		from user_constraints c, user_tables t
		where c.status = 'DISABLED'
			and c.search_condition is not null
			and c.table_name not like 'BIN$%'
		order by c.table_name
	) loop
		begin
			execute immediate 'delete from ' || c.table_name
				|| ' where not (' || c.search_condition || ')';
			if sql%rowcount > 0 then
				dbms_output.put_line( lpad( c.table_name, 30 ) 
					|| ' : ' || lpad( sql%rowcount, 5 ) || ' rows deleted (' || c.constraint_name || ')' );
			end if;
		exception
			when others then 
				dbms_output.put_line( lpad( c.table_name, 30 ) || ' : ' || sqlerrm );
		end;
	end loop;
end;