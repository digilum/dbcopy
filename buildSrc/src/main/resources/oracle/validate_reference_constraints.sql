begin
	for c in (
		select c.constraint_name, c.table_name, 'delete from ' || c.table_name 
			|| ' t where not exists (select 1 from ' || r.table_name || ' r where ' 
			|| listagg( 't.' || cc.column_name || ' = r.' || rc.column_name, ' and ' )
			within group ( order by cc.position ) || ') and (' 
			|| listagg( 't.' || cc.column_name || ' is not null', ' and ' )
			within group ( order by cc.position ) || ')' as query
		from user_constraints c, user_constraints r, user_cons_columns cc, user_cons_columns rc
		where c.status = 'DISABLED' and c.constraint_type = 'R' 
			and c.r_constraint_name = r.constraint_name
			and cc.constraint_name = c.constraint_name
			and rc.constraint_name = r.constraint_name
			and cc.position = rc.position
			and c.table_name not like 'BIN$%'
		group by c.constraint_name, c.table_name, r.table_name
		order by c.table_name

	) loop
		begin
			execute immediate c.query;
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