begin
	for c in (
		select c.constraint_name, c.table_name, 'delete from ' || c.table_name 
			|| ' where rowid in ( select rid from ( select rowid rid, row_number() over ( partition by '
			|| listagg( cc.column_name, ', ' ) within group ( order by cc.position )
			|| ' order by rowid ) rn from ' || c.table_name || ' ) where rn <> 1 )' as query
		from user_constraints c, user_cons_columns cc
		where c.status = 'DISABLED' and c.constraint_type in ( 'P', 'U' ) 
			and cc.constraint_name = c.constraint_name
			and c.table_name not like 'BIN$%'
		group by c.constraint_name, c.table_name
		order by c.table_name
	) loop
		begin
			execute immediate c.query;
			if sql%rowcount > 0 then
				dbms_output.put_line( lpad( c.table_name, 30 ) 
					|| ' : ' || lpad( sql%rowcount, 5 ) || ' rows deleted (' || c.constraint_name || ') ' );
			end if;
		exception
			when others then 
				dbms_output.put_line( lpad( c.table_name, 30 ) || ' : ' || sqlerrm );
		end;
	end loop;
end;