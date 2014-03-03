declare
	l_val number;
begin
	for s in (select sequence_name from user_sequences) loop
		execute immediate 'select ' || s.sequence_name || '.nextval from dual' into l_val;
		execute immediate 'alter sequence ' || s.sequence_name || ' increment by ' || (1000000 - l_val);
		execute immediate 'select ' || s.sequence_name || '.nextval from dual' into l_val;
		execute immediate 'alter sequence ' || s.sequence_name || ' increment by 1';
	end loop;
end;
