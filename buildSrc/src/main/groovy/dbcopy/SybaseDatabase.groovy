package dbcopy

class SybaseDatabase extends GenericDatabase
{
    String getSchema() { 'dbo' }


    def prepareForBulkLoad() {
        db.execute 'set transaction isolation level 0'
    }


    def finalizeBulkLoad() {
        db.execute 'set transaction isolation level 1'
    }


    def prepareForBulkLoad( String table, List<String> columns ) {
		db.execute 'alter table ' + table + ' disable trigger'

		String sql = 'select c.name from syscolumns c where c.status & 128 > 0 and id = object_id(\'dbo.' + table + '\')'
		def identities = db.rows( sql )
		if ( identities.size > 0 ) {
			sql = 'set identity_insert ' + table + ' on'
			db.execute sql
		}
    }


    def finalizeBulkLoad( String table, List<String> columns ) {
		String sql = 'select c.name from syscolumns c where c.status & 128 > 0 and id = object_id(\'dbo.' + table + '\')'
		def identities = db.rows( sql )
		if ( identities.size > 0 ) {
			sql = 'set identity_insert ' + table + ' off'
			db.execute sql
		}
		db.execute 'alter table ' + table + ' enable trigger'
    }
}