package dbcopy

import groovy.sql.Sql

class GenericDatabase implements Database {
    Sql db

    String getSchema() {
        db.getConnection().getMetaData().getUserName()
    }


    String qualifyTable( String table ) {
         getSchema() + '.' + table
    }


    final List<String> getTables( String copy, String skip ) {
        def rs = getMetadata().getTables( null, getSchema(), null, ['TABLE'] as String[] )

        def copyPattern = ( copy ? '(?i)' + copy : null )
        def skipPattern = ( skip ? '(?i)' + skip : null )

        def tables = []
        while ( rs.next() ) {
            def table = rs.getString( "TABLE_NAME" )
            if (( !(copy?.trim()) || table.matches( ~copyPattern )) &&
                ( !(skip?.trim()) || !table.matches( ~skipPattern ))) {
                tables << table
            }
        }

        return tables.sort { it.toLowerCase() }
    }


    final List<String> getColumns( String table ) {
        def rs = getMetadata().getColumns( null, getSchema(), table, null)

        def columns = []
        while ( rs.next() ){
            columns << rs.getString("COLUMN_NAME")
        }

        return columns.sort { it.toLowerCase() }
    }


    String getSelectQuery( String table, List<String> columns ) {
        'select ' + columns.join( ', ' ) + ' from ' + qualifyTable( table )
    }


    String getInsertPreparedStatement( String table, List<String> columns ) {
        'insert into ' + qualifyTable( table ) + ' (' + 
            columns.join( ', ' ) + ') values (' +
            columns.collect{ '?' }.join( ', ' ) + ')'
    }


    def truncateTable( String table ){
        def sql = "truncate table ${qualifyTable( table )}"
        db.execute sql.toString()
    }


    def prepareForBulkLoad() {}
    def prepareForBulkLoad( String table, List<String> columns ) {}
    def finalizeBulkLoad() {}
    def finalizeBulkLoad( String table, List<String> columns ) {}


    final close() {
        db.close()
    }


    final getMetadata() {
        db.getConnection().getMetaData()
    }


    final String getType() {
        getMetadata().getDatabaseProductName()
    }

    String getChecksum( String table, List<String> columns ) {
        return null;
    }
}