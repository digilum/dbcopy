package dbcopy

class MssqlDatabase extends GenericDatabase
{
    String getSchema() { 'dbo' }


    String qualifyTable( String table ) {
        '[' + getSchema() + '].[' + table + ']'
    }

    String getInsertPreparedStatement( String table, List<String> columns ) {
        'insert into [#' + table + '] (' + 
            columns.join( ', ' ) + ') values (' +
            columns.collect{ '?' }.join( ', ' ) + ')'
    }


    def prepareForBulkLoad() {
        println '\nDisabling constraints and triggers ...\n'

        db.execute """declare @sql nvarchar(max) = 
            'alter database ' + quotename(db_name()) + ' set recovery simple with no_wait;'
            exec sp_executesql @sql;""";

        db.execute getClass().getResource( '/mssql/create_constraints_info.sql' ).text
        db.execute getClass().getResource( '/mssql/disable_referential_constraints.sql' ).text
        db.execute getClass().getResource( '/mssql/disable_unique_constraints.sql' ).text
        db.execute getClass().getResource( '/mssql/disable_check_constraints.sql' ).text
        db.execute getClass().getResource( '/mssql/disable_triggers.sql' ).text
    }


    def finalizeBulkLoad() {
        println '\nFixing non-valid data ...\n'

        db.execute getClass().getResource( '/mssql/enable_constraints.sql' ).text
        db.execute getClass().getResource( '/mssql/validate_constraints.sql' ).text

        println '\nEnabling constraints and triggers ...\n'
  
        db.execute getClass().getResource( '/mssql/enable_constraints.sql' ).text
        db.execute getClass().getResource( '/mssql/enable_triggers.sql' ).text
        db.execute getClass().getResource( '/mssql/drop_constraints_info.sql' ).text

        db.execute """declare @sql nvarchar(max) = 
            'alter database ' + quotename(db_name()) + ' set recovery full with no_wait;'
            exec sp_executesql @sql;""";
    }


    def prepareForBulkLoad( String table, List<String> columns ) {
        String schemaTable = qualifyTable( table )

        String sql = "select top 0 * into [#${table}] from ${schemaTable}"
        db.execute sql.toString()

        sql = "select objectproperty(object_id('${schemaTable}'), 'TableHasIdentity') has_identity"
        def identities = db.rows( sql )
        if ( identities[0].has_identity > 0 ) {
            sql = "set identity_insert [#${table}] on"
            db.execute sql.toString()
        }
    }


    def finalizeBulkLoad( String table, List<String> columns ) {
        String schemaTable = qualifyTable( table )

        String sql = "select objectproperty(object_id('${schemaTable}'), 'TableHasIdentity') has_identity"
        def identities = db.rows( sql )

        if ( identities[0].has_identity > 0 ) {
            sql = "set identity_insert [#${table}] off"
            db.execute sql.toString()
        }

        if ( identities[0].has_identity > 0 ) {
            sql = "set identity_insert ${schemaTable} on"
            db.execute sql.toString()
        }

        sql = "insert into ${schemaTable} (${columns.join( ', ' )}) select ${columns.join( ', ' )} from [#${table}]"
        db.execute sql.toString()

        if ( identities[0].has_identity > 0 ) {
            sql = "set identity_insert ${schemaTable} off"
            db.execute sql.toString()
        }
    }


    String getChecksum( String table, List<String> columns ) {
        String schemaTable = qualifyTable( table )
        String checksum = null


        String sql = """select count(*) bin_count from sys.columns as c
            join sys.types as t on c.user_type_id = t.user_type_id
            where t.name in ('image', 'text', 'ntext', 'xml')
            and c.object_id = object_id('${schemaTable}')
            and c.name in (${columns.collect{ '\''+it+'\'' }.join( ', ' )})""";

        def rows = db.rows( sql )

        if ( rows[0].bin_count == 0 ) {
            sql = "select checksum_agg(binary_checksum(${columns.join( ', ' )})) checksum from ${schemaTable}"

            try {
                def checksums = db.rows( sql )
                checksum = checksums[0].checksum.toString()
            } catch(e) {}
        }
        
        return checksum
    }
}