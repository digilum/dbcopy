package dbcopy

import groovy.sql.Sql

class OracleDatabase extends GenericDatabase
{
    String getInsertPreparedStatement( String table, List<String> columns ) {
        String schemaTable = qualifyTable( table )
        'insert /*+ append */ into ' + schemaTable + ' (' + 
            columns.join( ', ' ) + ') values (' +
            columns.collect{ '?' }.join( ', ' ) + ')'
    }


    def prepareForBulkLoad() {
        // Turn on DBMS_OUTPUT
        enableOracleOutput()

        println '\nDisabling constraints and triggers ...\n'

        db.call getClass().getResource( '/oracle/disable_constraints.sql' ).text
        db.call getClass().getResource( '/oracle/disable_triggers.sql' ).text
        printOracleOutput()

        db.execute 'alter session enable parallel dml'
    }


    def finalizeBulkLoad() {
        db.execute 'alter session disable parallel dml'

        // Fix non-valid data in the target DB
        println '\nFixing non-valid data ...\n'

        disableOracleOutput()
        db.call getClass().getResource( '/oracle/enable_constraints.sql' ).text
        enableOracleOutput()
        db.call getClass().getResource( '/oracle/validate_check_constraints.sql' ).text
        printOracleOutput()
        db.call getClass().getResource( '/oracle/validate_unique_constraints.sql' ).text
        printOracleOutput()
        db.call getClass().getResource( '/oracle/validate_reference_constraints.sql' ).text
        printOracleOutput()

        // Enable constraints and triggers in the target DB
        println '\nEnabling constraints and triggers ...\n'

        db.call getClass().getResource( '/oracle/enable_constraints.sql' ).text
        db.call getClass().getResource( '/oracle/enable_triggers.sql' ).text
        printOracleOutput()

        // Reset sequences in the target DB
        println '\nResetting sequences ...\n'
        db.call getClass().getResource( '/oracle/reset_sequences.sql' ).text
    }


    def prepareForBulkLoad( String table, List<String> columns ) {
        String schemaTable = qualifyTable( table )

        db.execute 'alter table ' + schemaTable + ' nologging'
    }


    def finalizeBulkLoad( String table, List<String> columns ) {
        String schemaTable = qualifyTable( table )

        db.execute 'alter table ' + schemaTable + ' logging'
    }


    def enableOracleOutput() {
        db.call( 'begin dbms_output.enable( 1000000 ); end;' )
    }


    def disableOracleOutput() {
        db.call( 'begin dbms_output.disable(); end;' )

    }


    def printOracleOutput() {
        def eof = false
        while ( !eof ) {
            db.call( "begin dbms_output.get_line( ${Sql.VARCHAR}, ${Sql.INTEGER} ); end;" ) { line, status ->
                if ( status == 1 ) eof = true else println "${line}"
            }
        }
    }
}