package dbcopy

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction
import groovy.time.TimeCategory
import groovy.sql.GroovyResultSet
import java.sql.SQLException
import java.util.logging.*

class CopyDataTask extends DefaultTask {
    String sourceUrl, sourceUser, sourcePassword
    String targetUrl, targetUser, targetPassword
    String copyTables, skipTables

    List< String > intersection( List< String > lhs, List< String > rhs ) {
        lhs.findAll {
            rhs.collect{ it.toUpperCase() }.contains( it.toUpperCase() )
        }
    }

    def trasformValue( String source, String target, value ) {
        def result = value

        // skip CLOBs migration
        if ( result.getClass().toString().toLowerCase().contains( 'clob' ) ) {
            result = null
        }

        // convert empty strings into whitespaces to avoid constraints violation in Oracle
        if ( result.getClass().toString().toLowerCase().contains( 'string' )
            && ( target == 'Oracle' ) && result == '' ) {
            result = ' '
        }
        return result
    }


    List getStatementValues( String source, String target, GroovyResultSet rs ) {
        return rs.toRowResult().values().toList().collect {
            trasformValue( source, target, it )
        }
    }

    def setLogging( conn, boolean state ) 
    { 
        java.util.logging.Logger logger = conn.LOG 
        java.util.logging.Filter filter = new java.util.logging.Filter() { 
            public boolean isLoggable(java.util.logging.LogRecord record) 
            { 
                return state 
            } 
        } 
        logger.setFilter( filter ) 
    } 


    @TaskAction
    def main () {
        Database sourceDb = DatabaseFactory.newInstance( sourceUrl, sourceUser, sourcePassword )
        Database targetDb = DatabaseFactory.newInstance( targetUrl, targetUser, targetPassword )

        logger.quiet '\nSource: ' + sourceUrl + '\nTarget: ' + targetUrl

        targetDb.prepareForBulkLoad()

        def sourceTablesAll = sourceDb.getTables( copyTables, skipTables )
        def targetTablesAll = targetDb.getTables( copyTables, skipTables )

        def sourceTables = intersection( sourceTablesAll, targetTablesAll )
        def targetTables = intersection( targetTablesAll, sourceTablesAll )

        sourceTables.eachWithIndex { sourceTable, i ->
            def timeStart = new Date()

            def targetTable = targetTables[ i ]
            
            logger.quiet '\n' + sourceTable.toUpperCase()

            def sourceColumnsAll = sourceDb.getColumns( sourceTable )
            def targetColumnsAll = targetDb.getColumns( targetTable )

            def sourceColumns = intersection( sourceColumnsAll, targetColumnsAll )
            def targetColumns = intersection( targetColumnsAll, sourceColumnsAll )

            def rows = 0
            def errors = 0

            if ( targetColumns.size > 0 && sourceColumns.size > 0 ) {

                def skip = false

                if ( sourceDb.getType() == targetDb.getType() ) {
                    def sourceChecksum = sourceDb.getChecksum( sourceTable, sourceColumns )
                    def targetChecksum = targetDb.getChecksum( sourceTable, sourceColumns )

                    if ( sourceChecksum && targetChecksum && sourceChecksum == targetChecksum ) {
                        skip = true
                        logger.quiet 'Skipped (up-to-date)'
                    }
                }

                if ( !skip ) {

                    try {
                        targetDb.prepareForBulkLoad( targetTable, targetColumns )
                    } catch ( SQLException e ) {
                        logger.info e.message
                    }

                    targetDb.truncateTable( targetTable )

                    setLogging( targetDb.db, false )

                    def insertPreparedStatement = 
                        targetDb.getInsertPreparedStatement( targetTable, targetColumns )

                    def mode = 'batch'

                    try { // Attemping to load data in batch mode ...               
                        rows = 0
                        targetDb.db.withBatch( 10000, insertPreparedStatement ) { stmt  -> 
                            sourceDb.db.eachRow( sourceDb.getSelectQuery( sourceTable, sourceColumns ) ) { row ->
                                stmt.addBatch( getStatementValues( sourceDb.getType(), targetDb.getType(), row ) )
                                rows = rows + 1
                            }
                        }               
                    } catch ( SQLException batchException ) { // Switching to serial mode ...
                        mode = 'serial'

                        logger.info batchException.message
                        targetDb.truncateTable( targetTable )

                        rows = 0
                        sourceDb.db.eachRow( sourceDb.getSelectQuery( sourceTable, sourceColumns ) ) { row ->
                            rows = rows + 1
                            def values = getStatementValues( sourceDb.getType(), targetDb.getType(), row )
                            try {
                                targetDb.db.execute ( insertPreparedStatement, values )
                            } catch( rowException ) {
                                errors = errors + 1
                                logger.info rowException.message
                                logger.info values.toString()
                            }
                        }
                    }

                    setLogging( targetDb.db, true )

                    try {
                        targetDb.finalizeBulkLoad( targetTable, targetColumns )
                    } catch ( SQLException e ) {
                        logger.info e.message
                    }
                                    
                    def timeStop = new Date()
                    logger.quiet( (rows - errors).toString() + ' ' + (rows - errors == 1 ? 'row' : 'rows') + ' migrated in ' +
                        TimeCategory.minus( timeStop, timeStart ) + ' (' + mode + ' mode)' )
                    if ( errors > 0 ) {
                        logger.quiet( errors.toString() + ' ' + (errors == 1 ? 'row' : 'rows') + ' skipped due to errors' )
                    }
                }
            } else {
                logger.warn 'No intersection of source and target column sets'
            }
        }

        targetDb.finalizeBulkLoad()

        sourceDb.close()
        targetDb.close()
    }

}