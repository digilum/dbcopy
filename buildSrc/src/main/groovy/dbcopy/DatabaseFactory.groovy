package dbcopy

import groovy.sql.Sql
import java.sql.SQLException

class DatabaseFactory {
    static Database newInstance( String url, String user, String password ) {
        Sql sql
        try {
            sql = Sql.newInstance( url, user, password )
        } catch ( SQLException e ) {
            println e.getMessage()
            throw e
        }

        def metaData = sql.getConnection().getMetaData()
        Database db

        switch ( metaData.getDatabaseProductName() ) {
            case 'Oracle':
                db = new OracleDatabase( db: sql )
                break
            case 'Microsoft SQL Server':
                db = new MssqlDatabase( db: sql )
                break
            case 'ASE':
                db = new SybaseDatabase( db: sql )
                break
            default:
                db = new GenericDatabase( db: sql )
        }
    }
}
