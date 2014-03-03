package dbcopy

interface Database  {
    String getType()
    List<String> getTables( String copy, String skip )
    List<String> getColumns( String table )
    String getSelectQuery( String table, List<String> columns )
    String getInsertPreparedStatement( String table, List<String> columns )
    def truncateTable( String table )
    def prepareForBulkLoad()
    def finalizeBulkLoad()
    def prepareForBulkLoad( String table, List<String> columns )
    def finalizeBulkLoad( String table, List<String> columns )
    String getChecksum( String table, List<String> columns )
}