package dyoon;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Created by Dong Young Yoon on 10/9/18. */
public class DatabaseTool {
  private Connection conn;

  public DatabaseTool(Connection conn) {
    this.conn = conn;
  }

  public boolean checkTableExists(String table) throws SQLException {
    DatabaseMetaData dbm = conn.getMetaData();
    // check if "employee" table is there
    ResultSet tables = dbm.getTables(null, null, table, null);
    return tables.next();
  }
}
