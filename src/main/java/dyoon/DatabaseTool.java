package dyoon;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Created by Dong Young Yoon on 10/9/18. */
public class DatabaseTool {
  private Connection conn;
  private Cache cache;

  public DatabaseTool(Connection conn) {
    this.conn = conn;
    this.cache = Cache.getInstance();
  }

  public boolean checkTableExists(String table) throws SQLException {
    DatabaseMetaData dbm = conn.getMetaData();
    ResultSet tables = dbm.getTables(null, null, table, null);
    return tables.next();
  }

  public void findOrCreateJoinTable(Query q) {

    if (q.getJoinedTables().isEmpty()) {
      return;
    }

    String joinTable = this.findJoinTable(q);
    if (joinTable != null) {
      System.out.println("Found join table: " + joinTable);
      q.setJoinTableName(joinTable);
      return;
    }

    Joiner j = Joiner.on("_");
    Joiner j2 = Joiner.on(",");
    Joiner j3 = Joiner.on(" AND ");

    String joinTableName = q.getJoinTableName();
    String joinTables = j2.join(q.getJoinedTables());

    List<String> joinColumns = new ArrayList<>();
    for (Pair<String, String> pair : q.getJoinColumns()) {
      joinColumns.add(pair.getLeft() + " = " + pair.getRight());
    }
    String joinClause = j3.join(joinColumns);

    try {
      if (!checkTableExists(joinTableName)) {
        System.out.println("Creating join table: " + joinTableName);
        String sql =
            String.format(
                "CREATE TABLE %s STORED AS parquet AS SELECT * FROM %s WHERE %s",
                joinTableName, joinTables, joinClause);
        conn.createStatement().execute(sql);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private String findJoinTable(Query q) {
    try {
      ResultSet rs = conn.createStatement().executeQuery("SHOW TABLES");
      while (rs.next()) {
        String table = rs.getString(1);
        boolean containsAll = true;
        for (String t : q.getJoinedTables()) {
          if (!table.toLowerCase().contains(t)) {
            containsAll = false;
            break;
          }
        }
        if (containsAll) {
          return table;
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  public List<String> getColumns(String table) {
    List<String> columns = new ArrayList<>();
    try {
      ResultSet rs = conn.createStatement().executeQuery(String.format("DESCRIBE %s", table));
      while (rs.next()) {
        columns.add(rs.getString(1));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return columns;
  }

  public Pair<Long, Double> getGroupCountAndSize(Query q) {
    long groupCount = 0;
    double avgGroupSize = 0;
    if (cache.getGroupCount(q) != null && cache.getAverageGroupSize(q) != null) {
      groupCount = cache.getGroupCount(q);
      avgGroupSize = cache.getAverageGroupSize(q);
      return ImmutablePair.of(groupCount, avgGroupSize);
    }
    Joiner j = Joiner.on("_");
    String joinTableName = j.join(q.getJoinedTables());
    String tempTableName = "temp_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
    String qcsCols = Joiner.on(",").join(q.getQueryColumnSet());

    try {
      conn.createStatement()
          .execute(
              String.format(
                  "CREATE TABLE %s STORED AS parquet AS SELECT %s,"
                      + "count(*) as groupsize from %s GROUP BY %s",
                  tempTableName, qcsCols, joinTableName, qcsCols));

      ResultSet rs =
          conn.createStatement()
              .executeQuery(
                  String.format(
                      "SELECT count(*) as group_count, avg(groupsize) as avg_group_size from %s",
                      tempTableName));

      if (rs.next()) {
        groupCount = rs.getLong("group_count");
        avgGroupSize = rs.getDouble("avg_group_size");
      }
      conn.createStatement().execute(String.format("DROP TABLE IF EXISTS %s", tempTableName));
    } catch (SQLException e) {
      e.printStackTrace();
    }

    cache.setGroupCount(q, groupCount);
    cache.setAverageGroupSize(q, avgGroupSize);
    return ImmutablePair.of(groupCount, avgGroupSize);
  }
}
