package dyoon;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Created by Dong Young Yoon on 10/9/18. */
public class DatabaseTool {
  private Connection conn;
  private Cache cache;

  private static final double Z = 2.576; // 99% CI
  private static final double E = 0.01; // 1% error

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
        String tab = q.getJoinTableName();
        if (table.toLowerCase().equals(tab.toLowerCase())) {
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

  public Stat getGroupCountAndSize(Query q) {
    long populationSize = 0;
    long groupCount = 0;
    double avgGroupSize = 0;
    long maxGroupSize = 0;
    long minGroupSize = 0;
    double targetSampleSize = 0;

    String joinTableName = q.getJoinTableName();
    String statTableName = String.format("q%s__%.4f__%.4f", q.getId(), Z, E);
    statTableName = statTableName.replaceAll("\\.", "_");
    if (cache.loadStat(statTableName) != null) {
      return cache.loadStat(statTableName);
    }
    String qcsCols = Joiner.on(",").join(q.getQueryColumnSet());

    try {
      if (!checkTableExists(statTableName)) {
        conn.createStatement()
            .execute(
                String.format(
                    "CREATE TABLE %s STORED AS parquet AS SELECT %s, groupsize,"
                        + "(groupsize * (pow(%f,2)*0.25 / pow(%f,2)) ) / (groupsize + (pow(%f,2)*0.25 / pow(%f,2)) - 1) as target_group_sample_size "
                        + "FROM "
                        + "(SELECT %s,"
                        + "count(*) as groupsize from %s GROUP BY %s) t",
                    statTableName, qcsCols, Z, E, Z, E, qcsCols, joinTableName, qcsCols));
      }

      ResultSet rs =
          conn.createStatement()
              .executeQuery(
                  String.format(
                      "SELECT count(*) as group_count, sum(groupsize) as population_size, "
                          + "sum(target_group_sample_size) as target_sample_size, "
                          + "avg(groupsize) as avg_group_size,"
                          + "min(groupsize) as min_group_size,"
                          + "max(groupsize) as max_group_size "
                          + "FROM %s",
                      statTableName));

      if (rs.next()) {
        populationSize = rs.getLong("population_size");
        targetSampleSize = rs.getDouble("target_sample_size");
        groupCount = rs.getLong("group_count");
        avgGroupSize = rs.getDouble("avg_group_size");
        minGroupSize = rs.getLong("min_group_size");
        maxGroupSize = rs.getLong("max_group_size");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

    Stat stat =
        new Stat(
            populationSize, targetSampleSize, groupCount, avgGroupSize, minGroupSize, maxGroupSize);
    cache.saveStat(statTableName, stat);
    return stat;
  }
}
