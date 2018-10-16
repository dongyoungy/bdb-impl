package dyoon;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/** Created by Dong Young Yoon on 10/9/18. */
public class DatabaseTool {
  private final Connection conn;
  private final Meta meta;

  private static final double Z = 2.576; // 99% CI
  private static final double E = 0.01; // 1% error

  public DatabaseTool(final Connection conn) {
    this.conn = conn;
    this.meta = Meta.getInstance(conn);
  }

  public boolean checkTableExists(final String table) throws SQLException {
    final DatabaseMetaData dbm = this.conn.getMetaData();
    final ResultSet tables = dbm.getTables(null, null, table, null);
    return tables.next();
  }

  public boolean checkTableExists(String database, String table) throws SQLException {
    final DatabaseMetaData dbm = this.conn.getMetaData();
    final ResultSet tables = dbm.getTables(null, database, table, null);
    return tables.next();
  }

  public boolean checkTableExists(final Prejoin table) throws SQLException {
    final DatabaseMetaData dbm = this.conn.getMetaData();
    final ResultSet tables = dbm.getTables(null, table.getDatabase(), table.getName(), null);
    return tables.next();
  }

  public void addPrejoin(Prejoin p) {
    this.meta.addPrejoin(p);
  }

  public void createSample(final String database, final Sample s, boolean overwrite) {
    final String sampleTable = s.toString();

    try {
      if (this.checkTableExists(sampleTable) && !overwrite) {
        // sample already exists
        System.out.println("Sample already exists: " + sampleTable);
        return;
      }

      System.out.println("Creating a sample: " + sampleTable);
      if (s.getType() == Sample.Type.UNIFORM) {
        this.createUniformSample(database, s);
      } else if (s.getType() == Sample.Type.STRATIFIED) {
        this.createStratifiedSample(database, s);
      } else if (s.getType() == Sample.Type.STRATIFIED2) {
        this.createStratified2Sample(database, s);
      } else {
        System.out.println("Unsupported sample type: " + s.toString());
        return;
      }
      meta.addSample(s);
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  public void testSample(Sample s, Query q, boolean measureTime, String cacheClearScript)
      throws SQLException {
    String sql = q.getQuery();
    String statTable = String.format("q%s__%.4f__%.4f", s.getQuery().getId(), s.getZ(), s.getE());
    statTable = statTable.replaceAll("\\.", "_");
    String originalQuery = sql.replaceAll("FACT_TABLE", q.getFactTable());
    String sampleQuery;
    if (q.getSampleQuery().isEmpty()) {
      sampleQuery = sql.replaceAll("FACT_TABLE", s.toString());
    } else {
      sampleQuery =
          q.getSampleQuery()
              .replaceAll("FACT_TABLE", s.toString())
              .replaceAll("STAT_TABLE", statTable);
    }

    String origResultTable = String.format("q%s_orig", q.getId());
    String sampleResultTable = String.format("q%s_%s", q.getId(), s.toString());

    double origTimeTaken = 0, sampleTimeTaken = 0;
    Stopwatch watch;

    if (measureTime && cacheClearScript.isEmpty()) {
      System.out.println("You must provide a script for clearing cache to measure times.");
      return;
    }

    if (!checkTableExists(origResultTable) || measureTime) {
      conn.createStatement().execute(String.format("DROP TABLE IF EXISTS %s", origResultTable));
      clearCache(cacheClearScript);
      String createSql =
          String.format("CREATE TABLE %s STORED AS parquet AS %s", origResultTable, originalQuery);
      watch = Stopwatch.createStarted();
      conn.createStatement().execute(createSql);
      watch.stop();
      origTimeTaken = watch.elapsed(TimeUnit.MILLISECONDS);
    }
    if (!checkTableExists(sampleResultTable) || measureTime) {
      conn.createStatement().execute(String.format("DROP TABLE IF EXISTS %s", sampleResultTable));
      boolean statFound = false;
      ResultSet rs =
          conn.createStatement().executeQuery(String.format("SHOW TABLE STATS %s", s.toString()));
      if (rs.next()) {
        long numFile = rs.getLong(1);
        if (numFile != -1) {
          statFound = true;
        }
      }

      if (!statFound) {
        conn.createStatement().execute(String.format("COMPUTE STATS %s", s.toString()));
      }
      clearCache(cacheClearScript);
      String createSql =
          String.format("CREATE TABLE %s STORED AS parquet AS %s", sampleResultTable, sampleQuery);
      watch = Stopwatch.createStarted();
      conn.createStatement().execute(createSql);
      watch.stop();
      sampleTimeTaken = watch.elapsed(TimeUnit.MILLISECONDS);
    }

    TreeSet<String> aggColumns = q.getAggColumns();
    TreeSet<String> groupByColumns = q.getGroupByColumns();

    List<String> evalItems = new ArrayList<>();
    for (String col : aggColumns) {
      evalItems.add(
          String.format(
              "abs(quotient(((s.%s / s.groupsize * o.groupsize) - "
                  + "o.%s) * 100000, o.%s) / 100000)",
              col, col, col));
    }
    String sumEval = Joiner.on(" + ").join(evalItems);
    String selectClause =
        String.format("(avg(%s) / %d) as avg_per_error", sumEval, evalItems.size());
    String fromClause = String.format("%s as o, %s as s", origResultTable, sampleResultTable);

    List<String> joinItems = new ArrayList<>();
    for (String col : groupByColumns) {
      joinItems.add(String.format("o.%s = s.%s", col, col));
    }
    String joinClause = Joiner.on(" AND ").join(joinItems);

    String evalSql =
        String.format("SELECT %s FROM %s WHERE %s", selectClause, fromClause, joinClause);

    String origGroupCountSql =
        String.format("SELECT count(*) as groupcount from %s", origResultTable);
    String sampleGroupCountSql =
        String.format("SELECT count(*) as groupcount from %s", sampleResultTable);

    long origGroupCount = 0, sampleGroupCount = 0;
    double missingGroupRatio = 0, avgPercentError = 0;
    ResultSet rs = conn.createStatement().executeQuery(origGroupCountSql);
    if (rs.next()) {
      origGroupCount = rs.getLong("groupcount");
    }
    rs.close();

    rs = conn.createStatement().executeQuery(sampleGroupCountSql);
    if (rs.next()) {
      sampleGroupCount = rs.getLong("groupcount");
    }
    rs.close();

    rs = conn.createStatement().executeQuery(evalSql);
    if (rs.next()) {
      avgPercentError = rs.getDouble("avg_per_error");
    }

    missingGroupRatio = (double) (origGroupCount - sampleGroupCount) / (double) origGroupCount;
    System.out.println(
        String.format(
            "q%s with sample %s gives: missing group ratio = %.4f %%, "
                + "avg. percent error = %.4f %%, original time taken = %.4f s, sample time taken = %.4f s ",
            q.getId(),
            s.toString(),
            missingGroupRatio * 100,
            avgPercentError * 100,
            origTimeTaken / 1000,
            sampleTimeTaken / 1000));
  }

  private void clearCache(String cacheClearScript) {
    try {
      Process p = new ProcessBuilder("/bin/bash", cacheClearScript).start();
      p.waitFor();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void createStratifiedSample(final String database, final Sample s) throws SQLException {
    final String sampleTable = s.toString();
    final String factTable = s.getQuery().getFactTable();
    String sourceTable = factTable;
    String statTable = String.format("q%s__%.4f__%.4f", s.getQuery().getId(), s.getZ(), s.getE());
    statTable = statTable.replaceAll("\\.", "_");
    if (!this.checkTableExists(statTable)) {
      System.out.println("Stat table does not exist: " + statTable);
      return;
    }

    if (s.getJoinTables().size() > 1) {
      Prejoin p = meta.getPrejoinForSample(database, s);
      if (p == null) {
        System.out.println("Prejoin required for sample does not exist: " + s.toString());
      } else {
        sourceTable = p.getName();
      }
    }

    final List<String> factTableColumns = this.getColumns(factTable);
    final SortedSet<String> sampleColumns = s.getColumns();
    final List<String> sampleColumnsWithFactPrefix = new ArrayList<>();
    final List<String> joinColumns = new ArrayList<>();
    for (final String column : sampleColumns) {
      sampleColumnsWithFactPrefix.add(String.format("fact.%s", column));
      joinColumns.add(String.format("fact.%s = stat.%s", column, column));
    }

    final String sampleQCSClause = Joiner.on(",").join(sampleColumnsWithFactPrefix);
    final String joinClause = Joiner.on(" AND ").join(joinColumns);

    final String createSql =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s LIKE %s.%s STORED as parquet",
            database, sampleTable, database, factTable);

    this.conn.createStatement().execute(createSql);

    final String insertSql =
        String.format(
            "INSERT OVERWRITE TABLE %s.%s SELECT %s FROM "
                + "(SELECT fact.*, row_number() OVER (PARTITION BY %s ORDER BY %s) as rownum, "
                + "count(*) OVER (PARTITION BY %s ORDER BY %s) as groupsize, "
                + "stat.target_group_sample_size as target_group_sample_size "
                + "FROM %s as fact, %s as stat "
                + "WHERE %s ORDER BY rand()) tmp "
                + "WHERE tmp.rownum <= tmp.target_group_sample_size OR "
                + "(tmp.rownum > tmp.target_group_sample_size AND "
                + "rand(unix_timestamp()) < (tmp.target_group_sample_size / 20) / tmp.groupsize)",
            database,
            sampleTable,
            Joiner.on(",").join(factTableColumns),
            sampleQCSClause,
            sampleQCSClause,
            sampleQCSClause,
            sampleQCSClause,
            sourceTable,
            statTable,
            joinClause);
    System.err.println(String.format("Executing: %s", insertSql));

    this.conn.createStatement().execute(insertSql);
    this.conn
        .createStatement()
        .execute(String.format("COMPUTE STATS %s.%s", database, sampleTable));
  }

  private void createStratified2Sample(final String database, final Sample s) throws SQLException {
    final String sampleTable = s.toString();
    final String factTable = s.getTable();
    String sourceTable = factTable;

    if (s.getJoinTables().size() > 1) {
      Prejoin p = meta.getPrejoinForSample(database, s);
      if (p == null) {
        System.out.println("Prejoin required for sample does not exist: " + s.toString());
      } else {
        sourceTable = p.getName();
      }
    }

    final List<String> factTableColumns = this.getColumns(factTable);
    final SortedSet<String> sampleColumns = s.getColumns();
    final List<String> sampleColumnsWithFactPrefix = new ArrayList<>();
    for (final String column : sampleColumns) {
      sampleColumnsWithFactPrefix.add(String.format("fact.%s", column));
    }

    final String sampleQCSClause = Joiner.on(",").join(sampleColumnsWithFactPrefix);

    final String createSql =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s LIKE %s.%s STORED as parquet",
            database, sampleTable, database, factTable);

    this.conn.createStatement().execute(createSql);

    final String insertSql =
        String.format(
            "INSERT OVERWRITE TABLE %s.%s SELECT %s FROM "
                + "(SELECT fact.*, row_number() OVER (PARTITION BY %s ORDER BY %s) as rownum, "
                + "count(*) OVER (PARTITION BY %s ORDER BY %s) as groupsize, "
                + "%d as target_group_sample_size "
                + "FROM %s as fact "
                + "ORDER BY rand()) tmp "
                + "WHERE tmp.rownum <= %d OR "
                + "(tmp.rownum > %d AND "
                + "rand(unix_timestamp()) < (%d / 20) / tmp.groupsize)",
            database,
            sampleTable,
            Joiner.on(",").join(factTableColumns),
            sampleQCSClause,
            sampleQCSClause,
            sampleQCSClause,
            sampleQCSClause,
            s.getMinRow(),
            sourceTable,
            s.getMinRow(),
            s.getMinRow(),
            s.getMinRow());
    //            statTable,
    //            joinClause);
    System.err.println(String.format("Executing: %s", insertSql));

    this.conn.createStatement().execute(insertSql);
    this.conn
        .createStatement()
        .execute(String.format("COMPUTE STATS %s.%s", database, sampleTable));
  }

  private void createUniformSample(String database, Sample s) throws SQLException {
    final String sampleTable = s.toString();
    final String factTable = s.getQuery().getFactTable();
    final List<String> factTableColumns = this.getColumns(factTable);
    final String createSql =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s LIKE %s.%s STORED as parquet",
            database, sampleTable, database, factTable);

    conn.createStatement().execute(createSql);

    String insertSql =
        String.format(
            "INSERT OVERWRITE TABLE %s.%s SELECT %s FROM %s WHERE rand(unix_timestamp()) < %f",
            database, sampleTable, Joiner.on(",").join(factTableColumns), factTable, s.getRatio());

    System.err.println(String.format("Executing: %s", insertSql));
    this.conn.createStatement().execute(insertSql);
    this.conn
        .createStatement()
        .execute(String.format("COMPUTE STATS %s.%s", database, sampleTable));
  }

  public void findOrCreateJoinTable(final Query q) {

    if (q.getJoinedTables().isEmpty()) {
      return;
    }

    final String joinTable = this.findJoinTable(q);
    if (joinTable != null) {
      System.out.println("Found join table: " + joinTable);
      q.setJoinTableName(joinTable);
      return;
    }

    final Joiner j = Joiner.on("_");
    final Joiner j2 = Joiner.on(",");
    final Joiner j3 = Joiner.on(" AND ");

    final String joinTableName = q.getJoinTableName();
    final String joinTables = j2.join(q.getJoinedTables());

    final List<String> joinColumns = new ArrayList<>();
    for (final ColumnPair pair : q.getJoinColumns()) {
      joinColumns.add(pair.getLeft() + " = " + pair.getRight());
    }
    final String joinClause = j3.join(joinColumns);

    try {
      if (!this.checkTableExists(joinTableName)) {
        System.out.println("Creating join table: " + joinTableName);
        final String sql =
            String.format(
                "CREATE TABLE %s STORED AS parquet AS SELECT * FROM %s WHERE %s",
                joinTableName, joinTables, joinClause);
        this.conn.createStatement().execute(sql);
      }
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  public void createPrejoinTable(final Prejoin p) {

    final String database = p.getDatabase();
    final String joinTableName = p.getName();
    final Joiner j2 = Joiner.on(",");
    final Joiner j3 = Joiner.on(" AND ");

    final String joinTables = j2.join(p.getTableSet());

    final List<String> joinColumns = new ArrayList<>();
    for (ColumnPair pair : p.getJoinColumnSet()) {
      joinColumns.add(pair.getLeft() + " = " + pair.getRight());
    }
    final String joinClause = j3.join(joinColumns);

    try {
      if (!this.checkTableExists(joinTableName)) {
        final String sql =
            String.format(
                "CREATE TABLE %s.%s STORED AS parquet AS SELECT * FROM %s WHERE %s",
                database, joinTableName, joinTables, joinClause);
        System.out.println("Creating join table:\n\t" + sql);
        this.conn.createStatement().execute(sql);
      }
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  private String findJoinTable(final Query q) {
    try {
      final ResultSet rs = this.conn.createStatement().executeQuery("SHOW TABLES");
      while (rs.next()) {
        final String table = rs.getString(1);
        final String tab = q.getJoinTableName();
        if (table.toLowerCase().equals(tab.toLowerCase())) {
          return table;
        }
      }
    } catch (final SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  public List<String> getColumns(final String table) {
    final List<String> columns = new ArrayList<>();
    try {
      final ResultSet rs =
          this.conn.createStatement().executeQuery(String.format("DESCRIBE %s", table));
      while (rs.next()) {
        columns.add(rs.getString(1));
      }
    } catch (final SQLException e) {
      e.printStackTrace();
    }
    return columns;
  }

  public Stat getGroupCountAndSize(
      final String database, final Query q, final List<Prejoin> prejoins) {
    long populationSize = 0;
    long groupCount = 0;
    double avgGroupSize = 0;
    long maxGroupSize = 0;
    long minGroupSize = 0;
    double targetSampleSize = 0;
    String joinTableName = "";

    if (q.getJoinedTables().size() == 1) {
      joinTableName = q.getFactTable();
    } else {
      for (final Prejoin prejoin : prejoins) {
        if (prejoin.supports(database, q)) {
          joinTableName = prejoin.getName();
          break;
        }
      }
    }

    if (joinTableName.isEmpty()) {
      return null;
    }

    String statTableName =
        String.format("q%s__%.4f__%.4f", q.getId(), DatabaseTool.Z, DatabaseTool.E);
    statTableName = statTableName.replaceAll("\\.", "_");
    Stat stat = this.meta.loadStat(database, statTableName);
    final String qcsCols = Joiner.on(",").join(q.getQueryColumnSet());

    if (stat != null && stat.getPopulationSize() == 0) {
      stat = null;
    }

    try {
      if (!this.checkTableExists(statTableName)) {
        if (stat != null) {
          this.conn
              .createStatement()
              .execute(
                  String.format(
                      "CREATE TABLE %s STORED AS parquet AS SELECT * FROM %s",
                      statTableName, stat.getTableName()));
        } else {
          this.conn
              .createStatement()
              .execute(
                  String.format(
                      "CREATE TABLE %s STORED AS parquet AS SELECT %s, groupsize,"
                          + "(groupsize * (pow(%f,2)*0.25 / pow(%f,2)) ) / "
                          + "(groupsize + (pow(%f,2)*0.25 / pow(%f,2)) - 1) "
                          + "as target_group_sample_size "
                          + "FROM "
                          + "(SELECT %s,"
                          + "count(*) as groupsize from %s GROUP BY %s) t",
                      statTableName,
                      qcsCols,
                      DatabaseTool.Z,
                      DatabaseTool.E,
                      DatabaseTool.Z,
                      DatabaseTool.E,
                      qcsCols,
                      joinTableName,
                      qcsCols));
        }
      }

      if (stat == null) {
        final ResultSet rs =
            this.conn
                .createStatement()
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

        stat =
            new Stat(
                database,
                q,
                statTableName,
                populationSize,
                targetSampleSize,
                groupCount,
                avgGroupSize,
                minGroupSize,
                maxGroupSize);
      }
    } catch (final SQLException e) {
      e.printStackTrace();
    }

    Stat s = meta.loadStat(database, statTableName);

    if (s == null) {
      this.meta.saveStat(database, statTableName, stat);
    }

    return stat;
  }
}
