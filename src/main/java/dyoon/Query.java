package dyoon;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/** Created by Dong Young Yoon on 10/9/18. */
public class Query {
  private int id;
  private String joinTableName;
  private String query = "";
  private SortedSet<String> queryColumnSet;
  private SortedSet<String> joinedTables;
  private Set<Pair<String, String>> joinColumns;

  private static final String[] FACT_TABLES = {
    "store_sales",
    "store_returns",
    "catalog_sales",
    "catalog_returns",
    "web_sales",
    "web_returns",
    "inventory"
  };

  public Query(
      int id,
      List<String> queryColumns,
      List<String> joinedTables,
      List<Pair<String, String>> joinColumns) {
    this.id = id;
    this.queryColumnSet = new TreeSet<>(queryColumns);
    this.joinedTables = new TreeSet<>(joinedTables);
    this.joinColumns = new HashSet<>(joinColumns);
    this.joinTableName = Joiner.on("_").join(joinedTables);
  }

  public String getJoinTableName() {
    return joinTableName;
  }

  public void setJoinTableName(String joinTableName) {
    this.joinTableName = joinTableName;
  }

  public SortedSet<String> getJoinedTables() {
    return joinedTables;
  }

  public Set<String> getQueryColumnSet() {
    return queryColumnSet;
  }

  public String getQCSString() {
    return Joiner.on("_").join(queryColumnSet);
  }

  public Set<Pair<String, String>> getJoinColumns() {
    return joinColumns;
  }

  public ResultSet runQuery(Connection conn) {
    return null;
  }

  public int getId() {
    return id;
  }

  public String getFactTable() {
    for (String table : joinedTables) {
      if (Arrays.asList(FACT_TABLES).contains(table.toLowerCase())) {
        return table;
      }
    }
    return null;
  }
}
