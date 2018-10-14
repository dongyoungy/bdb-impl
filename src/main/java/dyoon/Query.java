package dyoon;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/** Created by Dong Young Yoon on 10/9/18. */
public class Query implements Serializable {
  private static final long serialVersionUID = 5426329759219434198L;
  private String id;
  private String joinTableName;
  private String query = "";
  private TreeSet<String> queryColumnSet;
  private TreeSet<String> joinedTables;
  private HashSet<Pair<String, String>> joinColumns;

  public static final String[] FACT_TABLES = {
    "store_sales",
    "store_returns",
    "catalog_sales",
    "catalog_returns",
    "web_sales",
    "web_returns",
    "inventory"
  };

  public Query(
      String id,
      List<String> queryColumns,
      List<String> joinedTables,
      List<Pair<String, String>> joinColumns) {
    this.id = id;
    this.queryColumnSet = new TreeSet<>(queryColumns);
    this.joinedTables = new TreeSet<>(joinedTables);
    this.joinColumns = new HashSet<>(joinColumns);
    this.joinTableName = Joiner.on("_").join(this.joinedTables);
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

  public String getId() {
    return id;
  }

  public String getUniqueName() {
    return Joiner.on("_").join(joinedTables) + "__" + getQCSString();
  }

  public String getFactTable() {
    for (String table : joinedTables) {
      if (Arrays.asList(FACT_TABLES).contains(table.toLowerCase())) {
        return table;
      }
    }
    return null;
  }

  public static String getFactTable(List<String> tables) {
    for (String table : tables) {
      if (Arrays.asList(FACT_TABLES).contains(table.toLowerCase())) {
        return table;
      }
    }
    return null;
  }
}
