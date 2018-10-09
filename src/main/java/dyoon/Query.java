package dyoon;

import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/** Created by Dong Young Yoon on 10/9/18. */
public class Query {
  private int id;
  private String query = "";
  private Set<String> queryColumnSet;
  private SortedSet<String> joinedTables;
  private Set<Pair<String, String>> joinColumns;

  public Query(int id, Set<String> queryColumnSet) {
    this.id = id;
    this.queryColumnSet = queryColumnSet;
  }

  public Query(int id, List<String> queryColumns) {
    this.id = id;
    this.queryColumnSet = new HashSet<>(queryColumns);
  }

  public Query(
      int id,
      List<String> queryColumns,
      List<String> joinedTables,
      List<Pair<String, String>> joinColumns) {
    this.id = id;
    this.queryColumnSet = new HashSet<>(queryColumns);
    this.joinedTables = new TreeSet<>(joinedTables);
    this.joinColumns = new HashSet<>(joinColumns);
  }

  public ResultSet runQuery(Connection conn) {
    return null;
  }

  public int getId() {
    return id;
  }
}
