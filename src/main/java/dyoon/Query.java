package dyoon;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/** Created by Dong Young Yoon on 10/9/18. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Query implements Serializable, Comparable<Query> {
  private static final long serialVersionUID = 5426329759219434198L;
  private String id;
  private String joinTableName;
  private String query = "";
  private String sampleQuery = "";
  private TreeSet<String> queryColumnSet;
  private TreeSet<String> joinedTables;
  private HashSet<ColumnPair> joinColumns;
  private TreeSet<String> groupByColumns;
  private TreeSet<String> aggColumns;

  public static final String[] FACT_TABLES = {
    "store_sales",
    "store_returns",
    "catalog_sales",
    "catalog_returns",
    "web_sales",
    "web_returns",
    "inventory"
  };

  public Query() {}

  public Query(String id, String query) {
    this.id = id;
    this.query = query;
  }

  public Query(
      String id,
      List<String> queryColumns,
      List<String> joinedTables,
      List<Pair<String, String>> joinColumns) {
    this.id = id;
    this.queryColumnSet = new TreeSet<>(queryColumns);
    this.joinedTables = new TreeSet<>(joinedTables);
    List<ColumnPair> newList = new ArrayList<>();
    for (Pair<String, String> pair : joinColumns) {
      newList.add(new ColumnPair(pair.getLeft(), pair.getRight()));
    }

    this.joinColumns = new HashSet<>(newList);
    this.joinTableName = Joiner.on("_").join(this.joinedTables);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    Query other = (Query) obj;
    return id.equals(other.getId());
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

  public Set<ColumnPair> getJoinColumns() {
    return joinColumns;
  }

  public String getQuery() {
    return query;
  }

  public String getId() {
    return id;
  }

  public String getUniqueName() {
    return Joiner.on("_").join(joinedTables) + "__" + getQCSString();
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public void setGroupByColumns(List<String> groupByColumns) {
    this.groupByColumns = new TreeSet<>(groupByColumns);
  }

  public void setAggColumns(List<String> aggColumns) {
    this.aggColumns = new TreeSet<>(aggColumns);
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

  public TreeSet<String> getGroupByColumns() {
    return groupByColumns;
  }

  public TreeSet<String> getAggColumns() {
    return aggColumns;
  }

  public String getSampleQuery() {
    return sampleQuery;
  }

  public void setSampleQuery(String sampleQuery) {
    this.sampleQuery = sampleQuery;
  }

  @Override
  public int compareTo(Query o) {
    return this.getId().compareTo(o.getId());
  }
}
