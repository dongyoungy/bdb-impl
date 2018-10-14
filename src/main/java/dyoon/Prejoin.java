package dyoon;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/** Created by Dong Young Yoon on 10/11/18. */
public class Prejoin implements Serializable {

  public static final String PREJOIN_PREFIX = "prejoin_";
  private static final long serialVersionUID = 1879156031101377881L;

  private String name;
  private String database;
  private String factTableName;
  private TreeSet<String> tableSet;
  private TreeSet<ColumnPair> joinColumnSet;

  public Prejoin() {
    this.tableSet = new TreeSet<>();
    this.joinColumnSet = new TreeSet<>();
  }

  public Prejoin(
      String name,
      String database,
      String factTableName,
      List<String> tableList,
      List<ColumnPair> joinColumnList) {
    this.name = name;
    this.database = database;
    this.factTableName = factTableName;
    this.tableSet = new TreeSet<>(tableList);
    this.joinColumnSet = new TreeSet<>(joinColumnList);
  }

  public Prejoin(String database, String factTableName) {
    this.database = database;
    this.name = PREJOIN_PREFIX + database + "_" + RandomStringUtils.randomAlphanumeric(8);
    this.factTableName = factTableName;
    this.tableSet = new TreeSet<>();
    this.joinColumnSet = new TreeSet<>();
  }

  public String toJSONString() {
    ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
    try {
      return ow.writeValueAsString(this).replaceAll("\\n", "");
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return null;
  }

  public String getName() {
    return name;
  }

  public String getDatabase() {
    return database;
  }

  public String getFactTableName() {
    return factTableName;
  }

  public void setFactTableName(String factTableName) {
    this.factTableName = factTableName;
  }

  public void addTable(String table) {
    tableSet.add(table);
  }

  public void addJoinColumnPair(String left, String right) {
    ColumnPair joinColumnPair;
    if (left.compareTo(right) < 0) {
      joinColumnPair = new ColumnPair(left, right);
    } else {
      joinColumnPair = new ColumnPair(right, left);
    }
    this.joinColumnSet.add(joinColumnPair);
  }

  public SortedSet<String> getTableSet() {
    return tableSet;
  }

  public SortedSet<ColumnPair> getJoinColumnSet() {
    return joinColumnSet;
  }

  public boolean contains(Prejoin p) {
    if (!this.database.equals(p.getDatabase())) {
      return false;
    }
    if (!this.factTableName.equals(p.getFactTableName())) {
      return false;
    }
    if (!this.tableSet.containsAll(p.getTableSet())) {
      return false;
    }
    return this.joinColumnSet.containsAll(p.getJoinColumnSet());
  }

  public boolean supports(String database, Query q) {
    if (!this.database.equals(database)) {
      return false;
    }
    if (!this.factTableName.equals(q.getFactTable())) {
      return false;
    }
    if (!this.getTableSet().containsAll(q.getJoinedTables())) {
      return false;
    }

    SortedSet<ColumnPair> queryJoinColumnPairs = new TreeSet<>();
    for (ColumnPair pair : q.getJoinColumns()) {
      ColumnPair joinColumnPair;
      String left = pair.getLeft();
      String right = pair.getRight();
      if (left.compareTo(right) < 0) {
        joinColumnPair = new ColumnPair(left, right);
      } else {
        joinColumnPair = new ColumnPair(right, left);
      }
      queryJoinColumnPairs.add(joinColumnPair);
    }
    return this.joinColumnSet.containsAll(queryJoinColumnPairs);
  }
}
