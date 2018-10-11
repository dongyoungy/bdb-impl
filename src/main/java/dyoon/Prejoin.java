package dyoon;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.SortedSet;
import java.util.TreeSet;

/** Created by Dong Young Yoon on 10/11/18. */
public class Prejoin implements Serializable {

  public static final String PREJOIN_PREFIX = "prejoin_";

  private String name;
  private String database;
  private String factTableName;
  private SortedSet<String> tableSet;
  private SortedSet<Pair<String, String>> joinColumnSet;

  public Prejoin(String database, String factTableName) {
    this.database = database;
    this.name = PREJOIN_PREFIX + database + "_" + RandomStringUtils.randomAlphanumeric(8);
    this.factTableName = factTableName;
    this.tableSet = new TreeSet<>();
    this.joinColumnSet = new TreeSet<>();
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
    Pair<String, String> joinColumnPair;
    if (left.compareTo(right) < 0) {
      joinColumnPair = ImmutablePair.of(left, right);
    } else {
      joinColumnPair = ImmutablePair.of(right, left);
    }
    this.joinColumnSet.add(joinColumnPair);
  }

  public SortedSet<String> getTableSet() {
    return tableSet;
  }

  public void setTableSet(SortedSet<String> tableSet) {
    this.tableSet = tableSet;
  }

  public SortedSet<Pair<String, String>> getJoinColumnSet() {
    return joinColumnSet;
  }

  public void setJoinColumnSet(SortedSet<Pair<String, String>> joinColumnSet) {
    this.joinColumnSet = joinColumnSet;
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

    SortedSet<Pair<String, String>> queryJoinColumnPairs = new TreeSet<>();
    for (Pair<String, String> pair : q.getJoinColumns()) {
      Pair<String, String> joinColumnPair;
      String left = pair.getLeft();
      String right = pair.getRight();
      if (left.compareTo(right) < 0) {
        joinColumnPair = ImmutablePair.of(left, right);
      } else {
        joinColumnPair = ImmutablePair.of(right, left);
      }
      queryJoinColumnPairs.add(joinColumnPair);
    }
    return this.joinColumnSet.containsAll(queryJoinColumnPairs);
  }
}
