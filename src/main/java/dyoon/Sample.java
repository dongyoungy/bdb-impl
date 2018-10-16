package dyoon;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Joiner;

import java.io.Serializable;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/** Created by Dong Young Yoon on 10/14/18. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Sample implements Serializable {
  private static final long serialVersionUID = -5549092531451045540L;

  public enum Type {
    UNIFORM,
    STRATIFIED,
    STRATIFIED2
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Query query;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private TreeSet<Query> queryList;

  private String table;
  private TreeSet<String> joinTables;
  private TreeSet<String> columns;
  private Type type;

  private double ratio; // used by uniform

  private double Z = 2.576;
  private double E = 0.01; // used by stratified

  private int minRow; // used by stratified2

  public Sample() {
    this.queryList = new TreeSet<>();
    this.joinTables = new TreeSet<>();
    this.columns = new TreeSet<>();
  }

  public Sample(Type type, String table, Set<String> joinTables, Set<String> columns) {
    this.query = null;
    this.type = type;
    this.table = table;
    this.joinTables = new TreeSet<>(joinTables);
    this.columns = new TreeSet<>(columns);
    this.queryList = new TreeSet<>();
  }

  public Sample(Query query, Type type, String table, Set<String> joinTables, Set<String> columns) {
    this.query = query;
    this.type = type;
    this.table = table;
    this.joinTables = new TreeSet<>(joinTables);
    this.columns = new TreeSet<>(columns);
    this.queryList = new TreeSet<>();
  }

  @Override
  public String toString() {
    String t = "";
    if (type == Type.UNIFORM) {
      t = String.format("uf_%.4f", ratio).replaceAll("\\.", "_");
      return table + "__" + t;
    } else if (type == Type.STRATIFIED) {
      t = String.format("st_%.4f_%.4f", Z, E).replaceAll("\\.", "_");
      return table + "__" + t + "__" + getColumnString();
    } else if (type == Type.STRATIFIED2) {
      t = String.format("st2_%d", minRow).replaceAll("\\.", "_");
      return table + "__" + t + "__" + getColumnString();
    } else {
      t = "unknown";
    }
    return table + "__" + t + "__" + getColumnString();
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

  public Query getQuery() {
    return query;
  }

  public String getTable() {
    return table;
  }

  public SortedSet<String> getJoinTables() {
    return joinTables;
  }

  public SortedSet<String> getColumns() {
    return columns;
  }

  public String getColumnString() {
    return Joiner.on("_").join(columns);
  }

  public void setQuery(Query query) {
    this.query = query;
    this.queryList.add(query);
  }

  public void addQuery(Query query) {
    this.queryList.add(query);
  }

  public SortedSet<Query> getQueryList() {
    if (query != null) queryList.add(query);
    return queryList;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public double getRatio() {
    return ratio;
  }

  public void setRatio(double ratio) {
    this.ratio = ratio;
  }

  public double getZ() {
    if (Z == 0) return 2.576;
    else return Z;
  }

  public void setZ(double z) {
    Z = z;
  }

  public double getE() {
    if (E == 0) return 0.01;
    else return E;
  }

  public void setE(double e) {
    E = e;
  }

  public int getMinRow() {
    return minRow;
  }

  public void setMinRow(int minRow) {
    this.minRow = minRow;
  }
}
