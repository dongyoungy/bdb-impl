package dyoon;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Joiner;

import java.io.Serializable;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/** Created by Dong Young Yoon on 10/14/18. */
public class Sample implements Serializable {
  private static final long serialVersionUID = -5549092531451045540L;

  public enum Type {
    UNIFORM,
    STRATIFIED
  }

  private Query query;
  private String table;
  private TreeSet<String> joinTables;
  private TreeSet<String> columns;
  private Type type;

  private double ratio; // used by uniform

  private double Z;
  private double E;

  public Sample() {}

  public Sample(Query query, Type type, String table, Set<String> joinTables, Set<String> columns) {
    this.query = query;
    this.type = type;
    this.table = table;
    this.joinTables = new TreeSet<>(joinTables);
    this.columns = new TreeSet<>(columns);
  }

  @Override
  public String toString() {
    String t = "";
    if (type == Type.UNIFORM) {
      t = String.format("uf_%.2f", ratio).replaceAll("\\.", "_");
    } else if (type == Type.STRATIFIED) {
      t = String.format("st_%.4f_%.4f", Z, E).replaceAll("\\.", "_");
    } else {
      t = "unknown";
    }
    return query.getFactTable() + "__" + t + "__" + getColumnString();
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
    return Z;
  }

  public void setZ(double z) {
    Z = z;
  }

  public double getE() {
    return E;
  }

  public void setE(double e) {
    E = e;
  }
}
