package dyoon;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Dong Young Yoon on 3/20/18.
 *
 * <p>A serializable, singleton cache class for store various statistics that could take a long time
 * to obtain into a file.
 */
public class Meta {

  public static final String TABLE_STAT_SUFFIX = ".tablestat";
  public static final String SAMPLE_SUFFIX = ".sample";

  private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
  private static final String META_NAME = "mymeta";

  private static Meta singleInstance = null;

  private File cacheFile;
  private Connection conn;

  private Meta(Connection conn) {
    this.conn = conn;
    if (!initialize()) {
      System.err.println("Failed to initialize meta");
    }
  }

  public static Meta getInstance(Connection conn) {
    if (singleInstance == null) {
      singleInstance = new Meta(conn);
    }
    return singleInstance;
  }

  private boolean initialize() {
    try {
      conn.createStatement()
          .execute(
              String.format(
                  "CREATE TABLE IF NOT EXISTS %s "
                      + "(type string, key string, "
                      + "value string, "
                      + "ts timestamp)",
                  META_NAME));
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public void saveStat(String database, String table, Stat stat) {
    String type = "stat";
    String key = database + "_" + table + TABLE_STAT_SUFFIX;
    String value = stat.toJSONString();
    String ts = new SimpleDateFormat(TIMESTAMP_FORMAT).format(new Date());
    String sql =
        String.format(
            "INSERT INTO %s VALUES ('%s', '%s', '%s', '%s')", META_NAME, type, key, value, ts);
    try {
      conn.createStatement().execute(sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public Stat loadStat(String database, String table) {
    String key = database + "_" + table + TABLE_STAT_SUFFIX;
    String sql =
        String.format(
            "SELECT value FROM %s WHERE type = 'stat' AND key = '%s' AND value != 'DELETED' "
                + "ORDER BY ts DESC",
            META_NAME, key);
    try {
      ResultSet rs = conn.createStatement().executeQuery(sql);
      if (rs.next()) {
        String json = rs.getString("value");
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json, Stat.class);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (JsonParseException e) {
      e.printStackTrace();
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public Stat loadStat(String id) {
    String key = id + TABLE_STAT_SUFFIX;
    String sql =
        String.format(
            "SELECT value FROM %s WHERE type = 'stat' AND key = '%s' AND value != 'DELETED' "
                + "ORDER BY ts DESC",
            META_NAME, key);
    try {
      ResultSet rs = conn.createStatement().executeQuery(sql);
      if (rs.next()) {
        String json = rs.getString("value");
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json, Stat.class);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (JsonParseException e) {
      e.printStackTrace();
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public void addPrejoin(Prejoin p) {
    String type = "prejoin";
    String key = p.getName();
    String value = p.toJSONString();
    String ts = new SimpleDateFormat(TIMESTAMP_FORMAT).format(new Date());
    try {
      conn.createStatement()
          .execute(
              String.format(
                  "INSERT INTO %s VALUES ('%s', '%s', '%s', '%s')",
                  META_NAME, type, key, value, ts));
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void removePrejoin(Prejoin p) {
    //    this.remove(p.getName());
  }

  public List<Prejoin> getPrejoins(String database) {
    List<Prejoin> list = new ArrayList<>();
    String sql =
        String.format(
            "SELECT key, value FROM %s WHERE type = 'prejoin' AND value != 'DELETED' "
                + "ORDER BY ts DESC",
            META_NAME);
    try {
      ResultSet rs = conn.createStatement().executeQuery(sql);
      while (rs.next()) {
        String key = rs.getString("key");
        String json = rs.getString("value");
        if (key.startsWith(Prejoin.PREJOIN_PREFIX + database)) {
          ObjectMapper mapper = new ObjectMapper();
          list.add(mapper.readValue(json, Prejoin.class));
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (JsonParseException e) {
      e.printStackTrace();
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return list;
  }

  public Prejoin getPrejoinForSample(String database, Sample s) {
    String sql =
        String.format(
            "SELECT value FROM %s WHERE type = 'prejoin' AND value != 'DELETED' "
                + "ORDER BY ts DESC",
            META_NAME);
    try {
      ResultSet rs = conn.createStatement().executeQuery(sql);
      if (rs.next()) {
        String json = rs.getString("value");
        ObjectMapper mapper = new ObjectMapper();
        Prejoin p = mapper.readValue(json, Prejoin.class);
        if (p.supports(database, s.getQuery())) {
          return p;
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (JsonParseException e) {
      e.printStackTrace();
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }

  public void addSample(Sample s) {
    String type = "sample";
    String key = s.toString() + SAMPLE_SUFFIX;
    String value = s.toJSONString();
    String ts = new SimpleDateFormat(TIMESTAMP_FORMAT).format(new Date());
    try {
      conn.createStatement()
          .execute(
              String.format(
                  "INSERT INTO %s VALUES ('%s', '%s', '%s', '%s')",
                  META_NAME, type, key, value, ts));
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
