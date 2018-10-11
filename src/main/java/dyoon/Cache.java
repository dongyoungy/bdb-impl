package dyoon;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Dong Young Yoon on 3/20/18.
 *
 * <p>A serializable, singleton cache class for store various statistics that could take a long time
 * to obtain into a file.
 */
public class Cache {

  public static final String TABLE_SIZE_SUFFIX = ".tablesize";
  public static final String TABLE_STAT_SUFFIX = ".tablestat";
  public static final String GROUP_COUNT_SUFFIX = ".groupcount";
  public static final String AVG_GROUP_SIZE_SUFFIX = ".avggroupsize";
  public static final String MAX_GROUP_SIZE_SUFFIX = ".maxgroupsize";
  public static final String MIN_GROUP_SIZE_SUFFIX = ".mingroupsize";

  private static final String CACHE_FILE_PATH = "." + File.separator + "cache";

  private static Cache singleInstance = null;
  private Map<String, Object> cache;

  private File cacheFile;

  private Cache() {
    cache = new HashMap<>();
    cacheFile = new File(CACHE_FILE_PATH);
    if (cacheFile.exists()) {
      if (cacheFile.isDirectory()) {
        System.err.println(
            String.format("A cache %s cannot be accessed: it is a directory.", CACHE_FILE_PATH));
      } else {
        if (!initialize()) {
          System.err.println("Failed to initialize cache");
        }
      }
    }
  }

  public static Cache getInstance() {
    if (singleInstance == null) {
      singleInstance = new Cache();
    }
    return singleInstance;
  }

  public static void clearCache() {
    if (singleInstance == null) {
      singleInstance = new Cache();
    }
    singleInstance.removeCache();
  }

  private void removeCache() {
    if (cacheFile.exists()) {
      cacheFile.delete();
    }
    cache = new HashMap<>();
  }

  private boolean initialize() {
    ObjectInputStream ois = null;
    try {
      FileInputStream in = new FileInputStream(cacheFile);
      ois = new ObjectInputStream(in);
      cache = (Map<String, Object>) ois.readObject();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return false;
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      return false;
    } finally {
      if (ois != null) {
        try {
          ois.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return true;
  }

  private void updateCacheFile() {
    ObjectOutputStream oos = null;
    try {
      FileOutputStream out = new FileOutputStream(cacheFile);
      oos = new ObjectOutputStream(out);
      oos.writeObject(cache);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (oos != null) {
        try {
          oos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public Object get(String key) {
    return cache.get(key.toLowerCase());
  }

  public void put(String key, Object value) {
    cache.put(key.toLowerCase(), value);
    this.updateCacheFile();
  }

  private void remove(String key) {
    cache.remove(key);
    this.updateCacheFile();
  }

  public Long getGroupCount(Query q) {
    String key = String.format("%s_%s_%s", q.getFactTable(), q.getQCSString(), GROUP_COUNT_SUFFIX);
    return (Long) this.get(key);
  }

  public void setGroupCount(Query q, Long value) {
    String key = String.format("%s_%s_%s", q.getFactTable(), q.getQCSString(), GROUP_COUNT_SUFFIX);
    this.put(key, value);
  }

  public Double getAverageGroupSize(Query q) {
    String key =
        String.format("%s_%s_%s", q.getFactTable(), q.getQCSString(), AVG_GROUP_SIZE_SUFFIX);
    return (Double) this.get(key);
  }

  public void setAverageGroupSize(Query q, Double value) {
    String key =
        String.format("%s_%s_%s", q.getFactTable(), q.getQCSString(), AVG_GROUP_SIZE_SUFFIX);
    this.put(key, value);
  }

  public Long getMaxGroupSize(Query q) {
    String key =
        String.format("%s_%s_%s", q.getFactTable(), q.getQCSString(), MAX_GROUP_SIZE_SUFFIX);
    return (Long) this.get(key);
  }

  public void setMaxGroupSize(Query q, Long value) {
    String key =
        String.format("%s_%s_%s", q.getFactTable(), q.getQCSString(), MAX_GROUP_SIZE_SUFFIX);
    this.put(key, value);
  }

  public Long getMinGroupSize(Query q) {
    String key =
        String.format("%s_%s_%s", q.getFactTable(), q.getQCSString(), MIN_GROUP_SIZE_SUFFIX);
    return (Long) this.get(key);
  }

  public void setMinGroupSize(Query q, Long value) {
    String key =
        String.format("%s_%s_%s", q.getFactTable(), q.getQCSString(), MIN_GROUP_SIZE_SUFFIX);
    this.put(key, value);
  }

  public void saveStat(String database, String table, Stat stat) {
    this.put(database + "_" + table + TABLE_STAT_SUFFIX, stat);
    this.put(stat.getId() + TABLE_STAT_SUFFIX, stat);
  }

  public Stat loadStat(String database, String table) {
    return (Stat) this.get(database + "_" + table + TABLE_STAT_SUFFIX);
  }

  public Stat loadStat(String id) {
    return (Stat) this.get(id + TABLE_STAT_SUFFIX);
  }

  public void addPrejoin(Prejoin p) {
    this.put(p.getName(), p);
  }

  public void removePrejoin(Prejoin p) {
    this.remove(p.getName());
  }

  public List<Prejoin> getPrejoins(String database) {
    List<Prejoin> list = new ArrayList<>();
    for (String key : cache.keySet()) {
      if (key.startsWith(Prejoin.PREJOIN_PREFIX + database)) {
        list.add((Prejoin) this.get(key));
      }
    }
    return list;
  }
}
