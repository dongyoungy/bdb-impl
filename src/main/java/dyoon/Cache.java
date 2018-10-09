package dyoon;

import java.io.*;
import java.util.*;

/**
 * Created by Dong Young Yoon on 3/20/18.
 * <p>
 * A serializable, singleton cache class for store various statistics
 * that could take a long time to obtain into a file.
 */
public class Cache {

  public final static String TABLE_SIZE_SUFFIX = ".tablesize";
  public final static String GROUP_COUNT_SUFFIX = ".groupcount";
  public final static String AVG_GROUP_SIZE_SUFFIX = ".avggroupsize";

  private final static String CACHE_FILE_PATH = "." + File.separator + "cache";

  private static Cache singleInstance = null;
  private Map<String, Object> cache;

  private File cacheFile;

  private Cache() {
    cache = new HashMap<>();
    cacheFile = new File(CACHE_FILE_PATH);
    if (cacheFile.exists()) {
      if (cacheFile.isDirectory()) {
        System.err.println(
            String.format("A cache %s cannot be accessed: it is a directory.",
                CACHE_FILE_PATH));
      }
      else {
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
    String key = String.format("%s_%s", q.getQCSString(), GROUP_COUNT_SUFFIX);
    return (Long) this.get(key);
  }

  public void setGroupCount(Query q, Long value) {
    String key = String.format("%s_%s", q.getQCSString(), GROUP_COUNT_SUFFIX);
    this.put(key, value);
  }

  public Double getAverageGroupSize(Query q) {
    String key = String.format("%s_%s", q.getQCSString(), AVG_GROUP_SIZE_SUFFIX);
    return (Double) this.get(key);
  }

  public void setAverageGroupSize(Query q, Double value) {
    String key = String.format("%s_%s", q.getQCSString(), AVG_GROUP_SIZE_SUFFIX);
    this.put(key, value);
  }
}

