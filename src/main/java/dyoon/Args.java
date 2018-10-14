package dyoon;

import com.beust.jcommander.Parameter;

/** Created by Dong Young Yoon on 10/14/18. */
public class Args {

  @Parameter(
      names = {"-d", "--database"},
      description = "database/schema")
  private String database = "tpcds_500_parquet";

  @Parameter(
      names = {"-h", "--host"},
      description = "host")
  private String host = "c220g5-110408.wisc.cloudlab.us:21050";

  @Parameter(names = "--create", description = "Create samples at the end")
  private boolean create = false;

  @Parameter(names = "--prejoin", description = "Use prejoins")
  private boolean prejoin = false;

  @Parameter(names = "--overwrite", description = "overwrite samples")
  private boolean overwrite = false;

  @Parameter(names = "--load-prejoin-file", description = "Load prejoin meta from file")
  private String loadPrejoinFile = "";

  @Parameter(names = "--help", help = true)
  private boolean help = false;

  public String getDatabase() {
    return database;
  }

  public String getHost() {
    return host;
  }

  public boolean isPrejoin() {
    return prejoin;
  }

  public boolean isCreate() {
    return create;
  }

  public boolean isHelp() {
    return help;
  }

  public String getLoadPrejoinFile() {
    return loadPrejoinFile;
  }

  public boolean isOverwrite() {
    return overwrite;
  }
}
