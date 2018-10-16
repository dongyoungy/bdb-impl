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

  @Parameter(names = "--test-all-samples", description = "test/evaluate samples")
  private boolean testAllSamples = false;

  @Parameter(names = "--load-prejoin-file", description = "Load prejoin meta from file")
  private String loadPrejoinFile = "";

  @Parameter(names = "--clear-cache-script", description = "Location of cache clear script")
  private String clearCacheScript = "";

  @Parameter(names = "--measure-time", description = "measure time when test/evaluating samples")
  private boolean measureTime = false;

  @Parameter(names = "--help", help = true)
  private boolean help = false;

  @Parameter(names = "--create-sample", description = "create a sample")
  private String createSample = "";

  @Parameter(names = "--test-sample", description = "test a sample")
  private String testSample = "";

  @Parameter(names = "--test-queries", description = "ids of queries to test (comma-separated)")
  private String testQueries = "";

  @Parameter(
      names = "--sample-tables",
      description = "tables for sample being created (comma-separated)")
  private String sampleTables = "";

  @Parameter(
      names = "--sample-columns",
      description = "columns for sample being created (comma-separated)")
  private String sampleColumns = "";

  @Parameter(names = "--min-rows", description = "minimum rows for stratified2")
  private int minRows = 0;

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

  public String getCreateSample() {
    return createSample;
  }

  public String getSampleTables() {
    return sampleTables;
  }

  public int getMinRows() {
    return minRows;
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  public boolean isTestAllSamples() {
    return testAllSamples;
  }

  public String getClearCacheScript() {
    return clearCacheScript;
  }

  public boolean isMeasureTime() {
    return measureTime;
  }

  public String getSampleColumns() {
    return sampleColumns;
  }

  public String getTestSample() {
    return testSample;
  }

  public String getTestQueries() {
    return testQueries;
  }
}
