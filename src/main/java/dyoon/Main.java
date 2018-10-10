package dyoon;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Created by Dong Young Yoon on 10/9/18. */
public class Main {

  private static List<Query> queries = new ArrayList<>();

  private static final String HOST = "c220g1-030622.wisc.cloudlab.us";
  private static final int PORT = 21050;
  private static final String DATABASE = "tpcds_5000_parquet";

  private static final long UNIFORM_THRESHOLD = 100000;
  private static final double MIN_IO_REDUCTION_RATIO = (2.0/3.0);
  private static final double Z = 2.576; // 99% CI
  private static final double E = 0.01; // 1% error

  public static void main(String[] args) {
    Connection conn = null;
    setQueries();

    try {
      Class.forName("com.cloudera.impala.jdbc41.Driver");
      String connectionStr = String.format("jdbc:impala://%s:%d/%s", HOST, PORT, DATABASE);
      conn = DriverManager.getConnection(connectionStr, "", "");
      DatabaseTool tool = new DatabaseTool(conn);

      for (Query q : queries) {
        tool.findOrCreateJoinTable(q);
        Pair<Long, Double> groupCountAndSize = tool.getGroupCountAndSize(q);
        long groupCount = groupCountAndSize.getLeft();
        double avgGroupSize = groupCountAndSize.getRight();
        double sampleSize = getSampleSize(avgGroupSize, Z, E);
        System.out.println(
            String.format(
                "For query %d (group count = %d, avg group size = %f, target sample size = %f:",
                q.getId(), groupCount, avgGroupSize, sampleSize));
        System.out.print("\t");
        if (avgGroupSize > UNIFORM_THRESHOLD) {
          System.out.println(
              String.format(
                  "Create %f%% uniform sample on %s.",
                  UNIFORM_THRESHOLD / avgGroupSize, q.getFactTable()));
        } else {
          if ((sampleSize / avgGroupSize) <= MIN_IO_REDUCTION_RATIO) {
            System.out.println(
                String.format(
                    "Create stratified sample on %s with (%s) and %d min rows.",
                    q.getFactTable(), q.getQCSString(), (long) Math.ceil(sampleSize)));
          } else {
            System.out.println("No viable samples.");
          }
        }
      }

    } catch (SQLException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  private static double getSampleSize(double avgGroupSize, double z, double e) {
    double s0 = (Math.pow(z, 2) * 0.5 * 0.5) / Math.pow(e, 2);
    return (avgGroupSize * s0) / (avgGroupSize + s0 - 1);
  }

  private static void setQueries() {
    List<Pair<String, String>> q1JoinCols = new ArrayList<>();
    q1JoinCols.add(ImmutablePair.of("d_date_sk", "sr_returned_date_sk"));
    Query q1 =
        new Query(
            1,
            Arrays.asList("d_year", "sr_customer_sk", "sr_store_sk"),
            Arrays.asList("date_dim", "store_returns"),
            q1JoinCols);
    queries.add(q1);

    List<Pair<String, String>> q3JoinCols = new ArrayList<>();
    q3JoinCols.add(ImmutablePair.of("d_date_sk", "ss_sold_date_sk"));
    q3JoinCols.add(ImmutablePair.of("i_item_sk", "ss_item_sk"));
    Query q3 =
        new Query(
            3,
            Arrays.asList("d_moy", "d_year", "i_manufact_id", "i_brand", "i_brand_id"),
            Arrays.asList("date_dim", "item", "store_sales"),
            q3JoinCols);
    queries.add(q3);

    List<Pair<String, String>> q6JoinCols = new ArrayList<>();
    q6JoinCols.add(ImmutablePair.of("ca_address_sk", "c_current_addr_sk"));
    q6JoinCols.add(ImmutablePair.of("c_customer_sk", "ss_customer_sk"));
    q6JoinCols.add(ImmutablePair.of("d_date_sk", "ss_sold_date_sk"));
    q6JoinCols.add(ImmutablePair.of("i_item_sk", "ss_item_sk"));
    Query q6 =
        new Query(
            6,
            Arrays.asList("d_month_seq", "i_current_price", "ca_state"),
            Arrays.asList("customer_address", "customer", "date_dim", "item", "store_sales"),
            q6JoinCols);
    queries.add(q6);

    List<Pair<String, String>> q7JoinCols = new ArrayList<>();
    q7JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q7JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    q7JoinCols.add(ImmutablePair.of("ss_cdemo_sk", "cd_demo_sk"));
    q7JoinCols.add(ImmutablePair.of("ss_promo_sk", "p_promo_sk"));
    Query q7 =
        new Query(
            7,
            Arrays.asList(
                "cd_gender",
                "cd_marital_status",
                "cd_education_status",
                "p_channel_email",
                "p_channel_event",
                "d_year",
                "i_item_id"),
            Arrays.asList("store_sales", "customer_demographics", "date_dim", "item", "promotion"),
            q7JoinCols);
    queries.add(q7);

    List<Pair<String, String>> q13JoinCols = new ArrayList<>();
    q13JoinCols.add(ImmutablePair.of("s_store_sk", "ss_store_sk"));
    q13JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q13JoinCols.add(ImmutablePair.of("ss_hdemo_sk", "hd_demo_sk"));
    q13JoinCols.add(ImmutablePair.of("ss_cdemo_sk", "cd_demo_sk"));
    Query q13 =
        new Query(
            13,
            Arrays.asList(
                "d_year",
                "cd_marital_status",
                "cd_education_status",
                "ss_sales_price",
                "hd_dep_count"),
            Arrays.asList(
                "store_sales",
                "store",
                "customer_demographics",
                "household_demographics",
                "date_dim"),
            q13JoinCols);
    queries.add(q13);

    List<Pair<String, String>> q15JoinCols = new ArrayList<>();
    q15JoinCols.add(ImmutablePair.of("cs_bill_customer_sk", "c_customer_sk"));
    q15JoinCols.add(ImmutablePair.of("c_current_addr_sk", "ca_address_sk"));
    q15JoinCols.add(ImmutablePair.of("cs_sold_date_sk", "d_date_sk"));
    String q15Tables = "catalog_sales,customer,customer_address,date_dim";
    Query q15 =
        new Query(
            15,
            Arrays.asList("ca_zip", "ca_state", "cs_sales_price", "d_qoy", "d_year", "ca_zip"),
            Arrays.asList(q15Tables.split(",")),
            q15JoinCols);
    queries.add(q15);
  }
}
