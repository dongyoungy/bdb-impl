package dyoon;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/** Created by Dong Young Yoon on 10/9/18. */
public class Main {

  private static final String DATABASE = "tpcds_500_parquet";

  private static final String HOST = "c220g5-110408.wisc.cloudlab.us";
  private static final int PORT = 21050;

  private static final long UNIFORM_THRESHOLD = 100000;
  private static final double MIN_IO_REDUCTION_RATIO = (2.0 / 3.0);
  private static final double Z = 2.576; // 99% CI
  private static final double E = 0.01; // 1% error
  private static List<Query> queries = new ArrayList<>();

  public static void main(String[] args) {
    Connection conn = null;
    //    setQueriesWithoutPrejoin();
    setQueries();

    try {
      Class.forName("com.cloudera.impala.jdbc41.Driver");
      String connectionStr = String.format("jdbc:impala://%s:%d/%s", HOST, PORT, DATABASE);
      conn = DriverManager.getConnection(connectionStr, "", "");
      DatabaseTool tool = new DatabaseTool(conn);

      Cache cache = Cache.getInstance();
      List<Prejoin> prejoinList = cache.getPrejoins(DATABASE);
      PriorityQueue<Prejoin> prejoinQueue = new PriorityQueue<>(100, new PrejoinSizeComparator());

      for (Prejoin prejoin : prejoinList) {
        if (!tool.checkTableExists(prejoin)) {
          cache.removePrejoin(prejoin);
        }
      }

      for (Query q : queries) {
        if (q.getJoinedTables().size() > 1) {
          Prejoin p = new Prejoin(DATABASE, q.getFactTable());
          for (String table : q.getJoinedTables()) {
            p.addTable(table);
          }
          for (Pair<String, String> pair : q.getJoinColumns()) {
            p.addJoinColumnPair(pair.getLeft(), pair.getRight());
          }
          prejoinQueue.add(p);
        }
      }

      Iterator<Prejoin> it = prejoinQueue.iterator();
      while (it.hasNext()) {
        Prejoin p = it.next();
        boolean exists = false;
        for (Prejoin availablePrejoin : prejoinList) {
          if (availablePrejoin.contains(p)) {
            exists = true;
            break;
          }
        }

        if (!exists) {
          tool.createPrejoinTable(p);
          cache.addPrejoin(p);
          prejoinList.add(p);
        }
      }

      for (Query q : queries) {
        Stat groupCountAndSize = tool.getGroupCountAndSize(DATABASE, q, prejoinList);
        if (groupCountAndSize == null) {
          System.out.println("Something wrong: stat null. Exiting.");
          System.exit(-1);
        }
        long populationSize = groupCountAndSize.getPopulationSize();
        double targetSampleSize = groupCountAndSize.getTargetSampleSize();
        long groupCount = groupCountAndSize.getGroupCount();
        double avgGroupSize = groupCountAndSize.getAvgGroupSize();
        long minGroupSize = groupCountAndSize.getMinGroupSize();
        long maxGroupSize = groupCountAndSize.getMaxGroupSize();
        //        double sampleSize = getSampleSize((double) maxGroupSize, Z, E);
        System.out.println(
            String.format(
                "For query %s (population = %d, target sample size = %.3f, "
                    + "group count = %d, avg group size = %.3f, min group size = %d, "
                    + "max group size = %d:",
                q.getId(),
                populationSize,
                targetSampleSize,
                groupCount,
                avgGroupSize,
                minGroupSize,
                maxGroupSize));
        System.out.print("\t");
        if (avgGroupSize > UNIFORM_THRESHOLD) {
          System.out.println(
              String.format(
                  "Create %f %% uniform sample on %s.",
                  (UNIFORM_THRESHOLD / avgGroupSize) * 100, q.getFactTable()));
        } else {
          double ratio = targetSampleSize / (double) populationSize;
          if (ratio <= MIN_IO_REDUCTION_RATIO) {
            System.out.println(
                String.format(
                    "Create stratified sample on %s with (%s) for estimated sample size of %.2f %%.",
                    q.getFactTable(), q.getQCSString(), ratio * 100));
          } else {
            System.out.println(String.format("No viable samples (ratio = %.2f %%).", ratio * 100));
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
            "1",
            Arrays.asList("d_year", "sr_customer_sk", "sr_store_sk"),
            Arrays.asList("date_dim", "store_returns"),
            q1JoinCols);
    queries.add(q1);

    List<Pair<String, String>> q3JoinCols = new ArrayList<>();
    q3JoinCols.add(ImmutablePair.of("d_date_sk", "ss_sold_date_sk"));
    q3JoinCols.add(ImmutablePair.of("i_item_sk", "ss_item_sk"));
    Query q3 =
        new Query(
            "3",
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
            "6",
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
            "7",
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

    List<Pair<String, String>> q11_1JoinCols = new ArrayList<>();
    q11_1JoinCols.add(ImmutablePair.of("c_customer_sk", "ss_customer_sk"));
    q11_1JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    String q11_1Tables = "customer,store_sales,date_dim";
    String q11_1QCS =
        "c_customer_id,c_first_name,c_last_name,c_preferred_cust_flag,c_birth_country,c_login,c_email_address,d_year";
    Query q11_1 =
        new Query(
            "11_1",
            Arrays.asList(q11_1QCS.split(",")),
            Arrays.asList(q11_1Tables.split(",")),
            q11_1JoinCols);
    queries.add(q11_1);

    List<Pair<String, String>> q11_2JoinCols = new ArrayList<>();
    q11_2JoinCols.add(ImmutablePair.of("c_customer_sk", "ws_bill_customer_sk"));
    q11_2JoinCols.add(ImmutablePair.of("ws_sold_date_sk", "d_date_sk"));
    String q11_2Tables = "customer,web_sales,date_dim";
    String q11_2QCS =
        "c_customer_id,c_first_name,c_last_name,c_preferred_cust_flag,c_birth_country,c_login,c_email_address,d_year";
    Query q11_2 =
        new Query(
            "11_2",
            Arrays.asList(q11_2QCS.split(",")),
            Arrays.asList(q11_2Tables.split(",")),
            q11_2JoinCols);
    queries.add(q11_2);

    List<Pair<String, String>> q12JoinCols = new ArrayList<>();
    q12JoinCols.add(ImmutablePair.of("ws_sold_date_sk", "d_date_sk"));
    q12JoinCols.add(ImmutablePair.of("ws_item_sk", "i_item_sk"));
    String q12Tables = "item,web_sales,date_dim";
    String q12QCS = "i_item_id,i_item_desc,i_category,i_class,i_current_price,d_date";
    Query q12 =
        new Query(
            "12",
            Arrays.asList(q12QCS.split(",")),
            Arrays.asList(q12Tables.split(",")),
            q12JoinCols);
    queries.add(q12);

    List<Pair<String, String>> q13JoinCols = new ArrayList<>();
    q13JoinCols.add(ImmutablePair.of("s_store_sk", "ss_store_sk"));
    q13JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q13JoinCols.add(ImmutablePair.of("ss_hdemo_sk", "hd_demo_sk"));
    q13JoinCols.add(ImmutablePair.of("ss_cdemo_sk", "cd_demo_sk"));
    Query q13 =
        new Query(
            "13",
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
            "15",
            Arrays.asList("ca_zip", "ca_state", "cs_sales_price", "d_qoy", "d_year", "ca_zip"),
            Arrays.asList(q15Tables.split(",")),
            q15JoinCols);
    queries.add(q15);

    List<Pair<String, String>> q16JoinCols = new ArrayList<>();
    q16JoinCols.add(ImmutablePair.of("cs_ship_date_sk", "d_date_sk"));
    q16JoinCols.add(ImmutablePair.of("cs_ship_addr_sk", "ca_address_sk"));
    q16JoinCols.add(ImmutablePair.of("cs_call_center_sk", "cc_call_center_sk"));
    String q16Tables = "catalog_sales,date_dim,customer_address,call_center";
    Query q16 =
        new Query(
            "16",
            Arrays.asList("d_date", "ca_state", "cc_county", "cs_warehouse_sk"),
            Arrays.asList(q16Tables.split(",")),
            q16JoinCols);
    queries.add(q16);

    //    List<Pair<String, String>> q19JoinCols = new ArrayList<>();
    //    q19JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    //    q19JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    //    q19JoinCols.add(ImmutablePair.of("ss_customer_sk", "c_customer_sk"));
    //    q19JoinCols.add(ImmutablePair.of("c_current_addr_sk", "ca_address_sk"));
    //    String q19Tables = "date_dim, store_sales, item,customer,customer_address,store";
    //    String q19QCS =
    //        "i_brand ,i_brand_id ,i_manufact_id ,i_manufact, i_manager_id, d_moy, d_year, ca_zip,
    // s_zip";
    //    Query q19 =
    //        new Query(
    //            "19",
    //            Arrays.asList(q19QCS.replaceAll("\\s", "").split(",")),
    //            Arrays.asList(q19Tables.replaceAll("\\s", "").split(",")),
    //            q19JoinCols);
    //    queries.add(q19);

    List<Pair<String, String>> q20JoinCols = new ArrayList<>();
    q20JoinCols.add(ImmutablePair.of("cs_sold_date_sk", "d_date_sk"));
    q20JoinCols.add(ImmutablePair.of("cs_item_sk", "i_item_sk"));
    String q20Tables = "catalog_sales ,item ,date_dim";
    String q20QCS = " i_item_id ,i_item_desc ,i_category ,i_class ,i_current_price, d_date";
    Query q20 =
        new Query(
            "20",
            Arrays.asList(q20QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q20Tables.replaceAll("\\s", "").split(",")),
            q20JoinCols);
    queries.add(q20);

    List<Pair<String, String>> q26JoinCols = new ArrayList<>();
    q26JoinCols.add(ImmutablePair.of("cs_sold_date_sk", "d_date_sk"));
    q26JoinCols.add(ImmutablePair.of("cs_item_sk", "i_item_sk"));
    q26JoinCols.add(ImmutablePair.of("cs_bill_cdemo_sk", "cd_demo_sk"));
    q26JoinCols.add(ImmutablePair.of("cs_promo_sk", "p_promo_sk"));
    String q26Tables = "catalog_sales, customer_demographics, date_dim, item, promotion";
    String q26QCS =
        "i_item_id, cd_gender, cd_marital_status, cd_education_status, p_channel_email, p_channel_event, d_year";
    Query q26 =
        new Query(
            "26",
            Arrays.asList(q26QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q26Tables.replaceAll("\\s", "").split(",")),
            q26JoinCols);
    queries.add(q26);

    List<Pair<String, String>> q30JoinCols = new ArrayList<>();
    q30JoinCols.add(ImmutablePair.of("wr_returned_date_sk", "d_date_sk"));
    q30JoinCols.add(ImmutablePair.of("wr_returning_addr_sk", "ca_address_sk"));
    String q30Tables = "web_returns ,date_dim ,customer_address";
    String q30QCS = "wr_returning_customer_sk ,ca_state, d_year";
    Query q30 =
        new Query(
            "30",
            Arrays.asList(q30QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q30Tables.replaceAll("\\s", "").split(",")),
            q30JoinCols);
    queries.add(q30);

    List<Pair<String, String>> q31_1JoinCols = new ArrayList<>();
    q31_1JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q31_1JoinCols.add(ImmutablePair.of("ss_addr_sk", "ca_address_sk"));
    String q31_1Tables = "store_sales,date_dim,customer_address";
    String q31_1QCS = "ca_county,d_qoy,d_year";
    Query q31_1 =
        new Query(
            "31_1",
            Arrays.asList(q31_1QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q31_1Tables.replaceAll("\\s", "").split(",")),
            q31_1JoinCols);
    queries.add(q31_1);

    List<Pair<String, String>> q31_2JoinCols = new ArrayList<>();
    q31_2JoinCols.add(ImmutablePair.of("ws_sold_date_sk", "d_date_sk"));
    q31_2JoinCols.add(ImmutablePair.of("ws_bill_addr_sk", "ca_address_sk"));
    String q31_2Tables = "web_sales,date_dim,customer_address";
    String q31_2QCS = "ca_county,d_qoy,d_year";
    Query q31_2 =
        new Query(
            "31_2",
            Arrays.asList(q31_2QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q31_2Tables.replaceAll("\\s", "").split(",")),
            q31_2JoinCols);
    queries.add(q31_2);

    List<Pair<String, String>> q32JoinCols = new ArrayList<>();
    q32JoinCols.add(ImmutablePair.of("cs_sold_date_sk", "d_date_sk"));
    q32JoinCols.add(ImmutablePair.of("cs_item_sk", "i_item_sk"));
    String q32Tables = "catalog_sales,date_dim,item";
    String q32QCS = "d_date,i_manufact_id";
    Query q32 =
        new Query(
            "32",
            Arrays.asList(q32QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q32Tables.replaceAll("\\s", "").split(",")),
            q32JoinCols);
    queries.add(q32);

    List<Pair<String, String>> q33_1JoinCols = new ArrayList<>();
    q33_1JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q33_1JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    q33_1JoinCols.add(ImmutablePair.of("ss_addr_sk", "ca_address_sk"));
    String q33_1Tables = "store_sales, date_dim, customer_address, item";
    String q33_1QCS = "i_manufact_id,d_year,d_moy,ca_gmt_offset";
    Query q33_1 =
        new Query(
            "33_1",
            Arrays.asList(q33_1QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q33_1Tables.replaceAll("\\s", "").split(",")),
            q33_1JoinCols);
    queries.add(q33_1);

    List<Pair<String, String>> q33_2JoinCols = new ArrayList<>();
    q33_2JoinCols.add(ImmutablePair.of("cs_sold_date_sk", "d_date_sk"));
    q33_2JoinCols.add(ImmutablePair.of("cs_item_sk", "i_item_sk"));
    q33_2JoinCols.add(ImmutablePair.of("cs_bill_addr_sk", "ca_address_sk"));
    String q33_2Tables = "catalog_sales, date_dim, customer_address, item";
    String q33_2QCS = "i_manufact_id,d_year,d_moy,ca_gmt_offset";
    Query q33_2 =
        new Query(
            "33_2",
            Arrays.asList(q33_2QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q33_2Tables.replaceAll("\\s", "").split(",")),
            q33_2JoinCols);
    queries.add(q33_2);

    List<Pair<String, String>> q33_3JoinCols = new ArrayList<>();
    q33_3JoinCols.add(ImmutablePair.of("ws_sold_date_sk", "d_date_sk"));
    q33_3JoinCols.add(ImmutablePair.of("ws_item_sk", "i_item_sk"));
    q33_3JoinCols.add(ImmutablePair.of("ws_bill_addr_sk", "ca_address_sk"));
    String q33_3Tables = "web_sales, date_dim, customer_address, item";
    String q33_3QCS = "i_manufact_id,d_year,d_moy,ca_gmt_offset";
    Query q33_3 =
        new Query(
            "33_3",
            Arrays.asList(q33_3QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q33_3Tables.replaceAll("\\s", "").split(",")),
            q33_3JoinCols);
    queries.add(q33_3);

    List<Pair<String, String>> q34JoinCols = new ArrayList<>();
    q34JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q34JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    q34JoinCols.add(ImmutablePair.of("ss_hdemo_sk", "hd_demo_sk"));
    String q34Tables = "store_sales,date_dim,store,household_demographics";
    String q34QCS = "d_dom,hd_buy_potential,hd_vehicle_count,d_year,s_county";
    Query q34 =
        new Query(
            "34",
            Arrays.asList(q34QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q34Tables.replaceAll("\\s", "").split(",")),
            q34JoinCols);
    queries.add(q34);

    List<Pair<String, String>> q39JoinCols = new ArrayList<>();
    q39JoinCols.add(ImmutablePair.of("inv_date_sk", "d_date_sk"));
    q39JoinCols.add(ImmutablePair.of("inv_item_sk", "i_item_sk"));
    q39JoinCols.add(ImmutablePair.of("inv_warehouse_sk", "w_warehouse_sk"));
    String q39Tables = "inventory ,item ,warehouse ,date_dim";
    String q39QCS = "d_year,w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy";
    Query q39 =
        new Query(
            "39",
            Arrays.asList(q39QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q39Tables.replaceAll("\\s", "").split(",")),
            q39JoinCols);
    queries.add(q39);

    List<Pair<String, String>> q42JoinCols = new ArrayList<>();
    q42JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q42JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    String q42Tables = "date_dim,store_sales,item";
    String q42QCS = "d_year,i_category_id,i_category,i_manager_id,d_moy";
    Query q42 =
        new Query(
            "42",
            Arrays.asList(q42QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q42Tables.replaceAll("\\s", "").split(",")),
            q42JoinCols);
    queries.add(q42);

    List<Pair<String, String>> q43JoinCols = new ArrayList<>();
    q43JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q43JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    String q43Tables = "date_dim,store_sales,store";
    String q43QCS = "s_gmt_offset,d_year,s_store_name,s_store_id";
    Query q43 =
        new Query(
            "43",
            Arrays.asList(q43QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q43Tables.replaceAll("\\s", "").split(",")),
            q43JoinCols);
    queries.add(q43);

    List<Pair<String, String>> q46JoinCols = new ArrayList<>();
    q46JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q46JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    q46JoinCols.add(ImmutablePair.of("ss_hdemo_sk", "hd_demo_sk"));
    q46JoinCols.add(ImmutablePair.of("ss_addr_sk", "ca_address_sk"));
    String q46Tables = "store_sales,date_dim,store,household_demographics,customer_address ";
    String q46QCS =
        "ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city,d_dow,d_year,s_city,hd_dep_count,hd_vehicle_count";
    Query q46 =
        new Query(
            "46",
            Arrays.asList(q46QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q46Tables.replaceAll("\\s", "").split(",")),
            q46JoinCols);
    queries.add(q46);

    List<Pair<String, String>> q47JoinCols = new ArrayList<>();
    q47JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q47JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    q47JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    String q47Tables = "item, store_sales, date_dim, store";
    String q47QCS = "i_category, i_brand, s_store_name, s_company_name, d_year, d_moy";
    Query q47 =
        new Query(
            "47",
            Arrays.asList(q47QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q47Tables.replaceAll("\\s", "").split(",")),
            q47JoinCols);
    queries.add(q47);

    List<Pair<String, String>> q48JoinCols = new ArrayList<>();
    String q48Tables = "store_sales, store, customer_demographics, customer_address, date_dim";
    q48JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q48JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    q48JoinCols.add(ImmutablePair.of("ss_cdemo_sk", "cd_demo_sk"));
    q48JoinCols.add(ImmutablePair.of("ss_addr_sk", "ca_address_sk"));
    String q48QCS =
        "d_year, cd_marital_status, cd_education_status, ss_sales_price, ca_country, ca_state, ss_net_profit";
    Query q48 =
        new Query(
            "48",
            Arrays.asList(q48QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q48Tables.replaceAll("\\s", "").split(",")),
            q48JoinCols);
    queries.add(q48);

    List<Pair<String, String>> q52JoinCols = new ArrayList<>();
    String q52Tables = "store_sales, item, date_dim";
    q52JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q52JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    String q52QCS = "d_year, i_brand, i_brand_id, d_moy, i_manager_id";
    Query q52 =
        new Query(
            "52",
            Arrays.asList(q52QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q52Tables.replaceAll("\\s", "").split(",")),
            q52JoinCols);
    queries.add(q52);

    List<Pair<String, String>> q53JoinCols = new ArrayList<>();
    String q53Tables = "store_sales, item, date_dim, store";
    q53JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q53JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    q53JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    String q53QCS = "d_month_seq, i_category, i_class, i_brand, i_manufact_id, d_qoy";
    Query q53 =
        new Query(
            "53",
            Arrays.asList(q53QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q53Tables.replaceAll("\\s", "").split(",")),
            q53JoinCols);
    queries.add(q53);

    //    List<Pair<String, String>> q54JoinCols = new ArrayList<>();
    //    String q54Tables = "store_sales, customer_address, store, date_dim, customer";
    //    q54JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    //    q54JoinCols.add(ImmutablePair.of("ss_customer_sk", "c_customer_sk"));
    //    q54JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    //    String q54QCS = "d_month_seq, i_category, i_class, i_brand, i_manufact_id, d_qoy";
    //    Query q54 =
    //        new Query(
    //            "54",
    //            Arrays.asList(q54QCS.replaceAll("\\s", "").split(",")),
    //            Arrays.asList(q54Tables.replaceAll("\\s", "").split(",")),
    //            q54JoinCols);
    //    queries.add(q54);

    List<Pair<String, String>> q55JoinCols = new ArrayList<>();
    String q55Tables = "store_sales, item, date_dim";
    q55JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q55JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    String q55QCS = "i_manager_id, d_moy, d_year, i_brand, i_brand_id";
    Query q55 =
        new Query(
            "55",
            Arrays.asList(q55QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q55Tables.replaceAll("\\s", "").split(",")),
            q55JoinCols);
    queries.add(q55);

    List<Pair<String, String>> q57JoinCols = new ArrayList<>();
    String q57Tables = "item, catalog_sales, date_dim, call_center";
    q57JoinCols.add(ImmutablePair.of("cs_sold_date_sk", "d_date_sk"));
    q57JoinCols.add(ImmutablePair.of("cs_item_sk", "i_item_sk"));
    q57JoinCols.add(ImmutablePair.of("cs_call_center_sk", "cc_call_center_sk"));
    String q57QCS = "i_category, i_brand, cc_name , d_year, d_moy";
    Query q57 =
        new Query(
            "57",
            Arrays.asList(q57QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q57Tables.replaceAll("\\s", "").split(",")),
            q57JoinCols);
    queries.add(q57);

    List<Pair<String, String>> q58_1JoinCols = new ArrayList<>();
    String q58_1Tables = "store_sales, item, date_dim";
    q58_1JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q58_1JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    String q58_1QCS = "d_date, i_item_id";
    Query q58_1 =
        new Query(
            "58_1",
            Arrays.asList(q58_1QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q58_1Tables.replaceAll("\\s", "").split(",")),
            q58_1JoinCols);
    queries.add(q58_1);

    List<Pair<String, String>> q58_2JoinCols = new ArrayList<>();
    String q58_2Tables = "catalog_sales, item, date_dim";
    q58_2JoinCols.add(ImmutablePair.of("cs_sold_date_sk", "d_date_sk"));
    q58_2JoinCols.add(ImmutablePair.of("cs_item_sk", "i_item_sk"));
    String q58_2QCS = "d_date, i_item_id";
    Query q58_2 =
        new Query(
            "58_2",
            Arrays.asList(q58_2QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q58_2Tables.replaceAll("\\s", "").split(",")),
            q58_2JoinCols);
    queries.add(q58_2);

    List<Pair<String, String>> q58_3JoinCols = new ArrayList<>();
    String q58_3Tables = "web_sales, item, date_dim";
    q58_3JoinCols.add(ImmutablePair.of("ws_sold_date_sk", "d_date_sk"));
    q58_3JoinCols.add(ImmutablePair.of("ws_item_sk", "i_item_sk"));
    String q58_3QCS = "d_date, i_item_id";
    Query q58_3 =
        new Query(
            "58_3",
            Arrays.asList(q58_3QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q58_3Tables.replaceAll("\\s", "").split(",")),
            q58_3JoinCols);
    queries.add(q58_3);

    List<Pair<String, String>> q59JoinCols = new ArrayList<>();
    String q59Tables = "date_dim, store_sales";
    q59JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    String q59QCS = "d_week_seq, ss_store_sk";
    Query q59 =
        new Query(
            "59",
            Arrays.asList(q59QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q59Tables.replaceAll("\\s", "").split(",")),
            q59JoinCols);
    queries.add(q59);

    List<Pair<String, String>> q60_1JoinCols = new ArrayList<>();
    q60_1JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q60_1JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    q60_1JoinCols.add(ImmutablePair.of("ss_addr_sk", "ca_address_sk"));
    String q60_1Tables = "store_sales, date_dim, customer_address, item";
    String q60_1QCS = "i_item_id,d_year,d_moy,ca_gmt_offset";
    Query q60_1 =
        new Query(
            "60_1",
            Arrays.asList(q60_1QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q60_1Tables.replaceAll("\\s", "").split(",")),
            q60_1JoinCols);
    queries.add(q60_1);

    List<Pair<String, String>> q60_2JoinCols = new ArrayList<>();
    q60_2JoinCols.add(ImmutablePair.of("cs_sold_date_sk", "d_date_sk"));
    q60_2JoinCols.add(ImmutablePair.of("cs_item_sk", "i_item_sk"));
    q60_2JoinCols.add(ImmutablePair.of("cs_bill_addr_sk", "ca_address_sk"));
    String q60_2Tables = "catalog_sales, date_dim, customer_address, item";
    String q60_2QCS = "i_item_id,d_year,d_moy,ca_gmt_offset";
    Query q60_2 =
        new Query(
            "60_2",
            Arrays.asList(q60_2QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q60_2Tables.replaceAll("\\s", "").split(",")),
            q60_2JoinCols);
    queries.add(q60_2);

    List<Pair<String, String>> q60_3JoinCols = new ArrayList<>();
    q60_3JoinCols.add(ImmutablePair.of("ws_sold_date_sk", "d_date_sk"));
    q60_3JoinCols.add(ImmutablePair.of("ws_item_sk", "i_item_sk"));
    q60_3JoinCols.add(ImmutablePair.of("ws_bill_addr_sk", "ca_address_sk"));
    String q60_3Tables = "web_sales, date_dim, customer_address, item";
    String q60_3QCS = "i_item_id,d_year,d_moy,ca_gmt_offset";
    Query q60_3 =
        new Query(
            "60_3",
            Arrays.asList(q60_3QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q60_3Tables.replaceAll("\\s", "").split(",")),
            q60_3JoinCols);
    queries.add(q60_3);

    //    List<Pair<String, String>> q61JoinCols = new ArrayList<>();
    //    String q61Tables = "store_sales ,store ,promotion ,date_dim ,customer ,customer_address
    // ,item";
    //    q61JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    //    q61JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    //    q61JoinCols.add(ImmutablePair.of("ss_promo_sk", "p_promo_sk"));
    //    q61JoinCols.add(ImmutablePair.of("ss_customer_sk", "c_customer_sk"));
    //    q61JoinCols.add(ImmutablePair.of("ca_address_sk", "c_current_addr_sk"));
    //    q61JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    //    String q61QCS =
    //        "ca_gmt_offset, i_category, p_channel_dmail, p_channel_email, p_channel_tv,
    // s_gmt_offset, d_year, d_moy";
    //    Query q61 =
    //        new Query(
    //            "61",
    //            Arrays.asList(q61QCS.replaceAll("\\s", "").split(",")),
    //            Arrays.asList(q61Tables.replaceAll("\\s", "").split(",")),
    //            q61JoinCols);
    //    queries.add(q61);

    List<Pair<String, String>> q62JoinCols = new ArrayList<>();
    String q62Tables = "web_sales ,warehouse ,ship_mode ,web_site ,date_dim";
    q62JoinCols.add(ImmutablePair.of("ws_ship_date_sk", "d_date_sk"));
    q62JoinCols.add(ImmutablePair.of("ws_warehouse_sk", "w_warehouse_sk"));
    q62JoinCols.add(ImmutablePair.of("ws_ship_mode_sk", "sm_ship_mode_sk"));
    q62JoinCols.add(ImmutablePair.of("ws_web_site_sk", "web_site_sk"));
    String q62QCS = "w_warehouse_name, sm_type, web_name, d_month_seq";
    Query q62 =
        new Query(
            "62",
            Arrays.asList(q62QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q62Tables.replaceAll("\\s", "").split(",")),
            q62JoinCols);
    queries.add(q62);

    List<Pair<String, String>> q63JoinCols = new ArrayList<>();
    String q63Tables = "store_sales, item, date_dim, store";
    q63JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q63JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    q63JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    String q63QCS = "d_month_seq, i_category, i_class, i_brand, i_manager_id, d_moy";
    Query q63 =
        new Query(
            "63",
            Arrays.asList(q63QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q63Tables.replaceAll("\\s", "").split(",")),
            q63JoinCols);
    queries.add(q63);

    List<Pair<String, String>> q65JoinCols = new ArrayList<>();
    String q65Tables = "store_sales, date_dim";
    q65JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    String q65QCS = "d_month_seq, ss_store_sk, ss_item_sk";
    Query q65 =
        new Query(
            "65",
            Arrays.asList(q65QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q65Tables.replaceAll("\\s", "").split(",")),
            q65JoinCols);
    queries.add(q65);

    List<Pair<String, String>> q68JoinCols = new ArrayList<>();
    String q68Tables = "store_sales ,date_dim ,store ,household_demographics ,customer_address ";
    q68JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q68JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    q68JoinCols.add(ImmutablePair.of("ss_hdemo_sk", "hd_demo_sk"));
    q68JoinCols.add(ImmutablePair.of("ss_addr_sk", "ca_address_sk"));
    String q68QCS =
        "d_dom, hd_dep_count, hd_vehicle_count, d_year, s_city, ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city";
    Query q68 =
        new Query(
            "68",
            Arrays.asList(q68QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q68Tables.replaceAll("\\s", "").split(",")),
            q68JoinCols);
    queries.add(q68);

    List<Pair<String, String>> q73JoinCols = new ArrayList<>();
    String q73Tables = "store_sales,date_dim,store,household_demographics";
    q73JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q73JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    q73JoinCols.add(ImmutablePair.of("ss_hdemo_sk", "hd_demo_sk"));
    String q73QCS =
        "d_dom, hd_buy_potential, hd_vehicle_count, d_year, s_county, ss_ticket_number,ss_customer_sk";
    Query q73 =
        new Query(
            "73",
            Arrays.asList(q73QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q73Tables.replaceAll("\\s", "").split(",")),
            q73JoinCols);
    queries.add(q73);

    List<Pair<String, String>> q74_1JoinCols = new ArrayList<>();
    String q74_1Tables = "store_sales,date_dim,customer";
    q74_1JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q74_1JoinCols.add(ImmutablePair.of("ss_customer_sk", "c_customer_sk"));
    String q74_1QCS = "c_customer_id ,c_first_name ,c_last_name ,d_year";
    Query q74_1 =
        new Query(
            "74_1",
            Arrays.asList(q74_1QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q74_1Tables.replaceAll("\\s", "").split(",")),
            q74_1JoinCols);
    queries.add(q74_1);

    List<Pair<String, String>> q74_2JoinCols = new ArrayList<>();
    String q74_2Tables = "web_sales,date_dim,customer";
    q74_2JoinCols.add(ImmutablePair.of("ws_sold_date_sk", "d_date_sk"));
    q74_2JoinCols.add(ImmutablePair.of("ws_bill_customer_sk", "c_customer_sk"));
    String q74_2QCS = "c_customer_id ,c_first_name ,c_last_name ,d_year";
    Query q74_2 =
        new Query(
            "74_2",
            Arrays.asList(q74_2QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q74_2Tables.replaceAll("\\s", "").split(",")),
            q74_2JoinCols);
    queries.add(q74_2);

    List<Pair<String, String>> q76_1JoinCols = new ArrayList<>();
    String q76_1Tables = "store_sales,date_dim,item";
    q76_1JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q76_1JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    String q76_1QCS = "ss_addr_sk, d_year, d_qoy, i_category";
    Query q76_1 =
        new Query(
            "76_1",
            Arrays.asList(q76_1QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q76_1Tables.replaceAll("\\s", "").split(",")),
            q76_1JoinCols);
    queries.add(q76_1);

    List<Pair<String, String>> q76_2JoinCols = new ArrayList<>();
    String q76_2Tables = "catalog_sales,date_dim,item";
    q76_2JoinCols.add(ImmutablePair.of("cs_sold_date_sk", "d_date_sk"));
    q76_2JoinCols.add(ImmutablePair.of("cs_item_sk", "i_item_sk"));
    String q76_2QCS = "cs_warehouse_sk, d_year, d_qoy, i_category";
    Query q76_2 =
        new Query(
            "76_2",
            Arrays.asList(q76_2QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q76_2Tables.replaceAll("\\s", "").split(",")),
            q76_2JoinCols);
    queries.add(q76_2);

    List<Pair<String, String>> q76_3JoinCols = new ArrayList<>();
    String q76_3Tables = "web_sales,date_dim,item";
    q76_3JoinCols.add(ImmutablePair.of("ws_sold_date_sk", "d_date_sk"));
    q76_3JoinCols.add(ImmutablePair.of("ws_item_sk", "i_item_sk"));
    String q76_3QCS = "ws_web_page_sk, d_year, d_qoy, i_category";
    Query q76_3 =
        new Query(
            "76_3",
            Arrays.asList(q76_3QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q76_3Tables.replaceAll("\\s", "").split(",")),
            q76_3JoinCols);
    queries.add(q76_3);

    List<Pair<String, String>> q79JoinCols = new ArrayList<>();
    q79JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q79JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    q79JoinCols.add(ImmutablePair.of("ss_hdemo_sk", "hd_demo_sk"));
    String q79Tables = "store_sales,date_dim,store,household_demographics";
    String q79QCS =
        "ss_ticket_number,ss_customer_sk,ss_addr_sk,s_city,hd_dep_count,hd_vehicle_count,d_dow,d_year,s_number_employees";
    Query q79 =
        new Query(
            "79",
            Arrays.asList(q79QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q79Tables.replaceAll("\\s", "").split(",")),
            q79JoinCols);
    queries.add(q79);

    List<Pair<String, String>> q81JoinCols = new ArrayList<>();
    String q81Tables = "catalog_returns,date_dim,customer_address";
    q81JoinCols.add(ImmutablePair.of("cr_returned_date_sk", "d_date_sk"));
    q81JoinCols.add(ImmutablePair.of("cr_returning_addr_sk", "ca_address_sk"));
    String q81QCS = "cr_returning_customer_sk ,ca_state, d_year";
    Query q81 =
        new Query(
            "81",
            Arrays.asList(q81QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q81Tables.replaceAll("\\s", "").split(",")),
            q81JoinCols);
    queries.add(q81);

    List<Pair<String, String>> q83_1JoinCols = new ArrayList<>();
    String q83_1Tables = "store_returns,date_dim,item";
    q83_1JoinCols.add(ImmutablePair.of("sr_returned_date_sk", "d_date_sk"));
    q83_1JoinCols.add(ImmutablePair.of("sr_item_sk", "i_item_sk"));
    String q83_1QCS = "d_date, i_item_id";
    Query q83_1 =
        new Query(
            "83_1",
            Arrays.asList(q83_1QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q83_1Tables.replaceAll("\\s", "").split(",")),
            q83_1JoinCols);
    queries.add(q83_1);

    List<Pair<String, String>> q83_2JoinCols = new ArrayList<>();
    String q83_2Tables = "catalog_returns,date_dim,item";
    q83_2JoinCols.add(ImmutablePair.of("cr_returned_date_sk", "d_date_sk"));
    q83_2JoinCols.add(ImmutablePair.of("cr_item_sk", "i_item_sk"));
    String q83_2QCS = "d_date, i_item_id";
    Query q83_2 =
        new Query(
            "83_2",
            Arrays.asList(q83_2QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q83_2Tables.replaceAll("\\s", "").split(",")),
            q83_2JoinCols);
    queries.add(q83_2);

    List<Pair<String, String>> q83_3JoinCols = new ArrayList<>();
    String q83_3Tables = "web_returns,date_dim,item";
    q83_3JoinCols.add(ImmutablePair.of("wr_returned_date_sk", "d_date_sk"));
    q83_3JoinCols.add(ImmutablePair.of("wr_item_sk", "i_item_sk"));
    String q83_3QCS = "d_date, i_item_id";
    Query q83_3 =
        new Query(
            "83_3",
            Arrays.asList(q83_3QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q83_3Tables.replaceAll("\\s", "").split(",")),
            q83_3JoinCols);
    queries.add(q83_3);

    List<Pair<String, String>> q88JoinCols = new ArrayList<>();
    String q88Tables = "store_sales,time_dim,store,household_demographics";
    q88JoinCols.add(ImmutablePair.of("ss_sold_time_sk", "t_time_sk"));
    q88JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    q88JoinCols.add(ImmutablePair.of("ss_hdemo_sk", "hd_demo_sk"));
    String q88QCS = "t_hour, t_minute, hd_dep_count, s_store_name";
    Query q88 =
        new Query(
            "88",
            Arrays.asList(q88QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q88Tables.replaceAll("\\s", "").split(",")),
            q88JoinCols);
    queries.add(q88);

    List<Pair<String, String>> q89JoinCols = new ArrayList<>();
    String q89Tables = "store_sales,date_dim,store,item";
    q89JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q89JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    q89JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    String q89QCS = "d_year, i_category, i_class, i_brand, s_store_name, s_company_name, d_moy";
    Query q89 =
        new Query(
            "89",
            Arrays.asList(q89QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q89Tables.replaceAll("\\s", "").split(",")),
            q89JoinCols);
    queries.add(q89);

    List<Pair<String, String>> q90JoinCols = new ArrayList<>();
    String q90Tables = "web_sales, household_demographics , time_dim, web_page";
    q90JoinCols.add(ImmutablePair.of("ws_sold_time_sk", "t_time_sk"));
    q90JoinCols.add(ImmutablePair.of("ws_ship_hdemo_sk", "hd_demo_sk"));
    q90JoinCols.add(ImmutablePair.of("ws_web_page_sk", "wp_web_page_sk"));
    String q90QCS = "t_hour, hd_dep_count, wp_char_count";
    Query q90 =
        new Query(
            "90",
            Arrays.asList(q90QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q90Tables.replaceAll("\\s", "").split(",")),
            q90JoinCols);
    queries.add(q90);

    List<Pair<String, String>> q91JoinCols = new ArrayList<>();
    String q91Tables =
        "call_center, catalog_returns, date_dim, customer, customer_address, customer_demographics, household_demographics";
    q91JoinCols.add(ImmutablePair.of("cr_call_center_sk", "cc_call_center_sk"));
    q91JoinCols.add(ImmutablePair.of("cr_returned_date_sk", "d_date_sk"));
    q91JoinCols.add(ImmutablePair.of("cr_returning_customer_sk", "c_customer_sk"));
    q91JoinCols.add(ImmutablePair.of("cd_demo_sk", "c_current_cdemo_sk"));
    q91JoinCols.add(ImmutablePair.of("hd_demo_sk", "c_current_hdemo_sk"));
    q91JoinCols.add(ImmutablePair.of("ca_address_sk", "c_current_addr_sk"));
    String q91QCS =
        "d_year, d_moy, cd_marital_status, cd_education_status, hd_buy_potential, ca_gmt_offset, cc_call_center_id,cc_name,cc_manager";
    Query q91 =
        new Query(
            "91",
            Arrays.asList(q91QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q91Tables.replaceAll("\\s", "").split(",")),
            q91JoinCols);
    queries.add(q91);

    List<Pair<String, String>> q92JoinCols = new ArrayList<>();
    String q92Tables = "web_sales, item, date_dim";
    q92JoinCols.add(ImmutablePair.of("ws_sold_date_sk", "d_date_sk"));
    q92JoinCols.add(ImmutablePair.of("ws_item_sk", "i_item_sk"));
    String q92QCS = "i_manufact_id, d_date";
    Query q92 =
        new Query(
            "92",
            Arrays.asList(q92QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q92Tables.replaceAll("\\s", "").split(",")),
            q92JoinCols);
    queries.add(q92);

    List<Pair<String, String>> q96JoinCols = new ArrayList<>();
    String q96Tables = "store_sales,time_dim,store,household_demographics";
    q96JoinCols.add(ImmutablePair.of("ss_sold_time_sk", "t_time_sk"));
    q96JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    q96JoinCols.add(ImmutablePair.of("ss_hdemo_sk", "hd_demo_sk"));
    String q96QCS = "t_hour, t_minute, hd_dep_count, s_store_name";
    Query q96 =
        new Query(
            "96",
            Arrays.asList(q96QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q96Tables.replaceAll("\\s", "").split(",")),
            q96JoinCols);
    queries.add(q96);

    List<Pair<String, String>> q98JoinCols = new ArrayList<>();
    String q98Tables = "store_sales,date_dim,item";
    q98JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q98JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    String q98QCS = "i_item_id ,i_item_desc ,i_category ,i_class ,i_current_price, d_date";
    Query q98 =
        new Query(
            "98",
            Arrays.asList(q98QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q98Tables.replaceAll("\\s", "").split(",")),
            q98JoinCols);
    queries.add(q98);
  }

  private static void setQueriesWithoutPrejoin() {
    List<Pair<String, String>> q1JoinCols = new ArrayList<>();
    Query q1 =
        new Query(
            "1_nj",
            Arrays.asList("sr_returned_date_sk", "sr_customer_sk", "sr_store_sk"),
            Arrays.asList("store_returns"),
            q1JoinCols);
    queries.add(q1);

    List<Pair<String, String>> q3JoinCols = new ArrayList<>();
    Query q3 =
        new Query(
            "3_nj",
            Arrays.asList("ss_sold_date_sk", "ss_item_sk"),
            Arrays.asList("store_sales"),
            q3JoinCols);
    queries.add(q3);

    List<Pair<String, String>> q6JoinCols = new ArrayList<>();
    Query q6 =
        new Query(
            "6_nj",
            Arrays.asList("ss_sold_date_sk", "ss_item_sk", "ss_customer_sk"),
            Arrays.asList("store_sales"),
            q6JoinCols);
    queries.add(q6);

    List<Pair<String, String>> q7JoinCols = new ArrayList<>();
    Query q7 =
        new Query(
            "7_nj",
            Arrays.asList("ss_cdemo_sk", "ss_promo_sk", "ss_sold_date_sk", "ss_item_sk"),
            Arrays.asList("store_sales"),
            q7JoinCols);
    queries.add(q7);

    List<Pair<String, String>> q11_1JoinCols = new ArrayList<>();
    String q11_1Tables = "store_sales";
    String q11_1QCS = "ss_customer_sk,ss_sold_date_sk";
    Query q11_1 =
        new Query(
            "11_1_nj",
            Arrays.asList(q11_1QCS.split(",")),
            Arrays.asList(q11_1Tables.split(",")),
            q11_1JoinCols);
    queries.add(q11_1);

    List<Pair<String, String>> q11_2JoinCols = new ArrayList<>();
    String q11_2Tables = "web_sales";
    String q11_2QCS = "ws_bill_customer_sk,ws_sold_date_sk";
    Query q11_2 =
        new Query(
            "11_2_nj",
            Arrays.asList(q11_2QCS.split(",")),
            Arrays.asList(q11_2Tables.split(",")),
            q11_2JoinCols);
    queries.add(q11_2);

    List<Pair<String, String>> q12JoinCols = new ArrayList<>();
    String q12Tables = "web_sales";
    String q12QCS = "ws_sold_date_sk, ws_item_sk";
    Query q12 =
        new Query(
            "12_nj",
            Arrays.asList(q12QCS.split(",")),
            Arrays.asList(q12Tables.split(",")),
            q12JoinCols);
    queries.add(q12);

    List<Pair<String, String>> q13JoinCols = new ArrayList<>();
    Query q13 =
        new Query(
            "13_nj",
            Arrays.asList("ss_sold_date_sk", "ss_cdemo_sk", "ss_hdemo_sk", "ss_sales_price"),
            Arrays.asList("store_sales"),
            q13JoinCols);
    queries.add(q13);

    List<Pair<String, String>> q15JoinCols = new ArrayList<>();
    String q15Tables = "catalog_sales";
    Query q15 =
        new Query(
            "15_nj",
            Arrays.asList("cs_bill_customer_sk", "cs_sold_date_sk"),
            Arrays.asList(q15Tables.split(",")),
            q15JoinCols);
    queries.add(q15);

    List<Pair<String, String>> q16JoinCols = new ArrayList<>();
    String q16Tables = "catalog_sales";
    Query q16 =
        new Query(
            "16_nj",
            Arrays.asList(
                "cs_ship_date_sk", "cs_ship_addr_sk", "cs_call_center_sk", "cs_warehouse_sk"),
            Arrays.asList(q16Tables.split(",")),
            q16JoinCols);
    queries.add(q16);

    //    List<Pair<String, String>> q19JoinCols = new ArrayList<>();
    //    q19JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    //    q19JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    //    q19JoinCols.add(ImmutablePair.of("ss_customer_sk", "c_customer_sk"));
    //    q19JoinCols.add(ImmutablePair.of("c_current_addr_sk", "ca_address_sk"));
    //    String q19Tables = "date_dim, store_sales, item,customer,customer_address,store";
    //    String q19QCS =
    //        "i_brand ,i_brand_id ,i_manufact_id ,i_manufact, i_manager_id, d_moy, d_year, ca_zip,
    // s_zip";
    //    Query q19 =
    //        new Query(
    //            "19",
    //            Arrays.asList(q19QCS.replaceAll("\\s", "").split(",")),
    //            Arrays.asList(q19Tables.replaceAll("\\s", "").split(",")),
    //            q19JoinCols);
    //    queries.add(q19);

    List<Pair<String, String>> q20JoinCols = new ArrayList<>();
    String q20Tables = "catalog_sales";
    String q20QCS = "cs_sold_date_sk,cs_item_sk";
    Query q20 =
        new Query(
            "20_nj",
            Arrays.asList(q20QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q20Tables.replaceAll("\\s", "").split(",")),
            q20JoinCols);
    queries.add(q20);

    List<Pair<String, String>> q26JoinCols = new ArrayList<>();
    String q26Tables = "catalog_sales";
    String q26QCS = "cs_sold_date_sk, cs_item_sk, cs_bill_cdemo_sk, cs_promo_sk";
    Query q26 =
        new Query(
            "26_nj",
            Arrays.asList(q26QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q26Tables.replaceAll("\\s", "").split(",")),
            q26JoinCols);
    queries.add(q26);

    List<Pair<String, String>> q30JoinCols = new ArrayList<>();
    String q30Tables = "web_returns";
    String q30QCS = "wr_returning_customer_sk, wr_returned_date_sk, wr_returning_addr_sk";
    Query q30 =
        new Query(
            "30_nj",
            Arrays.asList(q30QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q30Tables.replaceAll("\\s", "").split(",")),
            q30JoinCols);
    queries.add(q30);

    List<Pair<String, String>> q31_1JoinCols = new ArrayList<>();
    String q31_1Tables = "store_sales";
    String q31_1QCS = "ss_sold_date_sk,ss_addr_sk";
    Query q31_1 =
        new Query(
            "31_1_nj",
            Arrays.asList(q31_1QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q31_1Tables.replaceAll("\\s", "").split(",")),
            q31_1JoinCols);
    queries.add(q31_1);

    List<Pair<String, String>> q31_2JoinCols = new ArrayList<>();
    String q31_2Tables = "web_sales";
    String q31_2QCS = "ws_sold_date_sk, ws_bill_addr_sk";
    Query q31_2 =
        new Query(
            "31_2_nj",
            Arrays.asList(q31_2QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q31_2Tables.replaceAll("\\s", "").split(",")),
            q31_2JoinCols);
    queries.add(q31_2);

    List<Pair<String, String>> q32JoinCols = new ArrayList<>();
    q32JoinCols.add(ImmutablePair.of("cs_sold_date_sk", "d_date_sk"));
    q32JoinCols.add(ImmutablePair.of("cs_item_sk", "i_item_sk"));
    String q32Tables = "catalog_sales";
    String q32QCS = "cs_sold_date_sk,cs_item_sk";
    Query q32 =
        new Query(
            "32_nj",
            Arrays.asList(q32QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q32Tables.replaceAll("\\s", "").split(",")),
            q32JoinCols);
    queries.add(q32);

    List<Pair<String, String>> q33_1JoinCols = new ArrayList<>();
    String q33_1Tables = "store_sales";
    String q33_1QCS = "ss_item_sk,ss_sold_date_sk,ss_addr_sk";
    Query q33_1 =
        new Query(
            "33_1_nj",
            Arrays.asList(q33_1QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q33_1Tables.replaceAll("\\s", "").split(",")),
            q33_1JoinCols);
    queries.add(q33_1);

    List<Pair<String, String>> q33_2JoinCols = new ArrayList<>();
    String q33_2Tables = "catalog_sales";
    String q33_2QCS = "cs_item_sk,cs_sold_date_sk,cs_bill_addr_sk";
    Query q33_2 =
        new Query(
            "33_2_nj",
            Arrays.asList(q33_2QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q33_2Tables.replaceAll("\\s", "").split(",")),
            q33_2JoinCols);
    queries.add(q33_2);

    List<Pair<String, String>> q33_3JoinCols = new ArrayList<>();
    String q33_3Tables = "web_sales";
    String q33_3QCS = "ws_item_sk,ws_sold_date_sk,ws_bill_addr_sk";
    Query q33_3 =
        new Query(
            "33_3_nj",
            Arrays.asList(q33_3QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q33_3Tables.replaceAll("\\s", "").split(",")),
            q33_3JoinCols);
    queries.add(q33_3);

    List<Pair<String, String>> q34JoinCols = new ArrayList<>();
    String q34Tables = "store_sales";
    String q34QCS = "ss_sold_date_sk,ss_hdemo_sk,ss_store_sk";
    Query q34 =
        new Query(
            "34_nj",
            Arrays.asList(q34QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q34Tables.replaceAll("\\s", "").split(",")),
            q34JoinCols);
    queries.add(q34);

    List<Pair<String, String>> q39JoinCols = new ArrayList<>();
    String q39Tables = "inventory";
    String q39QCS = "inv_date_sk,inv_warehouse_sk,inv_item_sk";
    Query q39 =
        new Query(
            "39_nj",
            Arrays.asList(q39QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q39Tables.replaceAll("\\s", "").split(",")),
            q39JoinCols);
    queries.add(q39);

    List<Pair<String, String>> q42JoinCols = new ArrayList<>();
    String q42Tables = "store_sales";
    String q42QCS = "ss_sold_date_sk, ss_item_sk";
    Query q42 =
        new Query(
            "42_nj",
            Arrays.asList(q42QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q42Tables.replaceAll("\\s", "").split(",")),
            q42JoinCols);
    queries.add(q42);

    List<Pair<String, String>> q43JoinCols = new ArrayList<>();
    String q43Tables = "store_sales";
    String q43QCS = "ss_sold_date_sk, ss_item_sk";
    Query q43 =
        new Query(
            "43_nj",
            Arrays.asList(q43QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q43Tables.replaceAll("\\s", "").split(",")),
            q43JoinCols);
    queries.add(q43);

    List<Pair<String, String>> q46JoinCols = new ArrayList<>();
    q46JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q46JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    q46JoinCols.add(ImmutablePair.of("ss_hdemo_sk", "hd_demo_sk"));
    q46JoinCols.add(ImmutablePair.of("ss_addr_sk", "ca_address_sk"));
    String q46Tables = "store_sales";
    String q46QCS =
        "ss_ticket_number,ss_customer_sk,ss_addr_sk,ss_sold_date_sk,ss_hdemo_sk,ss_store_sk";
    Query q46 =
        new Query(
            "46_nj",
            Arrays.asList(q46QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q46Tables.replaceAll("\\s", "").split(",")),
            q46JoinCols);
    queries.add(q46);

    List<Pair<String, String>> q47JoinCols = new ArrayList<>();
    String q47Tables = "store_sales";
    String q47QCS = "ss_item_sk, ss_sold_date_sk, ss_store_sk";
    Query q47 =
        new Query(
            "47_nj",
            Arrays.asList(q47QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q47Tables.replaceAll("\\s", "").split(",")),
            q47JoinCols);
    queries.add(q47);

    List<Pair<String, String>> q48JoinCols = new ArrayList<>();
    String q48Tables = "store_sales";
    String q48QCS = "ss_sold_date_sk, ss_cdemo_sk, ss_sales_price, ss_addr_sk, ss_net_profit";
    Query q48 =
        new Query(
            "48_nj",
            Arrays.asList(q48QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q48Tables.replaceAll("\\s", "").split(",")),
            q48JoinCols);
    queries.add(q48);

    List<Pair<String, String>> q52JoinCols = new ArrayList<>();
    String q52Tables = "store_sales";
    String q52QCS = "ss_sold_date_sk, ss_item_sk";
    Query q52 =
        new Query(
            "52_nj",
            Arrays.asList(q52QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q52Tables.replaceAll("\\s", "").split(",")),
            q52JoinCols);
    queries.add(q52);

    List<Pair<String, String>> q53JoinCols = new ArrayList<>();
    String q53Tables = "store_sales";
    String q53QCS = "ss_sold_date_sk, ss_item_sk";
    Query q53 =
        new Query(
            "53_nj",
            Arrays.asList(q53QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q53Tables.replaceAll("\\s", "").split(",")),
            q53JoinCols);
    queries.add(q53);

    //    List<Pair<String, String>> q54JoinCols = new ArrayList<>();
    //    String q54Tables = "store_sales, customer_address, store, date_dim, customer";
    //    q54JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    //    q54JoinCols.add(ImmutablePair.of("ss_customer_sk", "c_customer_sk"));
    //    q54JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    //    String q54QCS = "d_month_seq, i_category, i_class, i_brand, i_manufact_id, d_qoy";
    //    Query q54 =
    //        new Query(
    //            "54",
    //            Arrays.asList(q54QCS.replaceAll("\\s", "").split(",")),
    //            Arrays.asList(q54Tables.replaceAll("\\s", "").split(",")),
    //            q54JoinCols);
    //    queries.add(q54);

    List<Pair<String, String>> q55JoinCols = new ArrayList<>();
    String q55Tables = "store_sales";
    String q55QCS = "ss_sold_date_sk, ss_item_sk";
    Query q55 =
        new Query(
            "55_nj",
            Arrays.asList(q55QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q55Tables.replaceAll("\\s", "").split(",")),
            q55JoinCols);
    queries.add(q55);

    List<Pair<String, String>> q57JoinCols = new ArrayList<>();
    String q57Tables = "catalog_sales";
    String q57QCS = "cs_item_sk, cs_call_center_sk, cs_sold_date_sk";
    Query q57 =
        new Query(
            "57_nj",
            Arrays.asList(q57QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q57Tables.replaceAll("\\s", "").split(",")),
            q57JoinCols);
    queries.add(q57);

    List<Pair<String, String>> q58_1JoinCols = new ArrayList<>();
    String q58_1Tables = "store_sales";
    String q58_1QCS = "ss_sold_date_sk, ss_item_sk";
    Query q58_1 =
        new Query(
            "58_1_nj",
            Arrays.asList(q58_1QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q58_1Tables.replaceAll("\\s", "").split(",")),
            q58_1JoinCols);
    queries.add(q58_1);

    List<Pair<String, String>> q58_2JoinCols = new ArrayList<>();
    String q58_2Tables = "catalog_sales";
    String q58_2QCS = "cs_sold_date_sk, cs_item_sk";
    Query q58_2 =
        new Query(
            "58_2_nj",
            Arrays.asList(q58_2QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q58_2Tables.replaceAll("\\s", "").split(",")),
            q58_2JoinCols);
    queries.add(q58_2);

    List<Pair<String, String>> q58_3JoinCols = new ArrayList<>();
    String q58_3Tables = "web_sales";
    String q58_3QCS = "ws_sold_date_sk, ws_item_sk";
    Query q58_3 =
        new Query(
            "58_3_nj",
            Arrays.asList(q58_3QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q58_3Tables.replaceAll("\\s", "").split(",")),
            q58_3JoinCols);
    queries.add(q58_3);

    List<Pair<String, String>> q59JoinCols = new ArrayList<>();
    String q59Tables = "store_sales";
    String q59QCS = "ss_sold_date_sk, ss_store_sk";
    Query q59 =
        new Query(
            "59_nj",
            Arrays.asList(q59QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q59Tables.replaceAll("\\s", "").split(",")),
            q59JoinCols);
    queries.add(q59);

    List<Pair<String, String>> q60_1JoinCols = new ArrayList<>();
    String q60_1Tables = "store_sales";
    String q60_1QCS = "ss_item_sk,ss_sold_date_sk,ss_addr_sk";
    Query q60_1 =
        new Query(
            "60_1_nj",
            Arrays.asList(q60_1QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q60_1Tables.replaceAll("\\s", "").split(",")),
            q60_1JoinCols);
    queries.add(q60_1);

    List<Pair<String, String>> q60_2JoinCols = new ArrayList<>();
    String q60_2Tables = "catalog_sales";
    String q60_2QCS = "cs_item_sk,cs_sold_date_sk,cs_bill_addr_sk";
    Query q60_2 =
        new Query(
            "60_2_nj",
            Arrays.asList(q60_2QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q60_2Tables.replaceAll("\\s", "").split(",")),
            q60_2JoinCols);
    queries.add(q60_2);

    List<Pair<String, String>> q60_3JoinCols = new ArrayList<>();
    String q60_3Tables = "web_sales";
    String q60_3QCS = "ws_sold_date_sk,ws_item_sk,ws_bill_addr_sk";
    Query q60_3 =
        new Query(
            "60_3_nj",
            Arrays.asList(q60_3QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q60_3Tables.replaceAll("\\s", "").split(",")),
            q60_3JoinCols);
    queries.add(q60_3);

    //    List<Pair<String, String>> q61JoinCols = new ArrayList<>();
    //    String q61Tables = "store_sales ,store ,promotion ,date_dim ,customer ,customer_address
    // ,item";
    //    q61JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    //    q61JoinCols.add(ImmutablePair.of("ss_store_sk", "s_store_sk"));
    //    q61JoinCols.add(ImmutablePair.of("ss_promo_sk", "p_promo_sk"));
    //    q61JoinCols.add(ImmutablePair.of("ss_customer_sk", "c_customer_sk"));
    //    q61JoinCols.add(ImmutablePair.of("ca_address_sk", "c_current_addr_sk"));
    //    q61JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    //    String q61QCS =
    //        "ca_gmt_offset, i_category, p_channel_dmail, p_channel_email, p_channel_tv,
    // s_gmt_offset, d_year, d_moy";
    //    Query q61 =
    //        new Query(
    //            "61",
    //            Arrays.asList(q61QCS.replaceAll("\\s", "").split(",")),
    //            Arrays.asList(q61Tables.replaceAll("\\s", "").split(",")),
    //            q61JoinCols);
    //    queries.add(q61);

    List<Pair<String, String>> q62JoinCols = new ArrayList<>();
    String q62Tables = "web_sales";
    String q62QCS = "ws_warehouse_sk, ws_ship_mode_sk, ws_web_site_sk, ws_ship_date_sk";
    Query q62 =
        new Query(
            "62_nj",
            Arrays.asList(q62QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q62Tables.replaceAll("\\s", "").split(",")),
            q62JoinCols);
    queries.add(q62);

    List<Pair<String, String>> q63JoinCols = new ArrayList<>();
    String q63Tables = "store_sales";
    String q63QCS = "ss_sold_date_sk, ss_item_sk";
    Query q63 =
        new Query(
            "63_nj",
            Arrays.asList(q63QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q63Tables.replaceAll("\\s", "").split(",")),
            q63JoinCols);
    queries.add(q63);

    List<Pair<String, String>> q65JoinCols = new ArrayList<>();
    String q65Tables = "store_sales";
    String q65QCS = "ss_sold_date_sk, ss_store_sk, ss_item_sk";
    Query q65 =
        new Query(
            "65_nj",
            Arrays.asList(q65QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q65Tables.replaceAll("\\s", "").split(",")),
            q65JoinCols);
    queries.add(q65);

    List<Pair<String, String>> q68JoinCols = new ArrayList<>();
    String q68Tables = "store_sales";
    String q68QCS =
        "ss_sold_date_sk, ss_hdemo_sk, ss_store_sk, ss_ticket_number, ss_customer_sk, ss_addr_sk";
    Query q68 =
        new Query(
            "68_nj",
            Arrays.asList(q68QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q68Tables.replaceAll("\\s", "").split(",")),
            q68JoinCols);
    queries.add(q68);

    List<Pair<String, String>> q73JoinCols = new ArrayList<>();
    String q73Tables = "store_sales";
    String q73QCS = "ss_sold_date_sk, ss_hdemo_sk, ss_store_sk, ss_ticket_number,ss_customer_sk";
    Query q73 =
        new Query(
            "73_nj",
            Arrays.asList(q73QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q73Tables.replaceAll("\\s", "").split(",")),
            q73JoinCols);
    queries.add(q73);

    List<Pair<String, String>> q74_1JoinCols = new ArrayList<>();
    String q74_1Tables = "store_sales";
    String q74_1QCS = "ss_sold_date_sk,ss_customer_sk";
    Query q74_1 =
        new Query(
            "74_1_nj",
            Arrays.asList(q74_1QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q74_1Tables.replaceAll("\\s", "").split(",")),
            q74_1JoinCols);
    queries.add(q74_1);

    List<Pair<String, String>> q74_2JoinCols = new ArrayList<>();
    String q74_2Tables = "web_sales";
    String q74_2QCS = "ws_bill_customer_sk,ws_sold_date_sk";
    Query q74_2 =
        new Query(
            "74_2_nj",
            Arrays.asList(q74_2QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q74_2Tables.replaceAll("\\s", "").split(",")),
            q74_2JoinCols);
    queries.add(q74_2);

    List<Pair<String, String>> q76_1JoinCols = new ArrayList<>();
    String q76_1Tables = "store_sales";
    String q76_1QCS = "ss_addr_sk, ss_sold_date_sk, ss_item_sk";
    Query q76_1 =
        new Query(
            "76_1_nj",
            Arrays.asList(q76_1QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q76_1Tables.replaceAll("\\s", "").split(",")),
            q76_1JoinCols);
    queries.add(q76_1);

    List<Pair<String, String>> q76_2JoinCols = new ArrayList<>();
    String q76_2Tables = "catalog_sales";
    String q76_2QCS = "cs_warehouse_sk, cs_sold_date_sk, cs_item_sk";
    Query q76_2 =
        new Query(
            "76_2_nj",
            Arrays.asList(q76_2QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q76_2Tables.replaceAll("\\s", "").split(",")),
            q76_2JoinCols);
    queries.add(q76_2);

    List<Pair<String, String>> q76_3JoinCols = new ArrayList<>();
    String q76_3Tables = "web_sales";
    String q76_3QCS = "ws_web_page_sk, ws_sold_date_sk, ws_item_sk";
    Query q76_3 =
        new Query(
            "76_3_nj",
            Arrays.asList(q76_3QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q76_3Tables.replaceAll("\\s", "").split(",")),
            q76_3JoinCols);
    queries.add(q76_3);

    List<Pair<String, String>> q79JoinCols = new ArrayList<>();
    String q79Tables = "store_sales";
    String q79QCS =
        "ss_ticket_number,ss_customer_sk,ss_addr_sk,ss_store_sk,ss_hdemo_sk,ss_sold_date_sk";
    Query q79 =
        new Query(
            "79_nj",
            Arrays.asList(q79QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q79Tables.replaceAll("\\s", "").split(",")),
            q79JoinCols);
    queries.add(q79);

    List<Pair<String, String>> q81JoinCols = new ArrayList<>();
    String q81Tables = "catalog_returns";
    String q81QCS = "cr_returning_customer_sk ,cr_returning_addr_sk, cr_returned_date_sk";
    Query q81 =
        new Query(
            "81_nj",
            Arrays.asList(q81QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q81Tables.replaceAll("\\s", "").split(",")),
            q81JoinCols);
    queries.add(q81);

    List<Pair<String, String>> q83_1JoinCols = new ArrayList<>();
    String q83_1Tables = "store_returns";
    String q83_1QCS = "sr_returned_date_sk, sr_item_sk";
    Query q83_1 =
        new Query(
            "83_1_nj",
            Arrays.asList(q83_1QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q83_1Tables.replaceAll("\\s", "").split(",")),
            q83_1JoinCols);
    queries.add(q83_1);

    List<Pair<String, String>> q83_2JoinCols = new ArrayList<>();
    String q83_2Tables = "catalog_returns";
    String q83_2QCS = "cr_returned_date_sk, cr_item_sk";
    Query q83_2 =
        new Query(
            "83_2_nj",
            Arrays.asList(q83_2QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q83_2Tables.replaceAll("\\s", "").split(",")),
            q83_2JoinCols);
    queries.add(q83_2);

    List<Pair<String, String>> q83_3JoinCols = new ArrayList<>();
    String q83_3Tables = "web_returns";
    String q83_3QCS = "wr_returned_date_sk, wr_item_sk";
    Query q83_3 =
        new Query(
            "83_3_nj",
            Arrays.asList(q83_3QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q83_3Tables.replaceAll("\\s", "").split(",")),
            q83_3JoinCols);
    queries.add(q83_3);

    List<Pair<String, String>> q88JoinCols = new ArrayList<>();
    String q88Tables = "store_sales";
    String q88QCS = "ss_sold_time_sk, ss_hdemo_sk, ss_store_sk";
    Query q88 =
        new Query(
            "88_nj",
            Arrays.asList(q88QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q88Tables.replaceAll("\\s", "").split(",")),
            q88JoinCols);
    queries.add(q88);

    List<Pair<String, String>> q89JoinCols = new ArrayList<>();
    String q89Tables = "store_sales";
    String q89QCS = "ss_sold_date_sk, ss_item_sk, ss_store_sk";
    Query q89 =
        new Query(
            "89_nj",
            Arrays.asList(q89QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q89Tables.replaceAll("\\s", "").split(",")),
            q89JoinCols);
    queries.add(q89);

    List<Pair<String, String>> q90JoinCols = new ArrayList<>();
    String q90Tables = "web_sales";
    String q90QCS = "ws_sold_time_sk, ws_ship_hdemo_sk, ws_web_page_sk";
    Query q90 =
        new Query(
            "90_nj",
            Arrays.asList(q90QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q90Tables.replaceAll("\\s", "").split(",")),
            q90JoinCols);
    queries.add(q90);

    List<Pair<String, String>> q91JoinCols = new ArrayList<>();
    String q91Tables = "catalog_returns";
    String q91QCS = "cr_returned_date_sk, cr_returning_customer_sk, cr_call_center_sk";
    Query q91 =
        new Query(
            "91_nj",
            Arrays.asList(q91QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q91Tables.replaceAll("\\s", "").split(",")),
            q91JoinCols);
    queries.add(q91);

    List<Pair<String, String>> q92JoinCols = new ArrayList<>();
    String q92Tables = "web_sales";
    String q92QCS = "ws_sold_date_sk, ws_item_sk";
    Query q92 =
        new Query(
            "92_nj",
            Arrays.asList(q92QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q92Tables.replaceAll("\\s", "").split(",")),
            q92JoinCols);
    queries.add(q92);

    List<Pair<String, String>> q96JoinCols = new ArrayList<>();
    String q96Tables = "store_sales";
    String q96QCS = "ss_sold_time_sk, ss_hdemo_sk, ss_store_sk";
    Query q96 =
        new Query(
            "96_nj",
            Arrays.asList(q96QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q96Tables.replaceAll("\\s", "").split(",")),
            q96JoinCols);
    queries.add(q96);

    List<Pair<String, String>> q98JoinCols = new ArrayList<>();
    String q98Tables = "store_sales";
    q98JoinCols.add(ImmutablePair.of("ss_sold_date_sk", "d_date_sk"));
    q98JoinCols.add(ImmutablePair.of("ss_item_sk", "i_item_sk"));
    String q98QCS = "ss_item_sk,ss_sold_date_sk";
    Query q98 =
        new Query(
            "98_nj",
            Arrays.asList(q98QCS.replaceAll("\\s", "").split(",")),
            Arrays.asList(q98Tables.replaceAll("\\s", "").split(",")),
            q98JoinCols);
    queries.add(q98);
  }
}
