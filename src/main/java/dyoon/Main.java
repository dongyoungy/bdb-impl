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

  public static void main(String[] args) {
    Connection conn = null;
    setQueries();

    try {
      Class.forName("com.cloudera.impala.jdbc41.Driver");
      String connectionStr = String.format("jdbc:impala://%s:%d/%s", HOST, PORT, DATABASE);
      conn =
          DriverManager.getConnection(connectionStr, "", "");
      DatabaseTool tool = new DatabaseTool(conn);

    } catch (SQLException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    for (Query q : queries) {}
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
  }
}
