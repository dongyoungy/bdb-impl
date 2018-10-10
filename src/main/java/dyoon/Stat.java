package dyoon;

/**
 * Created by Dong Young Yoon on 10/9/18.
 */
public class Stat {
  private long groupCount;
  private double avgGroupSize;
  private long maxGroupSize;

  public Stat(long groupCount, double avgGroupSize, long maxGroupSize) {
    this.groupCount = groupCount;
    this.avgGroupSize = avgGroupSize;
    this.maxGroupSize = maxGroupSize;
  }

  public long getGroupCount() {
    return groupCount;
  }

  public void setGroupCount(long groupCount) {
    this.groupCount = groupCount;
  }

  public double getAvgGroupSize() {
    return avgGroupSize;
  }

  public void setAvgGroupSize(double avgGroupSize) {
    this.avgGroupSize = avgGroupSize;
  }

  public long getMaxGroupSize() {
    return maxGroupSize;
  }

  public void setMaxGroupSize(long maxGroupSize) {
    this.maxGroupSize = maxGroupSize;
  }
}
