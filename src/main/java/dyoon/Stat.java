package dyoon;

import java.io.Serializable;

/** Created by Dong Young Yoon on 10/9/18. */
public class Stat implements Serializable {
  private long groupCount;
  private double avgGroupSize;
  private double targetSampleSize;
  private long populationSize;
  private long maxGroupSize;
  private long minGroupSize;

  public Stat(
      long populationSize,
      double targetSampleSize,
      long groupCount,
      double avgGroupSize,
      long minGroupSize,
      long maxGroupSize) {
    this.populationSize = populationSize;
    this.targetSampleSize = targetSampleSize;
    this.groupCount = groupCount;
    this.avgGroupSize = avgGroupSize;
    this.minGroupSize = minGroupSize;
    this.maxGroupSize = maxGroupSize;
  }

  public long getMinGroupSize() {
    return minGroupSize;
  }

  public void setMinGroupSize(long minGroupSize) {
    this.minGroupSize = minGroupSize;
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

  public double getTargetSampleSize() {
    return targetSampleSize;
  }

  public void setTargetSampleSize(double targetSampleSize) {
    this.targetSampleSize = targetSampleSize;
  }

  public long getPopulationSize() {
    return populationSize;
  }

  public void setPopulationSize(long populationSize) {
    this.populationSize = populationSize;
  }
}
