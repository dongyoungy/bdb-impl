package dyoon;

import java.io.Serializable;

/** Created by Dong Young Yoon on 10/14/18. */
public class ColumnPair implements Serializable, Comparable<ColumnPair> {
  private static final long serialVersionUID = 5124715268882221529L;
  private String left;
  private String right;

  public ColumnPair() {}

  public ColumnPair(String left, String right) {
    this.left = left;
    this.right = right;
  }

  public String getLeft() {
    return left;
  }

  public String getRight() {
    return right;
  }

  @Override
  public int hashCode() {
    return left.hashCode() + right.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    ColumnPair other = (ColumnPair) obj;
    if (this.left.equals(other.left) && this.right.equals(other.right)) {
      return true;
    }
    return false;
  }

  @Override
  public int compareTo(ColumnPair o) {
    int compare = left.compareTo(o.left);
    if (compare != 0) return compare;
    else {
      return right.compareTo(o.right);
    }
  }
}
