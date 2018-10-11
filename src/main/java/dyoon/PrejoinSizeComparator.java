package dyoon;

import java.util.Comparator;

/** Created by Dong Young Yoon on 10/11/18. */
public class PrejoinSizeComparator implements Comparator<Prejoin> {
  @Override
  public int compare(Prejoin o1, Prejoin o2) {
    return o2.getTableSet().size() - o1.getTableSet().size();
  }
}
