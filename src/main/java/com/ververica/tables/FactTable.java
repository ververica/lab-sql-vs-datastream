package com.ververica.tables;

import com.ververica.common.sources.ParallelBaseGenerator;
import java.util.SplittableRandom;

@SuppressWarnings("WeakerAccess")
public class FactTable extends ParallelBaseGenerator<FactTable.Fact> {
  private static final long serialVersionUID = 1L;

  private final int maxId;

  public FactTable(int maxRecordsPerSecond, int maxId) {
    super(maxRecordsPerSecond);
    this.maxId = maxId;
  }

  @Override
  public Fact randomEvent(SplittableRandom rnd, long id) {
    Fact fact = new Fact();
    fact.dim1 = rnd.nextLong(maxId);
    fact.dim2 = rnd.nextLong(maxId);
    fact.dim3 = rnd.nextLong(maxId);
    fact.dim4 = rnd.nextLong(maxId);
    fact.dim5 = rnd.nextLong(maxId);

    return fact;
  }

  public static class Fact {
    public long dim1;
    public long dim2;
    public long dim3;
    public long dim4;
    public long dim5;
  }
}
