package com.ververica.common.sources;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/** A simple random data generator with data rate throttling logic (may run in parallel). */
public abstract class ParallelBaseGenerator<T> extends SingleBaseGenerator<T>
    implements ParallelSourceFunction<T> {

  protected ParallelBaseGenerator(int maxRecordsPerSecond) {
    super(maxRecordsPerSecond);
  }
}
