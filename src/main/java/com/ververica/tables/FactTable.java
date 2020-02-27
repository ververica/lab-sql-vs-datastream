/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
