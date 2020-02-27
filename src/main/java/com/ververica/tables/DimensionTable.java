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
import com.ververica.common.utils.RandomStringGenerator;
import java.util.SplittableRandom;
import org.apache.flink.configuration.Configuration;

@SuppressWarnings("WeakerAccess")
public class DimensionTable extends ParallelBaseGenerator<DimensionTable.Dimension> {
  private static final long serialVersionUID = 1L;

  private transient RandomStringGenerator randomString;
  private final int maxId;

  public DimensionTable(int maxRecordsPerSecond, int maxId) {
    super(maxRecordsPerSecond);
    this.maxId = maxId;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    randomString = new RandomStringGenerator();
  }

  @Override
  public Dimension randomEvent(SplittableRandom rnd, long id) {
    Dimension dimension = new Dimension();
    dimension.id = rnd.nextLong(maxId);
    dimension.col1 = randomString.randomString();
    dimension.col2 = randomString.randomString();
    dimension.col3 = randomString.randomString();
    dimension.col4 = randomString.randomString();
    dimension.col5 = randomString.randomString();

    return dimension;
  }

  public static class Dimension {
    public long id;
    public String col1;
    public String col2;
    public String col3;
    public String col4;
    public String col5;
  }
}
