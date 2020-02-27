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

package com.ververica;

import com.ververica.tables.DimensionTable;
import com.ververica.tables.FactTable;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.EnvironmentSettings.Builder;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LateralTableJoin {

  private static final Logger LOG = LoggerFactory.getLogger(LateralTableJoin.class);

  public static void main(String[] args) throws Exception {
    ParameterTool tool = ParameterTool.fromArgs(args);
    int factTableRate = tool.getInt("factRate", 500_000);
    int dimTableRate = tool.getInt("dimRate", 1_000);
    String queryFile = tool.get("query", "query.sql");

    // disable optimisations:
    boolean noBlink = tool.has("noBlink");
    boolean noObjectReuse = tool.has("noObjectReuse");

    StreamExecutionEnvironment env = getEnvironment(tool);
    Builder plannerBuilder =
        noBlink
            ? EnvironmentSettings.newInstance().useOldPlanner()
            : EnvironmentSettings.newInstance().useBlinkPlanner();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, plannerBuilder.build());

    env.enableCheckpointing(60 * 1000 * 3);
    if (!noObjectReuse) {
      env.getConfig().enableObjectReuse();
    }

    int maxId = 100_000;
    tEnv.createTemporaryView(
        "fact_table",
        env.addSource(new FactTable(factTableRate, maxId)),
        "dim1, dim2, dim3, dim4, dim5, f_proctime.proctime");
    tEnv.createTemporaryView(
        "dim_table1",
        env.addSource(new DimensionTable(dimTableRate, maxId)),
        "id, col1, col2, col3, col4, col5, r_proctime.proctime");
    tEnv.createTemporaryView(
        "dim_table2",
        env.addSource(new DimensionTable(dimTableRate, maxId)),
        "id, col1, col2, col3, col4, col5, r_proctime.proctime");
    tEnv.createTemporaryView(
        "dim_table3",
        env.addSource(new DimensionTable(dimTableRate, maxId)),
        "id, col1, col2, col3, col4, col5, r_proctime.proctime");
    tEnv.createTemporaryView(
        "dim_table4",
        env.addSource(new DimensionTable(dimTableRate, maxId)),
        "id, col1, col2, col3, col4, col5, r_proctime.proctime");
    tEnv.createTemporaryView(
        "dim_table5",
        env.addSource(new DimensionTable(dimTableRate, maxId)),
        "id, col1, col2, col3, col4, col5, r_proctime.proctime");

    tEnv.registerFunction(
        "dimension_table1",
        tEnv.from("dim_table1").createTemporalTableFunction("r_proctime", "id"));
    tEnv.registerFunction(
        "dimension_table2",
        tEnv.from("dim_table2").createTemporalTableFunction("r_proctime", "id"));
    tEnv.registerFunction(
        "dimension_table3",
        tEnv.from("dim_table3").createTemporalTableFunction("r_proctime", "id"));
    tEnv.registerFunction(
        "dimension_table4",
        tEnv.from("dim_table4").createTemporalTableFunction("r_proctime", "id"));
    tEnv.registerFunction(
        "dimension_table5",
        tEnv.from("dim_table5").createTemporalTableFunction("r_proctime", "id"));

    String query = loadQuery(queryFile);
    LOG.info(query);

    Table results = tEnv.sqlQuery(query);

    tEnv.toAppendStream(results, Row.class).addSink(new DiscardingSink<>());
    env.execute();
  }

  static StreamExecutionEnvironment getEnvironment(ParameterTool tool) {
    final boolean localWithUi = tool.has("local");
    final int parallelism = tool.getInt("parallelism", -1);
    final StreamExecutionEnvironment env;
    if (localWithUi) {
      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
      if (parallelism > 0) {
        env.setParallelism(parallelism);
      }
    } else {
      env = StreamExecutionEnvironment.getExecutionEnvironment();
    }
    return env;
  }

  private static String loadQuery(String queryFile) throws IOException {
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    try (InputStream is = classLoader.getResourceAsStream(queryFile)) {
      if (is == null) {
        throw new FileNotFoundException("File 'query.sql' not found");
      }
      try (InputStreamReader isr = new InputStreamReader(is);
          BufferedReader reader = new BufferedReader(isr)) {
        return reader.lines().collect(Collectors.joining(System.lineSeparator()));
      }
    }
  }
}
