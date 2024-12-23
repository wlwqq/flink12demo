package com.wlh.p2;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;

public class HelloWorld {
    public static void main(String[] args) throws IOException {

        Configuration jobConfiguration = new Configuration();
        jobConfiguration.setString(RestOptions.BIND_PORT, "18081");

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(jobConfiguration);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        bsEnv.setParallelism(2);

        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        // 设置状态后端
//        bsEnv.setStateBackend(new RocksDBStateBackend(
//                "file:///Users/xxxxx/ideapro/rocksdb_state_dir", true));
//        RocksDBStateBackend stateBackend = (RocksDBStateBackend) bsEnv.getStateBackend();
//        stateBackend.setDbStoragePath("/Users/xxxxx/ideapro/rocksdb_data_dir");


        // 设置状态ttl
        bsTableEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(5));

        StatementSet statementSet = bsTableEnv.createStatementSet();

        InputStream resourceAsStream = HelloWorld.class.getClassLoader()
                .getResourceAsStream("sql/sql02_over_window1.sql");

        if (resourceAsStream == null) {
            throw new RuntimeException("resourceAsStream is null");
        }
        try (BufferedReader br = new BufferedReader(new InputStreamReader(resourceAsStream))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                if (StringUtils.isBlank(line)) {
                    continue;
                }
                line = line.trim();
                if (line.startsWith("--")) {
                    continue;
                }
                sb.append(line).append("\n");

                if (line.endsWith(";")) {
                    String _sql = sb.toString().trim();
                    String sql = _sql.substring(0, _sql.length() - 1);
                    if (sql.toLowerCase().startsWith("insert")) {
                        statementSet.addInsertSql(sql);
                    } else {
                        bsTableEnv.executeSql(sql);
                    }
                    sb = new StringBuilder();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        statementSet.execute();
    }
}
