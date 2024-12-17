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

public class HelloWorld {
    public static void main(String[] args) {

        Configuration jobConfiguration = new Configuration();
        jobConfiguration.setString(RestOptions.BIND_PORT, "18081");

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(jobConfiguration);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        StatementSet statementSet = bsTableEnv.createStatementSet();

        InputStream resourceAsStream = HelloWorld.class.getClassLoader().getResourceAsStream("sql/sql02_over_window1.sql");
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
                if (line.trim().startsWith("--")) {
                    continue;
                }
                sb.append(line.trim()).append("\n");

                if (line.trim().endsWith(";")) {
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
