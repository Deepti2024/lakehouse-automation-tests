package com.lakehouse.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Robust JDBC client wrapper to manage connections and query execution against
 * Trino.
 */
public class TrinoClient {
    private static final String JDBC_URL = "jdbc:trino://localhost:8082/iceberg";
    private static final String USER = "qa_automation";

    private Connection connection;

    /**
     * Opens a new connection to the Trino Coordinator.
     */
    public void connect() throws SQLException {
        if (connection == null || connection.isClosed()) {
            Properties properties = new Properties();
            properties.setProperty("user", USER);
            // Local Trino in our Docker stack does not have SSL or passwords enabled by
            // default
            connection = DriverManager.getConnection(JDBC_URL, properties);
        }
    }

    /**
     * Retrieves the active Connection instance.
     */
    public Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connect();
        }
        return connection;
    }

    /**
     * Utility method to execute a DDL or DML statement (CREATE, DROP, INSERT,
     * ALTER).
     */
    public void executeUpdate(String sql) throws SQLException {
        try (Statement stmt = getConnection().createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    /**
     * Closes the active connection safely.
     */
    public void close() {
        if (connection != null) {
            try {
                if (!connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException e) {
                System.err.println("Failed to close Trino connection safely: " + e.getMessage());
            }
        }
    }
}