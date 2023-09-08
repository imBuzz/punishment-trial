package it.buzz.punishment.data;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import it.buzz.punishment.Punishment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MySQLConnector {

    public final static String ACTIVE_TABLE = "active_p", HISTORY_TABLE = "history_p";
    private final ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("punishment-thread").build());
    private final Punishment plugin;
    private HikariDataSource dataSource;

    public MySQLConnector(Punishment plugin) {
        this.plugin = plugin;
    }

    public void init() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://localhost:3306/minecraft");
        config.setUsername("root");
        config.setPassword("root");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        dataSource = new HikariDataSource(config);

        //Create active punishments table
        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("CREATE TABLE IF NOT EXISTS " + ACTIVE_TABLE + " " +
                "(id INT AUTO_INCREMENT PRIMARY KEY, type VARCHAR(4), name VARCHAR(16), author VARCHAR(16), date BIGINT, expire BIGINT, reason VARCHAR(255))")) {
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Create history
        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("CREATE TABLE IF NOT EXISTS " + HISTORY_TABLE + " " +
                "(id INT AUTO_INCREMENT PRIMARY KEY, type VARCHAR(4), name VARCHAR(16), author VARCHAR(16), date BIGINT, expire BIGINT, reason VARCHAR(255))")) {
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void stop() {
        executor.shutdown();

        plugin.getLogger().info("Waiting for punishment-thread to end all tasks...");
        try {
            if (executor.awaitTermination(5, TimeUnit.SECONDS)) {
                plugin.getLogger().info("All tasks completed for punishment-thread");
            } else {
                plugin.getLogger().info("Timed out for punishment-thread");
            }
        } catch (Exception e) {
            e.printStackTrace();
            plugin.getLogger().info("An error occured while completing tasks for punishment-thread");
        }

        dataSource.close();
    }

    public void execute(Runnable runnable) {
        if (Thread.currentThread().getName().equals("punishment-thread")) runnable.run();
        else executor.execute(() -> {
            try {
                runnable.run();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public Connection connection() throws SQLException {
        return dataSource.getConnection();
    }


}
