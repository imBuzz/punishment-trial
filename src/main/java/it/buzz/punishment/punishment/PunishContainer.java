package it.buzz.punishment.punishment;

import it.buzz.punishment.Punishment;
import it.buzz.punishment.data.MySQLConnector;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.metadata.FixedMetadataValue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

@RequiredArgsConstructor
@Builder
public class PunishContainer {

    public final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd/MM/yyyy HH:mm");

    private final PunishType type;
    private final String name;
    private final CommandSender author;
    private final long start, end;
    private final String reason;

    public void execute(Punishment plugin) {
        Player player = Bukkit.getPlayerExact(name);
        String endingDate = end == -1 ? "Permanently" : DATE_FORMAT.format(new Date(end));

        MySQLConnector connector = plugin.getConnector();
        connector.execute(() -> {
            Exception exception = null;

            try (Connection connection = connector.connection(); PreparedStatement statement =
                    connection.prepareStatement("INSERT INTO " + MySQLConnector.ACTIVE_TABLE +
                            " (type, name, author, date, expire, reason) values (?, ?, ?, ?, ?, ?);")) {


                statement.setString(1, type.toString());
                statement.setString(2, name);
                statement.setString(3, author.getName());
                statement.setLong(4, start);
                statement.setLong(5, end);
                statement.setString(6, reason);

                statement.execute();
            } catch (SQLException e) {
                exception = e;
                e.printStackTrace();
            }


            try (Connection connection = connector.connection(); PreparedStatement statement =
                    connection.prepareStatement("INSERT INTO " + MySQLConnector.HISTORY_TABLE +
                            " (type, name, author, date, expire, reason) values (?, ?, ?, ?, ?, ?);")) {


                statement.setString(1, type.toString());
                statement.setString(2, name);
                statement.setString(3, author.getName());
                statement.setLong(4, start);
                statement.setLong(5, end);
                statement.setString(6, reason);

                statement.execute();

            } catch (SQLException e) {
                exception = e;
                e.printStackTrace();
            }

            if (exception == null) {
                Bukkit.getScheduler().runTaskLater(plugin, () -> {
                    switch (type) {
                        case BAN -> {
                            if (player != null) {
                                player.kickPlayer(ChatColor.RED + "You have been banned by " +
                                        author.getName() + " for " + reason + " expire at " + endingDate);
                            }

                            author.sendMessage(ChatColor.GREEN + "You have banned " + name + " until " + endingDate + " for " + reason);
                        }
                        case MUTE -> {
                            if (player != null) {
                                player.setMetadata("punish-mute", new FixedMetadataValue(plugin, end));
                                player.sendMessage(ChatColor.RED + "You have been muted by " +
                                        author.getName() + " for " + reason + " expire at " + endingDate);
                            }
                            author.sendMessage(ChatColor.GREEN + "You have muted " + name + " until " + endingDate + " for " + reason);
                        }
                        case KICK -> {
                            if (player != null) {
                                player.kickPlayer(ChatColor.RED + "You have been kicked by " +
                                        author.getName() + " for " + reason);
                            }
                            author.sendMessage(ChatColor.GREEN + "You have kicked " + name + " for " + reason);
                        }
                    }
                }, 1L);
            } else {
                author.sendMessage(ChatColor.RED + "An error occurred while performing this task (" + exception.getLocalizedMessage() + ")");
            }
        });

    }

    public enum PunishType {
        BAN, MUTE, KICK
    }

}
