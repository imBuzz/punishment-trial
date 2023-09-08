package it.buzz.punishment;

import it.buzz.punishment.data.MySQLConnector;
import it.buzz.punishment.punishment.PunishContainer;
import lombok.Getter;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerChatEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.metadata.FixedMetadataValue;
import org.bukkit.plugin.java.JavaPlugin;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public final class Punishment extends JavaPlugin implements Listener {

    @Getter
    private MySQLConnector connector;

    @Override
    public void onEnable() {
        connector = new MySQLConnector(this);
        connector.init();

        registerCommand("ban", (sender, args) -> {
            if (!sender.hasPermission("punishment.ban")) {
                sender.sendMessage(ChatColor.RED + "You don't have the permission to do that");
                return;
            }

            long end = -1;
            String reason = "Unfair Advantage";

            //ban <player> <time> <reason>
            if (args.length >= 2) {
                String[] timeUnits = args[1].split("(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)");
                if (timeUnits.length != 2) {
                    sender.sendMessage(ChatColor.RED + "Usage: /ban <player> <time> <reason>");
                    return;
                }

                long time;
                switch (timeUnits[1]) {
                    case "d" -> {
                        try {
                            time = TimeUnit.DAYS.toMillis(Integer.valueOf(timeUnits[0]));
                        } catch (Exception e) {
                            sender.sendMessage(ChatColor.RED + "Usage: /ban <player> <time> <reason>");
                            return;
                        }
                    }
                    case "m" -> {
                        try {
                            time = TimeUnit.MINUTES.toMillis(Integer.valueOf(timeUnits[0]));
                        } catch (Exception e) {
                            sender.sendMessage(ChatColor.RED + "Usage: /ban <player> <time> <reason>");
                            return;
                        }
                    }
                    case "s" -> {
                        try {
                            time = TimeUnit.SECONDS.toMillis(Integer.valueOf(timeUnits[0]));
                        } catch (Exception e) {
                            sender.sendMessage(ChatColor.RED + "Usage: /ban <player> <time> <reason>");
                            return;
                        }
                    }
                    default -> {
                        sender.sendMessage(ChatColor.RED + "Usage: /ban <player> <time> <reason>");
                        return;
                    }
                }

                end = time + System.currentTimeMillis();

                if (args.length >= 3) {
                    StringBuilder builder = new StringBuilder();
                    for (int i = 2; i < args.length; i++)
                        builder.append(args[i]).append(" ");

                    reason = builder.toString();
                }
            }

            PunishContainer.builder()
                    .type(PunishContainer.PunishType.BAN)
                    .name(args[0])
                    .author(sender)
                    .start(System.currentTimeMillis())
                    .end(end)
                    .reason(reason)
                    .build()
                    .execute(this);
        });
        registerCommand("mute", (sender, args) -> {
            if (!sender.hasPermission("punishment.mute")) {
                sender.sendMessage(ChatColor.RED + "You don't have the permission to do that");
                return;
            }

            long end = -1;
            String reason = "Unfair Advantage";

            //mute <player> <time> <reason>
            if (args.length >= 2) {
                String[] timeUnits = args[1].split("(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)");
                if (timeUnits.length != 2) {
                    sender.sendMessage(ChatColor.RED + "Usage: /ban <player> <time> <reason>");
                    return;
                }

                long time;
                switch (timeUnits[1]) {
                    case "d" -> {
                        try {
                            time = TimeUnit.DAYS.toMillis(Integer.valueOf(timeUnits[0]));
                        } catch (Exception e) {
                            sender.sendMessage(ChatColor.RED + "Usage: /ban <player> <time> <reason>");
                            return;
                        }
                    }
                    case "m" -> {
                        try {
                            time = TimeUnit.MINUTES.toMillis(Integer.valueOf(timeUnits[0]));
                        } catch (Exception e) {
                            sender.sendMessage(ChatColor.RED + "Usage: /ban <player> <time> <reason>");
                            return;
                        }
                    }
                    case "s" -> {
                        try {
                            time = TimeUnit.SECONDS.toMillis(Integer.valueOf(timeUnits[0]));
                        } catch (Exception e) {
                            sender.sendMessage(ChatColor.RED + "Usage: /ban <player> <time> <reason>");
                            return;
                        }
                    }
                    default -> {
                        sender.sendMessage(ChatColor.RED + "Usage: /ban <player> <time> <reason>");
                        return;
                    }
                }

                end = time + System.currentTimeMillis();

                if (args.length >= 3) {
                    StringBuilder builder = new StringBuilder();
                    for (int i = 2; i < args.length; i++)
                        builder.append(args[i]).append(" ");

                    reason = builder.toString();
                }
            }

            PunishContainer.builder()
                    .type(PunishContainer.PunishType.MUTE)
                    .name(args[0])
                    .author(sender)
                    .start(System.currentTimeMillis())
                    .end(end)
                    .reason(reason)
                    .build()
                    .execute(this);
        });
        registerCommand("kick", (sender, args) -> {
            if (!sender.hasPermission("punishment.unmute")) {
                sender.sendMessage(ChatColor.RED + "You don't have the permission to do that");
                return;
            }
            if (args.length < 1) {
                sender.sendMessage(ChatColor.RED + "Invalid command");
                return;
            }

            connector.execute(() -> {
                StringBuilder reason = new StringBuilder();
                for (int i = 3; i < args.length; i++)
                    reason.append(args[i]).append(" ");

                PunishContainer.builder()
                        .type(PunishContainer.PunishType.KICK)
                        .name(args[0])
                        .author(sender)
                        .end(-1)
                        .reason(reason.toString())
                        .build()
                        .execute(this);
            });
        });

        registerCommand("unban", (sender, args) -> {
            if (!sender.hasPermission("punishment.unban")) {
                sender.sendMessage(ChatColor.RED + "You don't have the permission to do that");
                return;
            }
            if (args.length < 1) {
                sender.sendMessage(ChatColor.RED + "Usage /unban <player>");
                return;
            }

            final String targetName = args[0];
            connector.execute(() -> {
                try (Connection connection = connector.connection(); PreparedStatement statement =
                        connection.prepareStatement("DELETE FROM " + MySQLConnector.ACTIVE_TABLE +
                                " WHERE name= '" + targetName + "' AND type='BAN'")) {

                    int rows = statement.executeUpdate();
                    if (rows == 0) {
                        sender.sendMessage(ChatColor.RED + "This player is not banned!");
                    } else {
                        sender.sendMessage(ChatColor.GREEN + "Player unbanned!");
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        });
        registerCommand("unmute", (sender, args) -> {
            if (!sender.hasPermission("punishment.unmute")) {
                sender.sendMessage(ChatColor.RED + "You don't have the permission to do that");
                return;
            }
            if (args.length < 1) {
                sender.sendMessage(ChatColor.RED + "Usage /unmute <player>");
                return;
            }

            final String targetName = args[0];
            connector.execute(() -> {
                try (Connection connection = connector.connection(); PreparedStatement statement =
                        connection.prepareStatement("DELETE FROM " + MySQLConnector.ACTIVE_TABLE +
                                " WHERE name= '" + targetName + "' AND type='MUTE'")) {

                    int rows = statement.executeUpdate();
                    if (rows == 0) {
                        sender.sendMessage(ChatColor.RED + "This player is not muted!");
                    } else {
                        Bukkit.getScheduler().runTaskLater(this, () -> {
                            Player player = Bukkit.getPlayerExact(args[0]);
                            if (player != null) {
                                player.removeMetadata("punish-mute", this);
                                player.sendMessage(ChatColor.GREEN + "You have been unmuted");
                            }

                            sender.sendMessage(ChatColor.GREEN + "Player unmuted!");
                        }, 1L);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            });
        });

        registerCommand("history", (sender, args) -> {
            if (!sender.hasPermission("punishment.history")) {
                sender.sendMessage(ChatColor.RED + "You don't have the permission to do that");
                return;
            }
            if (args.length < 1) {
                sender.sendMessage(ChatColor.RED + "Usage /history <player>");
                return;
            }

            final String targetName = args[0];
            connector.execute(() -> {
                sender.sendMessage(ChatColor.RED + "Loading History of " + args[0] + " limit (10):");
                sender.sendMessage(" ");

                try (Connection connection = connector.connection(); PreparedStatement statement =
                        connection.prepareStatement("SELECT * FROM " + MySQLConnector.HISTORY_TABLE +
                                " WHERE name= '" + targetName + "' LIMIT 10")) {

                    try (ResultSet set = statement.executeQuery()) {
                        while (set.next()) {
                            String endingDate = set.getLong(6) == -1 ? "Permanently" : PunishContainer.DATE_FORMAT.format(new Date(set.getLong(6)));

                            // * (H) X was banned by Y for R until DATE
                            sender.sendMessage(ChatColor.RED + "* (%type%) %victim% was punished by %author% for %reason% until %expire%"
                                    .replace("%type%", set.getString(2))
                                    .replace("%victim%", set.getString(3))
                                    .replace("%author%", set.getString(4))
                                    .replace("%reason%", set.getString(7))
                                    .replace("%expire%", endingDate));
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        });

        registerCommand("checkban", (sender, args) -> {
            if (!sender.hasPermission("punishment.checkban")) {
                sender.sendMessage(ChatColor.RED + "You don't have the permission to do that");
                return;
            }
            if (args.length < 1) {
                sender.sendMessage(ChatColor.RED + "Usage /checkban <player>");
                return;
            }

            final String playerName = args[0];

            //check if he's banned
            connector.execute(() -> {
                try (Connection connection = connector.connection(); PreparedStatement statement =
                        connection.prepareStatement("SELECT type, author, expire, reason, id FROM " + MySQLConnector.ACTIVE_TABLE +
                                " WHERE name= '" + playerName + "' AND type='BAN' ORDER BY expire DESC LIMIT 1")) {
                    try (ResultSet set = statement.executeQuery()) {
                        if (set.next()) {
                            long end = set.getLong(3);
                            if (System.currentTimeMillis() < end) {
                                final String endingDate = end == -1 ? "Permanently" : PunishContainer.DATE_FORMAT.format(new Date(end));
                                final String author = set.getString(2), reason = set.getString(4);
                                sender.sendMessage(ChatColor.RED + args[0] + "is banned for " + reason + " expire at " + endingDate + " by " + author);
                            }
                        } else {
                            sender.sendMessage(ChatColor.GREEN + "This player is not banned");
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        });

        registerCommand("checkmute", (sender, args) -> {
            if (!sender.hasPermission("punishment.checkmute")) {
                sender.sendMessage(ChatColor.RED + "You don't have the permission to do that");
                return;
            }
            if (args.length < 1) {
                sender.sendMessage(ChatColor.RED + "Usage /checkmute <player>");
                return;
            }

            final String playerName = args[0];

            //check if he's muted
            connector.execute(() -> {
                try (Connection connection = connector.connection(); PreparedStatement statement =
                        connection.prepareStatement("SELECT type, author, expire, reason, id FROM " + MySQLConnector.ACTIVE_TABLE +
                                " WHERE name= '" + playerName + "' AND type='MUTE' ORDER BY expire DESC LIMIT 1")) {
                    try (ResultSet set = statement.executeQuery()) {
                        if (set.next()) {
                            long end = set.getLong(3);
                            if (System.currentTimeMillis() < end) {
                                final String endingDate = end == -1 ? "Permanently" : PunishContainer.DATE_FORMAT.format(new Date(end));
                                final String author = set.getString(2), reason = set.getString(4);
                                sender.sendMessage(args[0] + "is muted for " + reason + " expire at " + endingDate + " by " + author);
                            }
                        } else {
                            sender.sendMessage(ChatColor.GREEN + "This player is not muted");
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        });

        Bukkit.getPluginManager().registerEvents(this, this);
    }

    @Override
    public void onDisable() {
        connector.stop();
    }

    private void registerCommand(String command, BiConsumer<CommandSender, String[]> executor) {
        getCommand(command).setExecutor((sender, command1, label, args) -> {
            executor.accept(sender, args);
            return true;
        });
    }

    @EventHandler(priority = EventPriority.MONITOR)
    private void joinEvent(PlayerJoinEvent event) {
        final String playerName = event.getPlayer().getName();

        //check if he's banned
        connector.execute(() -> {
            try (Connection connection = connector.connection(); PreparedStatement statement =
                    connection.prepareStatement("SELECT type, author, expire, reason, id FROM " + MySQLConnector.ACTIVE_TABLE +
                            " WHERE name= '" + playerName + "' AND type='BAN' ORDER BY expire DESC LIMIT 1")) {
                try (ResultSet set = statement.executeQuery()) {
                    if (set.next()) {
                        int id = set.getInt(5);

                        long end = set.getLong(3);
                        if (System.currentTimeMillis() < end) {
                            final String endingDate = end == -1 ? "Permanently" : PunishContainer.DATE_FORMAT.format(new Date(end));
                            final String author = set.getString(2), reason = set.getString(4);

                            Bukkit.getScheduler().runTaskLater(this, () -> event.getPlayer().kickPlayer(ChatColor.RED + "You have been banned by " + author
                                    + " for " + reason + " expire at " + endingDate), 2L);
                        } else {
                            try (PreparedStatement statement1 = connection.prepareStatement("DELETE FROM " + MySQLConnector.ACTIVE_TABLE + " WHERE id= '" + id)) {
                                statement1.executeUpdate();
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

        //check if he's muted
        connector.execute(() -> {
            try (Connection connection = connector.connection(); PreparedStatement statement =
                    connection.prepareStatement("SELECT type, author, expire, reason, id FROM " + MySQLConnector.ACTIVE_TABLE +
                            " WHERE name= '" + playerName + "' AND type='MUTE' ORDER BY expire DESC LIMIT 1")) {
                try (ResultSet set = statement.executeQuery()) {
                    if (set.next()) {
                        int id = set.getInt(5);

                        long end = set.getLong(3);
                        if (System.currentTimeMillis() < end) {
                            Bukkit.getScheduler().runTaskLater(this, () -> event.getPlayer()
                                    .setMetadata("punish-mute", new FixedMetadataValue(this, end)), 2L);
                        } else {
                            try (PreparedStatement statement1 = connection.prepareStatement("DELETE FROM " + MySQLConnector.ACTIVE_TABLE + " WHERE id= '" + id)) {
                                statement1.executeUpdate();
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @EventHandler(priority = EventPriority.MONITOR)
    private void chatEvent(AsyncPlayerChatEvent event) {
        Player player = event.getPlayer();
        if (!player.hasMetadata("punish-mute")) return;
        event.setCancelled(true);

        long end = player.getMetadata("punish-mute").get(0).asLong();
        if (System.currentTimeMillis() >= end) {
            player.removeMetadata("punish-mute", this);
            event.setCancelled(false);
            return;
        }

        String endingDate = end == -1 ? "Permanently" : PunishContainer.DATE_FORMAT.format(new Date(end));
        player.sendMessage(ChatColor.RED + "You have been muted until " + endingDate);
    }

}
