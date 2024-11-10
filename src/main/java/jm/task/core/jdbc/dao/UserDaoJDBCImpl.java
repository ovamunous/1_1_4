package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE user ("
                    + "`id` INT NOT NULL AUTO_INCREMENT,"
                    + "`name` VARCHAR(45) NOT NULL,"
                    + "`lastName` VARCHAR(45) NOT NULL,"
                    + "`age` INT(3) NOT NULL,"
                    + "PRIMARY KEY (`id`));");
        } catch (SQLException _) {

        }
    }

    public void dropUsersTable() {
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("Drop table user");
        } catch (SQLException _) {

        }

    }

    public void saveUser(String name, String lastName, byte age) {
        String sql = "INSERT INTO user (name, lastName, age) VALUES (?, ?, ?)";
        try (Connection connection = Util.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setInt(3, age);
            preparedStatement.executeUpdate();
            System.out.println("User с именем - " + name + " добавлен в базу данных");;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void removeUserById(long id) {
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from user where id = " + id);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT * FROM user");
            while (rs.next()) {
                users.add(new User(rs.getString("name"),
                        rs.getString("lastName"),
                        rs.getByte("age") ));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return users;
    }

    public void cleanUsersTable() {
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("truncate user");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
