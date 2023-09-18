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
        String sqlCommand = "CREATE TABLE IF NOT EXISTS USERS (Id BIGINT PRIMARY KEY AUTO_INCREMENT, NAME VARCHAR(40), LASTNAME VARCHAR(40), AGE TINYINT UNSIGNED)";
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sqlCommand);
        } catch (SQLException e) {
            System.out.println("createUsersTable failed...");
        }
    }

    public void dropUsersTable() {
        String sqlCommand = "DROP TABLE IF EXISTS USERS";
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sqlCommand);
        } catch (SQLException e) {
            System.out.println("dropUsersTable failed...");
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        String sql = "INSERT INTO USERS (NAME, LASTNAME, AGE) VALUES ( ?, ?, ?)";
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = Util.getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            System.out.println("saveUser failed...");
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException rollbackException) {
                System.out.println("Rollback failed...");
                rollbackException.printStackTrace();
            } finally {
                try {
                    if (preparedStatement != null) {
                        preparedStatement.close();
                    }
                    if (connection != null) {
                        connection.close();
                    }
                } catch (SQLException closeException) {
                    System.out.println("Closing failed...");
                    closeException.printStackTrace();
                }
            }
        }
    }

    public void removeUserById(long id) {
        String sqlCommand = String.format("DELETE FROM USERS WHERE Id = %d", id);
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sqlCommand);
        } catch (SQLException e) {
            System.out.println("removeUserById failed...");
        }
    }

    public List<User> getAllUsers() {
        ArrayList<User> usersList = new ArrayList<>();
        String sql = "SELECT * FROM USERS";
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("ID"));
                user.setName(resultSet.getString("NAME"));
                user.setLastName(resultSet.getString("LASTNAME"));
                user.setAge(resultSet.getByte("AGE"));
                usersList.add(user);
            }
        } catch (SQLException e) {
            System.out.println("getAllUsers failed...");
            e.printStackTrace();
        }
        return usersList;
    }

    public void cleanUsersTable() {
        String sqlCommand = "DELETE FROM USERS";
        try (Connection connection = Util.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sqlCommand)) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.out.println("cleanUsersTable failed...");
        }
    }
}
