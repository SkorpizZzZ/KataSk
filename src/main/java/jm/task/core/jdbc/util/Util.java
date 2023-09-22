package jm.task.core.jdbc.util;

import jm.task.core.jdbc.model.User;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.service.ServiceRegistry;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Util {
    private static Util instance;
    private static final String URL = "jdbc:mysql://localhost:3306/mydbkata";
    private static final String USER = "root";
    private static final String PASSWORD = "root";
    private static final String JDBCDRIVER = "com.mysql.cj.jdbc.Driver";
    private static SessionFactory sessionFactory;

    private Util() {
    }


    public static SessionFactory getSessionFactory() {
        if (sessionFactory == null || sessionFactory.isClosed()) {
            try {
                Configuration configuration = new Configuration();
                Properties settings = new Properties();
                settings.put(Environment.DRIVER, JDBCDRIVER);
                settings.put(Environment.URL, URL);
                settings.put(Environment.USER, USER);
                settings.put(Environment.PASS, PASSWORD);
                settings.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
                settings.put(Environment.SHOW_SQL, true);
                settings.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
                settings.put(Environment.HBM2DDL_AUTO, "");
                configuration.setProperties(settings);
                configuration.addAnnotatedClass(User.class);
                ServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder()
                        .applySettings(configuration.getProperties()).build();
                sessionFactory = configuration.buildSessionFactory(serviceRegistry);
            } catch (Exception e) {
                System.out.println("getSessionFactory failed...");
            }
        }
        return sessionFactory;
    }

    public Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName(JDBCDRIVER);
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
            System.out.println("Connection OK");
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("Connection ERROR");
        }
        return connection;
    }

    public static Util getInstance() {
        Util localInstance = instance;
        if (localInstance == null) {
            synchronized (Util.class) {
                localInstance = instance;
                if (localInstance == null) {
                    instance = localInstance = new Util();
                }
            }
        }
        return localInstance;
    }

}
