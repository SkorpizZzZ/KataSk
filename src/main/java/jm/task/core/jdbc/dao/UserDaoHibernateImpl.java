package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.query.Query;

import java.util.List;

public class UserDaoHibernateImpl implements UserDao {
    public UserDaoHibernateImpl() {

    }


    @Override
    public void createUsersTable() {
        Transaction transaction = null;
        Session session = null;
        try {
            session = Util.getInstance().getSessionFactory().openSession();
            transaction = session.beginTransaction();
            String sql = "CREATE TABLE IF NOT EXISTS users (Id BIGINT PRIMARY KEY AUTO_INCREMENT, NAME VARCHAR(40), LASTNAME VARCHAR(40), AGE TINYINT UNSIGNED)";
            session.createSQLQuery(sql).addEntity(User.class).executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
                System.out.println("createUsersTable failed...");
            }
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    @Override
    public void dropUsersTable() {
        Transaction transaction = null;
        Session session = null;
        try {
            session = Util.getInstance().getSessionFactory().openSession();
            transaction = session.beginTransaction();
            String sql = "DROP TABLE IF EXISTS users";
            session.createSQLQuery(sql).addEntity(User.class).executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
                System.out.println("dropUsersTable failed...");
            }
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }


    @Override
    public void saveUser(String name, String lastName, byte age) {
        Transaction transaction = null;
        Session session = null;
        try {
            session = Util.getInstance().getSessionFactory().openSession();
            transaction = session.beginTransaction();
            session.save(new User(name, lastName, age));
            transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
            if (transaction != null) {
                transaction.rollback();
                System.out.println("saveUser failed...");
            }
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    @Override
    public void removeUserById(long id) {
        Transaction transaction = null;
        Session session = null;
        try {
            session = Util.getInstance().getSessionFactory().openSession();
            transaction = session.beginTransaction();
            session.delete(session.get(User.class, id));
            transaction.commit();
        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
                System.out.println("removeUserById failed...");
            }
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    @Override
    public List<User> getAllUsers() {
        Transaction transaction = null;
        List <User> users = null;
        Session session = null;
        try {
            session = Util.getInstance().getSessionFactory().openSession();
            transaction = session.beginTransaction();
            Query <User> query = session.createQuery("FROM User");
            users = query.list();
            transaction.commit();
        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
                System.out.println("getAllUsers failed...");
            }
        } finally {
            if (session != null) {
                session.close();
            }
        }
        return users;
    }

    @Override
    public void cleanUsersTable() {
        Transaction transaction = null;
        Session session = null;
        try {
            session = Util.getInstance().getSessionFactory().openSession();
            transaction = session.beginTransaction();
            session.createSQLQuery("DELETE FROM users").executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
                System.out.println("cleanUsersTable failed...");
            }
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }
}
