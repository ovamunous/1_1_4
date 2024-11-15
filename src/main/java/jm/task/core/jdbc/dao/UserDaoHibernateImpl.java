package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.hibernate.HibernateException;
import org.hibernate.Session;

import java.util.ArrayList;
import java.util.List;

public class UserDaoHibernateImpl implements UserDao {
    public UserDaoHibernateImpl() {

    }


    @Override
    public void createUsersTable() {
        try (Session session = Util.getSessionFactory().openSession()) {
            session.beginTransaction();
            session.createSQLQuery("CREATE TABLE IF NOT EXISTS user ("
                    + "`id` bigint NOT NULL AUTO_INCREMENT,"
                    + "`name` VARCHAR(255),"
                    + "`lastName` VARCHAR(255),"
                    + "`age` tinyint,"
                    + "PRIMARY KEY (`id`));").executeUpdate();
            session.getTransaction().commit();
        } catch (HibernateException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void dropUsersTable() {
        try (Session session = Util.getSessionFactory().openSession()) {
            session.beginTransaction();
            session.createSQLQuery("DROP TABLE if exists user").executeUpdate();
            session.getTransaction().commit();
        } catch (HibernateException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        try (Session session = Util.getSessionFactory().openSession()) {
           try {
               session.beginTransaction();
               User user = new User(name, lastName, age);
               session.save(user);
               System.out.println(user);
               session.getTransaction().commit();
           } catch (HibernateException e) {
               session.getTransaction().rollback();
           }
        } catch (HibernateException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeUserById(long id) {
        try (Session session = Util.getSessionFactory().openSession()) {
            try {
                session.beginTransaction();
                if (session.get(User.class, id) != null) {
                    session.delete(session.get(User.class, id));
                    session.getTransaction().commit();
                }
            } catch (HibernateException e) {
                session.getTransaction().rollback();
            }
        } catch (HibernateException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<User> getAllUsers() {
        List<User> users = new ArrayList<User>();
        try (Session session = Util.getSessionFactory().openSession()) {
            users = session.createQuery("from User").list();
            users.forEach(System.out::println);
        } catch (HibernateException e) {
            e.printStackTrace();
        }
        return users;
    }

    @Override
    public void cleanUsersTable() {
        try (Session session = Util.getSessionFactory().openSession()) {
            session.beginTransaction();
            session.createSQLQuery("TRUNCATE user").executeUpdate();
            session.getTransaction().commit();
        } catch (HibernateException e) {
            e.printStackTrace();
        }
    }
}
