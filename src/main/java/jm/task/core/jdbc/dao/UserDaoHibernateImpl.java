package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.hibernate.Session;

import java.util.List;

public class UserDaoHibernateImpl implements UserDao {
    public UserDaoHibernateImpl() {

    }


    @Override
    public void createUsersTable() {
        Session session = Util.getSessionFactory().openSession();
        session.beginTransaction();
        session.createSQLQuery("CREATE TABLE IF NOT EXISTS user ("
                + "`id` bigint NOT NULL AUTO_INCREMENT,"
                + "`name` VARCHAR(255),"
                + "`lastName` VARCHAR(255),"
                + "`age` tinyint,"
                + "PRIMARY KEY (`id`));").executeUpdate();
        session.getTransaction().commit();
        session.close();
    }

    @Override
    public void dropUsersTable() {
        Session session = Util.getSessionFactory().openSession();
        session.beginTransaction();
        session.createSQLQuery("DROP TABLE if exists user").executeUpdate();
        session.getTransaction().commit();
        session.close();
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        Session session = Util.getSessionFactory().openSession();
        session.beginTransaction();
        User user = new User(name, lastName, age);
        session.save(user);
        System.out.println(user);
        session.getTransaction().commit();
        session.close();
    }

    @Override
    public void removeUserById(long id) {
        Session session = Util.getSessionFactory().openSession();
        session.beginTransaction();
        session.delete(session.get(User.class, id));
        session.getTransaction().commit();
        session.close();
    }

    @Override
    public List<User> getAllUsers() {
        try (Session session = Util.getSessionFactory().openSession()) {
            List<User> users = session.createQuery("from User").list();
            users.forEach(System.out::println);
            return users;
        }
    }

    @Override
    public void cleanUsersTable() {
        Session session = Util.getSessionFactory().openSession();
        session.beginTransaction();
        session.createSQLQuery("TRUNCATE user").executeUpdate();
        session.getTransaction().commit();
        session.close();
    }
}
