package simpledb.jdbc.network;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author sanling
 * @date 2020/11/11
 */
public class NetworkDataSource implements DataSource {

    Properties p = null;

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public Connection getConnection(String url, String password) throws SQLException {
        try {
            NetworkDriver d = new NetworkDriver();
            System.out.println("已连接到NetworkDriver");
            return d.connect(url, null);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override

    public java.io.PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(java.io.PrintWriter out) throws SQLException {

    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {

    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
