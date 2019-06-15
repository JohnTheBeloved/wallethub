import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class MySqlUtil {
    
    
    public String url;
    public String database;
    public String user;
    public String password;
    public String logLineTableName;
    public String blockedIPTableName;
    Connection connection;

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private final String CREATE_DATABASE; 
    private final String CREATE_LOG_LINE_TABLE;
    private final String CREATE_BLOCKED_IP_TABLE;
    private final String INSERT_LOG_LINE_TABLE;
    private final String INSERT_BLOCKED_IP_TABLE;


    public MySqlUtil(Properties properties){
        this.url = properties.getProperty("url") != null ? properties.getProperty("url") : "";
        this.user = properties.getProperty("user") != null ? properties.getProperty("user") : "root";
        this.password = properties.getProperty("password") != null ? properties.getProperty("password") : "password";
        this.database = properties.getProperty("database") != null ? properties.getProperty("database") : "access_log";
        this.logLineTableName = properties.getProperty("table.log_line") != null ? properties.getProperty("table.log_line") : "log_line";
        this.blockedIPTableName = properties.getProperty("table.blocked_ip") != null ? properties.getProperty("table.blocked_ip") : "blocked_ip";
        CREATE_DATABASE = "CREATE DATABASE " + this.database;
        CREATE_LOG_LINE_TABLE = "CREATE TABLE " + this.logLineTableName + " (id INT(64) NOT NULL AUTO_INCREMENT,"
        + " log_time DATE, ip_address VARCHAR(20), request VARCHAR(100), status INT(64), user_agent VARCHAR(200), PRIMARY KEY (id));";
        CREATE_BLOCKED_IP_TABLE = "CREATE TABLE " + this.blockedIPTableName + " (id INT(64) NOT NULL AUTO_INCREMENT,"
        + "ip_address VARCHAR(20), comment VARCHAR(200), PRIMARY KEY (id));";
        INSERT_LOG_LINE_TABLE = "INSERT INTO "+ this.logLineTableName +" ( log_time, ip_address, request, status, user_agent ) VALUES(?, ?, ?, ?, ? )";
        INSERT_BLOCKED_IP_TABLE = "INSERT INTO "+ this.blockedIPTableName  +" ( ip_address, comment ) VALUES(?, ?)";
    }

    public static void loadClass()  throws InstantiationException, IllegalAccessException, ClassNotFoundException{
        Class.forName(JDBC_DRIVER).newInstance();
    }

    public Connection connect()  throws SQLException{
        return connect(true);
    }
    public Connection connect(boolean withDB) throws SQLException {
        String url = (withDB ? (this.url + "/" + database) :  this.url) + "?useLegacyDatetimeCode=false&serverTimezone=GMT";
        Connection newConnection = DriverManager.getConnection(url, user, password);
        connection = newConnection;
        return connection;
    }

    public boolean initDatabase() {
        try {
            if (dbExists())
            return true;
            Statement statement = connection.createStatement();
            int result = statement.executeUpdate(CREATE_DATABASE);
            return result == 1;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        
    }
    public boolean initTables() {
        try {
            if(!tableExists(logLineTableName)) {
                Statement statement = connection.createStatement();
                statement.executeUpdate(CREATE_LOG_LINE_TABLE);
            }
            if(!tableExists(blockedIPTableName)) {
                Statement statement = connection.createStatement();
                statement.executeUpdate(CREATE_BLOCKED_IP_TABLE);
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }
    
    private boolean dbExists()  throws SQLException{
        ResultSet resultSet = connection.getMetaData().getCatalogs();
        while (resultSet.next()) {
            String databaseName = resultSet.getString(1);
            if(databaseName.equals(database)){
                resultSet.close();
                return true;
            }
        }
        return false;
    }

    private boolean tableExists(String tableName)  throws SQLException{
        DatabaseMetaData dbm = connection.getMetaData();
        // check if logTable table is there
        ResultSet resultSet = dbm.getTables(null, null, tableName , null);
        while(resultSet.next()){
                if (resultSet != null && resultSet.getString("TABLE_NAME").equals(tableName)) {
                    resultSet.close();
                    return true;
                }
        }
       return false;
    }

    public boolean insertLogLine(LogLine logLine) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(INSERT_LOG_LINE_TABLE);
        statement.setDate(1, new Date(logLine.getDate().getTime()));
        statement.setString(2, logLine.getIP());
        statement.setString(3, logLine.getRequest());
        statement.setInt(4, logLine.getStatus());
        statement.setString(5, logLine.getUserAgent());
        int result = statement.executeUpdate();
        return result == 1;
    }

    public boolean insertBlockedIP(String IP, String comment) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(INSERT_BLOCKED_IP_TABLE);
        statement.setString(1, IP);
        statement.setString(2, comment);
        int result = statement.executeUpdate();
        return result == 1;
    }

}
