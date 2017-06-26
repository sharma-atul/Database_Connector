package connector;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class DBConnector {
	//the connection to database
	private Connection connection = null;
	//properties file with connection details.
	private Properties dbConnectionProps;
	
	/**
	 * 
	 * @param type
	 * @throws DBConnectorException
	 *  
	 */
	public DBConnector(DBType type) throws DBConnectorException{
		dbConnectionProps = loadPropertiesFile();
		
		System.out.println("-------- JDBC Connection Testing ------------");
		switch(type){
		case POSTGRESQL: 
			setConnection(initPostgresConnection());
		case MYSQL: 
			setConnection(initMySQLConnection());
		case MSSQL:
			setConnection(initMsSQLConnection());
		default: 
			throw new DBConnectorException("Could not initialize connection to db of type" + type.toString());
		}

	}

	private Properties loadPropertiesFile() throws DBConnectorException{
		FileInputStream input = null;
		try {
			 input = new FileInputStream("db.properties");
			// load a properties file
			dbConnectionProps.load(input);
			return dbConnectionProps;

		} catch (IOException ex) {
			throw new DBConnectorException("Could not load properties file:" +
					"\n"+ " Due to " + ex.getMessage() );
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					throw new DBConnectorException("Could not close properties file:" +
							"\n"+ " Due to " + e.getMessage() );
				}
			}
		}
	}
	
	public ResultSet runSql(String sql) throws SQLException, DBConnectorException {
		if(connection == null){
			throw new DBConnectorException("No Connection to run statement.");
		}
		Statement sta = connection.createStatement();
		return sta.executeQuery(sql);
	}
 
	@Override
	protected void finalize() throws Throwable {
		if (connection != null && !connection.isClosed()) {
			connection.close();
		}
	}
	
	/**
	 * PRIVATE METHODS
	 * 
	 */
	private Connection initMsSQLConnection() throws DBConnectorException{
		return initConnection("MSSQL","com.microsoft.sqlserver.jdbc.SQLServerDriver");
	}

	private Connection initMySQLConnection() throws DBConnectorException{
		return initConnection("MySQL","com.mysql.jdbc.Driver");
	}

	public Connection initPostgresConnection()throws DBConnectorException{
		return initConnection("PostgreSQL","org.postgresql.Driver");
	}

	public Connection initConnection(String dbName, String driverName) throws DBConnectorException{
		Connection ret = null;
		try {

			Class.forName(driverName);

		} catch (ClassNotFoundException e) {
			System.out.println(dbName + " JDBC Driver missing on classpath.");
			throw new DBConnectorException(dbName + " JDBC Driver missing on classpath.");
		}

		System.out.println(dbName + " JDBC Driver Registered!");
		try{
			String url = "jdbc:postgresql://" + dbConnectionProps.getProperty("postgres.host") +
						":"  + dbConnectionProps.getProperty("postgres.port") +
						"/" + dbConnectionProps.getProperty("postgres.db") ;
						
			ret = DriverManager.getConnection(url, dbConnectionProps.getProperty("postgres.user"),
					dbConnectionProps.getProperty("postgres.password"));

		} catch (SQLException e) {

			System.out.println(dbName + "Connection Failed! Check output console");
			throw new DBConnectorException(dbName + 
					"Connection Failed! Check output console" +
					"\n" + " Due to " + e.getMessage());
		}

		
		if (ret != null) {
			System.out.println("You made it, take control your " + dbName + " database now!");
			return ret;
		} 
		
		throw new DBConnectorException("Failed to make connection!");
	}
	
	private void setConnection(Connection conn) {
		this.connection = conn;
	}

}
