/*analyze schema data to create setup database. This can be edited by the user before learning.
  If setup = 0, we skip this step and use the existing setup database
  Yan Sept 10th*/


//Assumption: 
  // No suffix or *_std refers to the original database provided to FactorBase
  // *_setup is the setup database



import java.sql.DriverManager;
import java.sql.SQLException;
import com.mysql.jdbc.Connection;


public class MakeSetup {

	static Connection con_result;

	//  to be read from config.cfg.
	// The config.cfg file should  be the working directory.
	static String databaseName_result,dbname;
	static String dbUsername;
	static String dbPassword;
	static String dbaddress;

	

	public static void main(String args[]) throws Exception {
		setup_for_convert();
	}
	
	public static void setup_for_convert() throws Exception {
		setVarsFromConfig();
		connectDB();
		
		BZScriptRunner bzsr = new BZScriptRunner(dbname,con_result);

		bzsr.runScript("scripts/setup.sql");  
		
        
		disconnectDB(con_result);
	}

	//@Overload
	public static void setup_for_convert(String databasename) throws Exception{
		setVarsFromConfig();
		con_result = connectDB(databasename);

		BZScriptRunner bzsr = new BZScriptRunner(databasename,con_result);
		bzsr.runScript("scripts/setup.sql"); 

		disconnectDB(con_result);
	}

	/*public static void create_views() throws Exception{

		setVarsFromConfig();
		con_view = connectDB(databasename);
		BZScriptRunner bzsr = new BZScriptRunner(databasename,con_result);
		bzsr.runScript("scripts/setup.sql"); 

		disconnectDB(con_view);
	}*/
	
	
	public static void setVarsFromConfig(){
		Config conf = new Config();
		dbname = conf.getProperty("dbname");
		dbUsername = conf.getProperty("dbusername");
		dbPassword = conf.getProperty("dbpassword");
		dbaddress = conf.getProperty("dbaddress");
	}

	public static void connectDB() throws SQLException {
		//open database connections to the original database
		String CONN_STR = "jdbc:" + dbaddress + "/" + dbname;
		try {
			java.lang.Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception ex) {
			System.err.println("Unable to load MySQL JDBC driver");
		}
		con_result = (Connection) DriverManager.getConnection(CONN_STR, dbUsername, dbPassword);
	}

	//@Overload
	public static Connection connectDB(String database) throws Exception{

		String CONN_STR = "jdbc:" + dbaddress + "/" + database;

		try {
			java.lang.Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception ex) {
			System.err.println("Unable to load MySQL JDBC driver");
		}

		try{
			return ((Connection) DriverManager.getConnection(CONN_STR, dbUsername, dbPassword));
		} catch (Exception e){
			System.err.println("Could not connect to the database " + database );
		}
		
		return null;

	}
	
	public static void disconnectDB(Connection con) throws SQLException {
		con.close();
	}


}
