import com.mysql.jdbc.Connection;
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.io.IOException;
import java.util.ArrayList;
import java.lang.*;


public class Convert_table{

	static String dbUsername;
	static String dbPassword;
	static String dbAddress;
	static String dbName;
	static String resultDB_name;

	private static Connection con_convert;
	private static String dbname_convert;
	private static String test_tbname="a_CT";


	
	public static void setVarsFromConfig(){

		Config conf = new Config();
		dbName = conf.getProperty("dbname");
		dbUsername = conf.getProperty("dbusername");
		dbPassword = conf.getProperty("dbpassword");
		dbAddress = conf.getProperty("dbaddress");
		dbname_convert = dbName + "_convert";

	}

	public static Connection connectDB(String database) throws Exception{

		String CONN_STR = "jdbc:" + dbAddress + "/" + database;

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

	public static ArrayList<String> find_primaryKey(String dbname_convert, String test_tbname) throws Exception{

		Statement st = con_convert.createStatement();
		ResultSet rst = st.executeQuery("SHOW KEYS FROM "+ dbname_convert +"."+ test_tbname +" WHERE Key_name = 'PRIMARY';");
		ArrayList<String> sets = new ArrayList<String>();
		/*while(rst.next()){
			System.out.println(rst.getString(0));
			
			sets.add(rst.getString(0));
		}*/
		return sets;
	}


	//main function
	public static void Convert_table(){
		long t1 = System.currentTimeMillis(); 
		ArrayList<String> primaryKey = new ArrayList<String>();
		System.out.println("Start convert...");

		try{
			con_convert = connectDB(dbname_convert);
		} catch (Exception e) {
			System.err.println("Could not connect to the database " + dbname_convert );
			//throw new Exception();
		}

		try{
			primaryKey = find_primaryKey(dbname_convert,test_tbname);
		} catch (Exception e) {
			System.err.println("Could not run primaryKey function..." );
			//throw new Exception();
		}

		
		if(primaryKey.size() == 0)
			System.out.println("The table didn't have primary Key... ");


		

		/*







		*/

		long t2 = System.currentTimeMillis(); 
		System.out.println("Total Running time is " + (t2-t1) + "ms.");
		
	}

	public static void init() throws SQLException, IOException{

		MakeSetup set = new MakeSetup();

		try{
			set.setup_for_convert(dbName);
		} catch (Exception e) {
			System.err.println("Setup failed!!!" );
		}
		

	}

	public static void main(String[] args) throws SQLException, IOException{
		
		setVarsFromConfig();
		System.out.println("Set variables");

		init();
		
		Convert_table();

		disconnectDB();

	}


	public static void disconnectDB() throws SQLException {
		con_convert.close();
	}
 

		//select the delete tablenames,return a strinng
        /*st.execute("select concat('drop table ',table_name,';') as result FROM information_schema.tables where table_schema = " +dbname+ " and table_name != " +tablename+ ";" );  
		System.out.println(result);
		Statement tmp = con.createStatement();*/

 
}
 

