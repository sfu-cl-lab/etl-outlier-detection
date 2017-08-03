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
	private static String test_tbname="test";
	private static String id_column;


	
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
		while(rst.next()){
			System.out.println(rst.getString(0));
			
			sets.add(rst.getString(0));
		}
		return sets;
	}

	public static ArrayList<String> find_columnName(String test_tbname) throws Exception{
		//Because we couldn't set up a primary key,we should get ID column name from this function
		Statement st = con_convert.createStatement();
		ResultSet rst = st.executeQuery("SELECT column_name FROM information_schema.columns WHERE table_name =  '"+ test_tbname +"';");
		ArrayList<String> sets = new ArrayList<String>();
		while(rst.next()){
			//System.out.println(rst.getString("column_name"));
			
			sets.add(rst.getString("column_name"));
		}
		return sets;
	}

	public static ArrayList<Integer> find_diffID(String column_name) throws Exception{

		Statement st = con_convert.createStatement();
		ResultSet rst = st.executeQuery("SELECT DISTINCT `"+ column_name +"` FROM `" + test_tbname + "`;");
		ArrayList<Integer> sets = new ArrayList<Integer>();
		while(rst.next()){
			//System.out.println("NEW ID: "+rst.getString(column_name));
			sets.add(rst.getInt(column_name));
		}
		return sets;
	}

	public static ArrayList<String> selectFromId(int id,ArrayList<Integer> instance ) throws Exception{
		Statement st = con_convert.createStatement();
		ResultSet rst = st.executeQuery("SELECT * FROM `"+ test_tbname +"` WHERE `" + id_column + "` = " + id + ";");
		ArrayList<String> sets = new ArrayList<String>();
		//ArrayList<Integer> instance = new ArrayList<Integer>();
		int i=0;
		String instance_name = "";
		while(rst.next()){
			/*System.out.println("INSTANCE:"+rst.getInt(1)+"//"+rst.getInt(2)+"//"+rst.getString(3)+"//"+rst.getString(4)+"//"+rst.getString(5)
				+"//"+rst.getString(6)+"//"+rst.getString(7)+"//"+rst.getString(8)+"//"+rst.getString(9)+"//"+rst.getString(10)
				+"//"+rst.getString(11)+"//"+rst.getString(12)+"//"+rst.getString(13)+"//"+rst.getString(14)+",ok");*/

			instance_name = "("+rst.getString(3)+","+rst.getString(4)+","+rst.getString(5)+","+rst.getString(6)
			+","+rst.getString(7)+","+rst.getString(8)+","+rst.getString(9)+","+rst.getString(10)+","+rst.getString(11)
			+","+rst.getString(12)+","+rst.getString(13)+","+rst.getString(14)+")";

			instance.add(rst.getInt(2));
			//System.out.println("The mult of "+ instance_name + "is:" +instance.get(i));
			i++;
			sets.add(instance_name);
			
		}
		//int[] n = (int[])instance.toArray(new int[instance.size()]);
		//MultForID.add(n);

		return sets;
	}



	//main function
	public static void Convert_table(){
		long t1 = System.currentTimeMillis(); 
		ArrayList<String> column_name = new ArrayList<String>();
		ArrayList<Integer> id = new ArrayList<Integer>();
		ArrayList<String> id_instance = new ArrayList<String>();
		ArrayList<ArrayList<Integer>> MultForID = new ArrayList<ArrayList<Integer>>();
		ArrayList<Integer> instance = new ArrayList<Integer>();
	    //int count = 0;
		System.out.println("Start convert...");

		//connect database
		try{
			con_convert = connectDB(dbname_convert);
		} catch (Exception e) {
			System.err.println("Could not connect to the database " + dbname_convert );
			//throw new Exception();
		}

		//select the ID column name and find all different ID at store into a id ArrayList
		try{
			column_name = find_columnName(test_tbname);
			id_column = column_name.get(0);
			System.out.println("The id column name is: "+id_column);
			id = find_diffID(id_column);
		} catch (Exception e) {
			System.err.println("Could get the ID column name and diffID !");
			System.err.println(e);
		}  

		//select the mult for each instance about each id and store into a ArrayList<ArrayList<Integer>>
		try{
			for(int count=0;count<id.size();count++){
				instance = new ArrayList<Integer>();
				id_instance = selectFromId(id.get(count),instance);
				MultForID.add(instance);
				System.out.println("The mult for each instance of id "+ id.get(count) + " is " + MultForID.get(count));
			}
			
		} catch (Exception e) {
			System.err.println("Could not select id instance from database ! ");
			System.err.println(e);
		}


		/*
		if(primaryKey.size() == 0)
			System.out.println("The table didn't have primary Key... ");
		else{
			for(String key : primaryKey){
				System.out.println("The primary Key is "+ key );
			}
		}*/


		

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
 

