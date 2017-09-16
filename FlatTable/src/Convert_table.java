import com.mysql.jdbc.Connection;
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.DatabaseMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.io.IOException;
import java.util.ArrayList;
import java.lang.*;
import java.util.*;
import java.util.Map.Entry;


public class Convert_table{

	private static String dbUsername;
	private static String dbPassword;
	private static String dbAddress;
	private static String database;
	private static String dbname;      // schema name
	private static String column;      // primary key column name
	private static String tablename;   //original table name
	//private static Connection con_convert;

	



    public static void Convert_table ( ) throws Exception{
        //THE INPUT: dbname is database name, tablename is the original table name, 
        //column is primary column name in original table 

        long t1 = System.currentTimeMillis(); 
       
        ArrayList<Integer> id = new ArrayList<Integer>(); // list of all different ids
        ArrayList<ArrayList<String>> Id_instance = new ArrayList<ArrayList<String>>(); // for each id, contains list of row_ids
        ArrayList<ArrayList<Integer>> MultForID = new ArrayList<ArrayList<Integer>>(); // for each id, contains list of mult values
        ArrayList<Integer> instance_mult = new ArrayList<Integer>();   //temporarily stores list of mult for one id
        ArrayList<String> instance_name = new ArrayList<String>();     //temporarily stores list of row_ids for one id
        HashMap<String,Integer> convert_column = new HashMap<String,Integer>();  // to be made columns in converted table
        String convert_tablename = tablename + "_convert";
        Connection convert_con;
        

        //@ Connect database
        convert_con = connectDB(dbname);

        System.out.println("Start convert...");

        //@STEP 1: Find different id values from id_column
        id = find_diffID(convert_con,column,tablename);


        //@STEP 2: Select every instance mult and name for each id and add into the ArrayList<ArrayList<Integer>> and ArrayList<ArrayList<String>>
        for(int count=0;count<id.size();count++){
                instance_mult = new ArrayList<Integer>();
                instance_name = selectFromId(convert_con,id.get(count),column,dbname,tablename,instance_mult);
                MultForID.add(instance_mult);
                Id_instance.add(instance_name);

                System.out.println(" ");
                System.out.println("********************************************************************************************");
                System.out.println("The mult for each instance of id "+ id.get(count) + " is " + MultForID.get(count));
                System.out.println("The instance _name for each instance of id "+ id.get(count) + " is " + Id_instance.get(count));

            }


        //@STEP 3: Find a list of unique column names for convert
        Map_instance(Id_instance,convert_column);

        for(Entry<String,Integer> entry : convert_column.entrySet()){
            String key = entry.getKey();
            Integer value = entry.getValue();
            System.out.println("THE DIFFERENT  CONVERT COLUMN IS : "+ key +", value: " + value);
        }


        //@STEP 4: Set all different instance in convert table
        Set_Columns(convert_con,convert_tablename,convert_column);


        //@STEP 5: Insert instance for each id
        Insert_instance(convert_con,convert_tablename,id,MultForID,Id_instance);
        

        long t2 = System.currentTimeMillis(); 
        System.out.println("Total Running time is " + (t2-t1) + "ms.");
        disconnectDB(convert_con);


    }



	public static void main(String[] args) throws SQLException, IOException{
		
		setVarsFromConfig();
		System.out.println("Set variables from config...");
		// initialize the convert schema by transferring from unielwin_CT database
        //init();

		try{
			Convert_table();
			System.out.println(" Successfully convert table " + tablename);
		} catch (Exception e) {
			System.err.println( e ) ;
		}
		

		//disconnectDB(con_convert);

	}

	
	public static void setVarsFromConfig(){

		Config conf = new Config();
		dbname = conf.getProperty("dbname");
		dbUsername = conf.getProperty("dbusername");
		dbPassword = conf.getProperty("dbpassword");
		dbAddress = conf.getProperty("dbaddress");
		column = conf.getProperty("column");
		tablename = conf.getProperty("tablename");
		database = conf.getProperty("database");

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


	public static ArrayList<Integer> find_diffID(Connection con, String column_name,String tablename) throws Exception{

		Statement st = con.createStatement();
		ResultSet rst = st.executeQuery("SELECT DISTINCT `"+ column_name +"` FROM `" + tablename + "`;");
		ArrayList<Integer> sets = new ArrayList<Integer>();
		while(rst.next()){
			//System.out.println("NEW ID: "+rst.getString(column_name));
			sets.add(rst.getInt(column_name));
		}
		return sets;
	}


	// First main procedure. For each id, go through each row in the CT table that contains the ID, and find each value in the CT table, 
	// concatenate them to form a row id = column name in converted table.
	public static ArrayList<String> selectFromId(Connection con,int id,String id_column, String dbname, String tablename, ArrayList<Integer> instance_mult) throws Exception{
		// given input id, make a list Instance_mult that is indexed by the id, and contains a list of mults //
		//@ This function return the instance_name and instance_mult for each id 
		Statement st = con.createStatement();

		// make a list sets that is indexed by the id, and contains a list of strings that are row_ids. 
		//A row_id is a list of values in the CT table//
		ResultSet rst = st.executeQuery("SELECT * FROM `"+ tablename +"` WHERE `" + id_column + "` = " + id + ";");
		ResultSetMetaData rsmeta = rst.getMetaData();
		ArrayList<String> sets = new ArrayList<String>();
		
		
		while(rst.next()){
			/*System.out.println("INSTANCE:"+rst.getInt(1)+"//"+rst.getInt(2)+"//"+rst.getString(3)+"//"+rst.getString(4)+"//"+rst.getString(5)
				+"//"+rst.getString(6)+"//"+rst.getString(7)+"//"+rst.getString(8)+"//"+rst.getString(9)+"//"+rst.getString(10)
				+"//"+rst.getString(11)+"//"+rst.getString(12)+"//"+rst.getString(13)+"//"+rst.getString(14)+",ok");*/

			String instance_name = "("; // could also be called "row name"
			for(int j=2;j<rsmeta.getColumnCount();j++){
				instance_name += rst.getString(j);
				if(j<( rsmeta.getColumnCount() -1 )){instance_name += ","; }
			}

			instance_name += ")";
			//System.out.println("instance_name for id " + id + " is: " + instance_name );
			instance_mult.add(rst.getInt(1));
			//System.out.println("The mult of "+ instance_name + " is: " +rst.getInt(1));
			sets.add(instance_name);  
			
		}

		return sets;
	}

	//@ This function is to map all different instacnce in Id_instance to initialize the convert column name
	//ID_instance is indexed by ids. For each id, contains all instance_ids (= row_ids)
	public static void Map_instance( ArrayList<ArrayList<String>> Id_instance, HashMap<String,Integer> convert_column ) throws Exception{

		for(int i=0;i<Id_instance.size();i++){ // loop through all ids, find the list of row_ids for that id

			for(int j=0;j<Id_instance.get(i).size();j++){
				String column =  Id_instance.get(i).get(j);
				//System.out.println(Id_instance.get(i).get(j));
				// add instance ids to hash map called convert_column
				if(!(convert_column.containsKey(column))){
					convert_column.put(column,1);
				}else{
					convert_column.put(column,convert_column.get(column)+1);
				}
			}

		}


	}



	//@ Set the convert columns . Go through the hash map of row_ids and make them column names in SQL
	public static void Set_Columns( Connection con, String tablename , HashMap<String,Integer> convert_column ) throws Exception{

		Statement st = con.createStatement();

		String checkSQL = "DROP TABLE IF EXISTS `" + tablename + "` ;";
		int rst = st.executeUpdate(checkSQL);

		String newsql = "CREATE TABLE `" + tablename +"` ( id int(11)  NOT NULL, ";

		for(Entry<String,Integer> entry : convert_column.entrySet()){
				String key = entry.getKey();
				newsql += " `" + key +"` int(11)  DEFAULT 0 , ";
				
			}

			newsql+=" PRIMARY KEY ( id ) );";

			rst = st.executeUpdate(newsql);

		System.out.println(rst + "######"+newsql);

	}


	//@ insert each instance from each id
	public static void Insert_instance(Connection con, String tablename, ArrayList<Integer> id , ArrayList<ArrayList<Integer>> MultForID ,ArrayList<ArrayList<String>> Id_instance ) throws Exception {

		Statement st = con.createStatement();
		
		for(int i=0; i<Id_instance.size();i++){
			String newsql = "INSERT INTO `" + tablename + "` ( id ";

			for(int j=0;j<Id_instance.get(i).size();j++){ //for each id, loop through its column names
				String column = Id_instance.get(i).get(j);
				newsql += ", `" + column + "` ";
			}
			newsql += " ) VALUES ( " + id.get(i); //get the primary key value

			for(int k =0;k<MultForID.get(i).size();k++){ //go through the mults for the id. The index k should point to the right row_id.
				int mult = MultForID.get(i).get(k); //find the mult for the given id and the given row_id. The KEY STATEMENT.
				newsql += ", " + mult; 
			}
			newsql += " );";
			int rst = st.executeUpdate(newsql); //THE BIG ONE. CREATES THE WHOLE CONVERTED TABLE.
			System.out.println("%%%%%% The insert sql for id "+ id.get(i) + " is :" +newsql);
		}

	}



	public static void init() throws SQLException, IOException{

		MakeSetup set = new MakeSetup();

		try{
			set.setup_for_convert(database);
			System.out.println("Successfully initialize the convert schema...");
		} catch (Exception e) {
			System.err.println("Setup failed!!!" );
		}
		

	}




	public static void disconnectDB(Connection con) throws SQLException {
		con.close();
	}

 
}
 

