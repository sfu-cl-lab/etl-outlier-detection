import com.mysql.jdbc.Connection;
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.io.IOException;
import java.util.ArrayList;
import java.lang.*;
import java.util.*;
import java.util.Map.Entry;


public class Convert_table{

	static String dbUsername;
	static String dbPassword;
	static String dbAddress;
	static String dbName;
	private static Connection con_convert;
	private static String dbname_convert;
	



	//@ Overload 
    public static void Convert_table ( String dbname, String tablename, String column ) throws Exception{
        //THE INPUT: dbname is database name, tablename is the original table name, column is primary column name in original table 

        long t1 = System.currentTimeMillis(); 
       
        ArrayList<Integer> id = new ArrayList<Integer>();
        ArrayList<ArrayList<String>> Id_instance = new ArrayList<ArrayList<String>>();
        ArrayList<ArrayList<Integer>> MultForID = new ArrayList<ArrayList<Integer>>();
        ArrayList<Integer> instance_mult = new ArrayList<Integer>();
        ArrayList<String> instance_name = new ArrayList<String>();
        HashMap<String,Integer> convert_column = new HashMap<String,Integer>();
        String convert_tablename = tablename + "_convert";
        Connection convert_con;

        
        System.out.println("Start convert...");


        //Set Config
        setVarsFromConfig();

        //@ Connect database
        convert_con = connectDB(dbname);

        //@ Find different id values from id_column
        id = find_diffID(convert_con,column,tablename);


        //@ Select the mult for each instance about each id and store into a ArrayList<ArrayList<Integer>>
        for(int count=0;count<id.size();count++){
                instance_mult = new ArrayList<Integer>();
                instance_name = selectFromId(convert_con,id.get(count),column,tablename,instance_mult);
                MultForID.add(instance_mult);
                Id_instance.add(instance_name);

                System.out.println(" ");
                System.out.println("********************************************************************************************");
                System.out.println("The mult for each instance of id "+ id.get(count) + " is " + MultForID.get(count));
                System.out.println("The instance _name for each instance of id "+ id.get(count) + " is " + Id_instance.get(count));

            }


        //@ Find a list of unique column names for convert
        Map_instance(Id_instance,convert_column);

        for(Entry<String,Integer> entry : convert_column.entrySet()){
            String key = entry.getKey();
            Integer value = entry.getValue();
            System.out.println("THE DIFFERENT  CONVERT COLUMN IS : "+ key +", value: " + value);
        }


        //@ Set all different instance in convert table
        Set_Columns(convert_con,convert_tablename,convert_column);


        //@ Insert instance for each id
        Insert_instance(convert_con,convert_tablename,id,MultForID,Id_instance);
        

        long t2 = System.currentTimeMillis(); 
        System.out.println("Total Running time is " + (t2-t1) + "ms.");
        disconnectDB(convert_con);


    }



	public static void main(String[] args) throws SQLException, IOException{
		
		//init();
		String dbname = "unielwin_convert",tablename = "test",column = "ID(student0)";

		try{
			Convert_table(dbname,tablename,column);
			//Convert_table();
		} catch (Exception e) {
			System.err.println( e ) ;
		}
		

		//disconnectDB(con_convert);

	}

	
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
	
	public static ArrayList<String> selectFromId(Connection con,int id,String id_column, String tablename, ArrayList<Integer> instance_mult) throws Exception{
		// given input id, make a list Instance_mult that is indexed by the id, and contains a list of mults //
		//@ This function return the instance_name and instance_mult for each id 
		Statement st = con.createStatement();
		ResultSet rst = st.executeQuery("SELECT * FROM `"+ tablename +"` WHERE `" + id_column + "` = " + id + ";");
		ArrayList<String> sets = new ArrayList<String>();
		// make a list sets that is indexed by the id, and contains a list of strings that are row_ids. 
		//A row_id is a list of values in the CT table//
		
		int i=0;
		String instance_name = ""; // could also be called "row name"
		while(rst.next()){
			/*System.out.println("INSTANCE:"+rst.getInt(1)+"//"+rst.getInt(2)+"//"+rst.getString(3)+"//"+rst.getString(4)+"//"+rst.getString(5)
				+"//"+rst.getString(6)+"//"+rst.getString(7)+"//"+rst.getString(8)+"//"+rst.getString(9)+"//"+rst.getString(10)
				+"//"+rst.getString(11)+"//"+rst.getString(12)+"//"+rst.getString(13)+"//"+rst.getString(14)+",ok");*/

			instance_name = "("+rst.getString(3)+","+rst.getString(4)+","+rst.getString(5)+","+rst.getString(6)
			+","+rst.getString(7)+","+rst.getString(8)+","+rst.getString(9)+","+rst.getString(10)+","+rst.getString(11)
			+","+rst.getString(12)+","+rst.getString(13)+","+rst.getString(14)+")";

			instance_mult.add(rst.getInt(2));
			//System.out.println("The mult of "+ instance_name + "is:" +instance.get(i));
			i++;
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

		String checkSQL = "DROP TABLE IF EXISTS " + tablename + " ;";
		int rst = st.executeUpdate(checkSQL);

		String newsql = "CREATE TABLE " + tablename +"( id int(11)  NOT NULL, ";

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
			String newsql = "INSERT INTO " + tablename + " ( id ";

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



	//@ Main function process
	public static void Convert_table(){
		long t1 = System.currentTimeMillis(); 
		ArrayList<String> column_name = new ArrayList<String>(); //temporarily stores list of row_ids for each id
		String tablename = "ab_CT";
		String id_column = "ID(student0)";
		String table_convert = tablename + "_convert";
		ArrayList<Integer> id = new ArrayList<Integer>(); //lists all ids
		ArrayList<ArrayList<String>> Id_instance = new ArrayList<ArrayList<String>>(); //for each id, contains list of row_ids
		ArrayList<ArrayList<Integer>> MultForID = new ArrayList<ArrayList<Integer>>();
		ArrayList<Integer> instance_mult = new ArrayList<Integer>(); // for each id, contains list of mult values
		ArrayList<String> instance_name = new ArrayList<String>(); //temporarily stores list of row_ids for each id
		HashMap<String,Integer> convert_column = new HashMap<String,Integer>(); //hash map for list of all row_ids 
		//to be made columns in converted table

		setVarsFromConfig();
		System.out.println("Set variables");
	    
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
			
			//find different id values from id_column
			id = find_diffID(con_convert,id_column,tablename);
		} catch (Exception e) {
			System.err.println("Could get the ID column name and diffID !");
			System.err.println(e);
		}  

		//@ select the mult for each instance about each id and store into a ArrayList<ArrayList<Integer>>
		try{
			for(int count=0;count<id.size();count++){
				instance_mult = new ArrayList<Integer>();
				instance_name = selectFromId(con_convert,id.get(count),id_column,tablename,instance_mult);
				MultForID.add(instance_mult);
				Id_instance.add(instance_name);

				System.out.println(" ");
				System.out.println("********************************************************************************************");
				System.out.println("The mult for each instance of id "+ id.get(count) + " is " + MultForID.get(count));
				System.out.println("The instance _name for each instance of id "+ id.get(count) + " is " + Id_instance.get(count));

			}
			
		} catch (Exception e) {
			System.err.println("Could not select id instance from database ! ");
			System.err.println(e);
		}

		try{
			//@ Find a list of unique column names for convert
			Map_instance(Id_instance,convert_column);

			for(Entry<String,Integer> entry : convert_column.entrySet()){
				String key = entry.getKey();
				Integer value = entry.getValue();
				System.out.println("THE DIFFERENT  CONVERT COLUMN IS : "+ key +", value: " + value);
			}

			/*for(TypeKey column: convert_column.KeySet()){
				String key = column.toString();
				Integer value = convert_column.get(column).toInt();
				System.out.println("ALL DIFFERENT  CONVERT COLUMN IS : "+ key +", value: " + value);
			}*/
			
		} catch (Exception e) {
			System.err.println("Could not Map the instacnce to get convert column name! ");
			System.err.println(e);
		}



		try{
			
			Set_Columns(con_convert,table_convert,convert_column);

		} catch (Exception e) {
			System.err.println("Could not set up convert columns!!! ");
			System.err.println(e);
		}



		try{
			
			Insert_instance(con_convert,table_convert,id,MultForID,Id_instance);

		} catch (Exception e) {
			System.err.println("Could not insert instance into convert table !!! ");
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

		/*for(int i=0;i<id.size();i++)
			System.out.println("The instance size of id "+id.get(i) +" is " + Id_instance.get(i).size());*/

		

		//System.out.println("The size of Id is "+ id.get(0));
		//System.out.println("The size of Id_instance is "+ Id_instance.get(0).get(0));

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



	public static void disconnectDB(Connection con) throws SQLException {
		con.close();
	}
 

		//select the delete tablenames,return a strinng
        /*st.execute("select concat('drop table ',table_name,';') as result FROM information_schema.tables where table_schema = " +dbname+ " and table_name != " +tablename+ ";" );  
		System.out.println(result);
		Statement tmp = con.createStatement();*/

 
}
 

