import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.mysql.jdbc.Connection;


public class Wrapper {
	static Connection con4;
	static String ResultDB,dbOriginal,dbBN,dbSchema,GenericDB;
	static String dbUsername;
	static String dbPassword;
	static String dbaddress,outlier,ComputationMode;
	public static void main(String[] args) throws Exception {
		
		setVarsFromConfig();
		connectDB();
		 
		
		
		for(int i=0;i<PlayerTeamW("43").size();i++){
			Statement st1 = con4.createStatement();
			 String PlayerID = PlayerTeamW("43").get(i);
			 File file = new File("src/SubsetConfig.cfg");
				BufferedWriter output = new BufferedWriter(new FileWriter(file));
				output.write("dbOriginal = "+dbOriginal+"_"+PlayerID+"\n");
				output.write("dbSchema = "+dbOriginal+"_"+PlayerID+"_db\n");
				output.write("dbBN = "+dbOriginal+"_"+PlayerID+"_BN\n");
				output.write("ResultDB = "+dbOriginal+"_"+PlayerID+"_flat\n");
				output.write("GenericDB="+GenericDB+"\n");
				output.write("dbusername = " + dbUsername +"\n");
				output.write("dbpassword = "+ dbPassword + "\n");
				output.write("dbaddress = "+ dbaddress + "\n");
				output.write("Outlier = "+outlier+"\n");
	
				System.out.println("Outlierr "+outlier);
				output.write("ComputationMode = "+ComputationMode+"\n");
				//OriginalDB
				output.close();
				PlayerFlat.PlayerFlat();
				st1.close();
				//st1.execute("insert into `Premier_League_Strikers_00`.`IDSFlat` Values ("+PlayerID+")");
		}
		checkColumnNumber();
		checkSumColumn() ;
		disconnectDB();
	}
	
	
public static ArrayList<String> PlayerTeamW(String TeamID) throws SQLException {
		
		ArrayList<String> PlayerList = new ArrayList<String>();
		Statement st1 = con4.createStatement();
		System.out.println("	sa");
		//where PlayerID not in (select PlayerID from PPLPositvePlayer.Path_BayesNets)
	/*	ResultSet rs = st1.executeQuery(" select PlayerID  from" +
				" Premier_League_MidStr_00.PlayersClass where class=4 "  );*/
	//	ResultSet rs = st1.executeQuery(" select movieid from  `imdb_MovieLens_Drama_bk`.`selected`;" );
		//ResultSet rs = st1.executeQuery(" select movieid  from movies where movieid not in (select PlayerID from imdb_MovieLens_Comedy_01.);" );
//	ResultSet rs = st1.executeQuery("select distinct PlayerID from Players;" );
	//	ResultSet rs = st1.executeQuery("select distinct PlayerID from Premier_League_2011.Scores where PlayerID not in (select PlayerID from Premier_League_2011.Scores) ;" );
		
		//`Premier_League_Strikers_00`.`OutlierDetector`
		ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM imdb_MovieLens_Drama_Sep29_00.selected where class=2; "  );
		while (rs.next()) {
			PlayerList.add(""+rs.getInt(1));

		}
		  
		st1.close(); 
	
	
	return PlayerList;
	
}
public static void setVarsFromConfig(){
	Config conf = new Config();
	dbOriginal=conf.getProperty("dbOriginal");
	dbSchema=conf.getProperty("dbSchema");
	dbBN=conf.getProperty("dbBN");
	ResultDB = conf.getProperty("ResultDB");
	GenericDB=conf.getProperty("GenericDB");
	dbUsername = conf.getProperty("dbusername");
	dbPassword = conf.getProperty("dbpassword");
	dbaddress = conf.getProperty("dbaddress");
	outlier=conf.getProperty("Outlier");

	ComputationMode=conf.getProperty("ComputationMode");
	System.out.println("haha");
	//OriginalDB
	//OutputDB=Premier_League_2011_3_db
	
}
public static void connectDB() throws SQLException {
	String CONN_STR2 = "jdbc:" + dbaddress + "/" + dbOriginal;
	

	try {
		java.lang.Class.forName("com.mysql.jdbc.Driver");
	} catch (Exception ex) {
		System.err.println("Unable to load MySQL JDBC driver");
	}
	con4 = (Connection) DriverManager.getConnection(CONN_STR2, dbUsername, dbPassword);
	

	
}
private static int disconnectDB()
{
	try
	{
		con4.close();
	}
	catch ( SQLException e )
	{ 
		System.out.println( "Failed to close database connection." );
		e.printStackTrace();
		return -1;
	}
	
	return 0;
}

public static void checkColumnNumber() throws SQLException{
	connectDB();
	Statement st1 = con4.createStatement();
	st1.execute("Drop table if exists "+dbBN+".ColumnsSimilarity");
	 st1.execute("Create table if not exists  "+dbBN+".ColumnsSimilarity (ID int NOT NULL" +
	 		", colCount varchar(10), PRIMARY KEY (`ID`) )");

	for(int i=0;i<PlayerTeamW("43").size()-1;i++){
		 String PlayerID = PlayerTeamW("43").get(i);
		 String NextPlayerID = PlayerTeamW("43").get(i+1);
		 /*Create table if not exists "+ResultDB+".ColumnCount(  `ctName`  VARCHAR(45) NOT NULL , " +
				" `colCount` int NULL ,  PRIMARY KEY (`ctName`) ) "*/
				String Query="select * from "+dbOriginal+"_"+PlayerID+"_flat.ColumnCount" +
				" where (ctName, colCount) not in (select ctName, colCount from " +dbOriginal
				+"_"+NextPlayerID+"_flat.ColumnCount)";
		
		ResultSet rs = st1.executeQuery(Query);
		
		if(!rs.isBeforeFirst()){
			System.out.println("No Data");
			st1.execute("insert into " +dbBN+".ColumnsSimilarity values("+NextPlayerID+", 'Yes!')");
			
			
		}
		else {
			st1.execute("insert into " +dbBN+".ColumnsSimilarity values("+NextPlayerID+", 'NO!')");
		}
	}


	
}

public static void checkSumColumn() throws SQLException{
	connectDB();
	Statement st1 = con4.createStatement();
	st1.execute("Drop table if exists "+dbBN+".ColumnSum");
	 st1.execute("Create table if not exists  "+dbBN+".ColumnSum (ID int NOT NULL" +
	 		", colCount varchar(10), PRIMARY KEY (`ID`) )");
		for(int i=0;i<PlayerTeamW("43").size();i++){
			 String PlayerID = PlayerTeamW("43").get(i);
			 String QueryQ="Select sum(colCount) from "+dbOriginal+"_"+PlayerID+"_flat.ColumnCount";
			 System.out.println("QuerQQQ"+QueryQ);
				ResultSet rs = st1.executeQuery(QueryQ);
				ArrayList<String> PlayerList = new ArrayList<String>();
				while(rs.next()){
					PlayerList.add(""+rs.getInt(1));
				}
				String Sum=PlayerList.get(0);
				st1.execute("insert into " +dbBN+".ColumnSum values("+PlayerID+", "+Sum+")");
			
		}
}

public static void union()throws SQLException{
	
	connectDB();
	Statement st1 = con4.createStatement();
	String Query="";
	for(int i=0;i<PlayerTeamW("43").size();i++){
		String PlayerID = PlayerTeamW("43").get(i);
		Query=Query+" create table `Premier_League_Strikers_UnionBN`.UnionPathBayesNets  as Select * from `Premier_League_Strikers_UnionBN`.`Path_BayesNets_"+PlayerID+"` union";
		
	}
	Query=Query.substring(0,Query.length()-6);
	st1.execute(Query);
}
}
