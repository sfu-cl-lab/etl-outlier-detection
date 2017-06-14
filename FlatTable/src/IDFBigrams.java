import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
public class IDFBigrams {
	static Connection con4;
	static String ResultDB,dbOriginal,dbBN,dbSchema,GenericDB;
	static String dbUsername;
	static String dbPassword;
	static String dbaddress,FileTitleFreq;
	static String outlier="0"; //0 for non outlieres, 1 for outliers
	public static void main(String[] args) throws Exception {
		setVarsFromConfig();
		connectDB();
		disconnectDB1();
		createNodeTable();
		init();
		ComputeBiGrams();
 }
	static void init(){
		
		try{ 
		//	delete(new File(FileTitleFreq+"/"));

		}catch (Exception e){
			
			
		}

		if(outlier.equals("1")){
			System.out.println("We are yhere");
			FileTitleFreq="CSVOutlier/Bigrams/";
			
		}
		else{
			//FileTitle="CSVNormal/";
			System.out.println("outlierr="+outlier);
			System.out.println("We are not yhere");
			FileTitleFreq="CSVNormal/Bigrams/";

		}
		System.out.println(FileTitleFreq);
		new File(FileTitleFreq+"/" + File.separator).mkdirs();
	}
	public static ArrayList<String>  createNodeTable() throws SQLException{
		// TODO Auto-generated method stub
		connectDB();
		Statement st=con4.createStatement();
		ResultSet CT=st.executeQuery("select 2nid from "+dbOriginal+"_BN.2Nodes union select 1nid from "+dbOriginal+"_BN.1Nodes");
		ArrayList<String> Nodes = new ArrayList<String>();
		while ( CT.next() )
		{
			String fid = CT.getString( 1 );
			String s= fid.substring(1, fid.length() - 1);
			Nodes.add( s );
		}
		
		CT.close();
		return Nodes;
		
	}
	public static void ComputeBiGrams() throws SQLException, IOException {
		// TODO Auto-generated method stub
		connectDB();
		RandomAccessFile csvFreq = new RandomAccessFile(FileTitleFreq+"/" + File.separator + File.separator + dbOriginal + ".csv", "rw");
		//Statement st=con2.createStatement();

			
			Statement st=con4.createStatement();
		
			String csvString="";
			if(outlier.equals("1")){
			 csvString = "Outlier,";
			}
			else{
				 csvString = "Normal,";
			}
			st.execute("Drop table if exists "+dbOriginal+"_CT.IDFs");
			System.out.println("create table "+dbOriginal+"_CT.IDFs(id INT NOT NULL, " +
					"Node1 VARCHAR(45) NULL, Node2 VARCHAR(45) NULL, MULT VARCHAR(45) NULL, Primary key(id))");
			st.execute("create table "+dbOriginal+"_CT.IDFs (id INT NOT NULL, " +
					"Node1 VARCHAR(45) NULL, Node2 VARCHAR(45) NULL, MULT VARCHAR(45) NULL, Primary key(id))");
			int counter2=0;
			for(int j=0; j<createNodeTable().size();j++){
				
				String Node1=createNodeTable().get(j);
				//
				ResultSet rsNode1=st.executeQuery("select distinct `"+Node1+"` from "+dbOriginal+"_CT.`a,b,c_CT` ");
				ArrayList<String> Node1Values = new ArrayList<String>();
				while ( rsNode1.next() )
				{
					String fid = rsNode1.getString( 1 );
					Node1Values.add( fid);
				}
				
				rsNode1.close();
				for(int k=0; k<createNodeTable().size();k++){
					
					String Node2=createNodeTable().get(k);
					if(!Node1.equals(Node2)){ 
					ResultSet rsNode2=st.executeQuery("select distinct `"+Node2+"` from "+dbOriginal+"_CT.`a,b,c_CT`");
					ArrayList<String> Node2Values = new ArrayList<String>();
					while ( rsNode2.next() )
					{
						String fid = rsNode2.getString( 1 );
						Node2Values.add( fid);
					}
					
					for(int ii=0; ii<Node1Values.size();ii++){
						
						String Node1V=Node1Values.get(ii);
						String NodeandValues1=Node1+Node1V;
						for(int jj=0; jj<Node2Values.size();jj++){
							counter2++;
							String Node2V=Node2Values.get(jj);
							String NodeandValues2=Node2+Node2V;
							/*System.out.println("select sum(MULT) from "+dbOriginal+"_"+PlayerID+"_CT.`a,b,c_CT`" +
									" where `"+ Node1+"` = "+Node1V+" and `"+Node2+"` = "+Node2V);
							ResultSet sumM = st.executeQuery("select sum(MULT) from "+dbOriginal+"_"+PlayerID+"_CT.`a,b,c_CT`" +
									" where `"+ Node1+"` = '"+Node1V+"' and `"+Node2+"` = '"+Node2V+"'");
							
							sumM.next();
							String sumMult = sumM.getString(1);
							if(sumMult!=null){
								csvString += sumMult + ",";
								counter++;
							System.out.println("insert into "+dbOriginal+"_"+PlayerID+"_flat.BigramVector values" +
									"("+ counter+", '"+
									NodeandValues1+"', '"+NodeandValues2+"', "+sumMult+")");
							st.execute("insert into "+dbOriginal+"_"+PlayerID+"_flat.BigramVector values" +
									"("+ counter+", '"+
									NodeandValues1+"', '"+NodeandValues2+"', "+sumMult+")");
							}
							else{
								csvString += "0" + ",";
								
							}*/
							int counter=0;
							for(int i=0;i<PlayerTeamW("43").size();i++){
							String PlayerID = PlayerTeamW("43").get(i);
							System.out.println("select MULT from "+dbOriginal+"_"+PlayerID+"_flat.BigramVector" +
									" where `"+ Node1+"` = '"+NodeandValues1+"' and `"+Node2+"` = '"+NodeandValues2+"'");
							ResultSet sumM = st.executeQuery("select MULT from "+dbOriginal+"_"+PlayerID+"_flat.BigramVector" +
									" where  Node1='"+ NodeandValues1+"' and Node2= '" + NodeandValues2+"'");
							String sumMult="";
							if(sumM.next()){
								
							 sumMult = sumM.getString(1);
							}
							else {
								sumMult="0";
							}
							if(!sumMult.equals("0")){
								counter++;
							}
							
							else{
								System.out.print("o");
							}
							
						}
							ResultSet exists=st.executeQuery("Select * from "+dbOriginal+"_CT.IDFs where Node1='"+NodeandValues2
									+"' and Node2='"+NodeandValues1+"'");
							if(!exists.next()){
							st.execute("insert into "+dbOriginal+"_CT.IDFs values" +
									"("+ counter2+", '"+
									NodeandValues1+"', '"+NodeandValues2+"', "+counter+")");
							}
							else{
								System.out.println("Do Nothing");
							}
					}
						
				
					
				//	st.execute("");
					
					
					}
				}
			}
		//	csvString = csvString.substring(0, csvString.length() - 1);
		//	csvFreq.writeBytes(csvString + "\n");
			
		}
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
	//	outlier=conf.getProperty("Outlier");
		//ComputationMode=conf.getProperty("ComputationMode");
		System.out.println("haha");
		
	}
	public static void connectDB() throws SQLException  {

		
		
		String CONN_STR_DB = "jdbc:" + dbaddress + "/" + dbOriginal;
		try {
			java.lang.Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception ex) {
			System.err.println("Unable to load MySQL JDBC driver");
		}
		System.out.println(dbUsername+"UserName");
		con4 = (Connection) DriverManager.getConnection(CONN_STR_DB, dbUsername, dbPassword);
	}
		

		


		private static int disconnectDB1()
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
		
		public static ArrayList<String> PlayerTeamW(String TeamID) throws SQLException {
			
			ArrayList<String> PlayerList = new ArrayList<String>();
			Statement st1 = con4.createStatement();
			System.out.println("	sa");
			//where PlayerID not in (select PlayerID from PPLPositvePlayer.Path_BayesNets)
		/*	ResultSet rs = st1.executeQuery(" select PlayerID  from" +
					" Premier_League_MidStr_00.PlayersClass where class=4 "  );*/
		//	ResultSet rs = st1.executeQuery(" select movieid from  `imdb_MovieLens_Drama_bk`.`selected`;" );
			//ResultSet rs = st1.executeQuery(" select movieid  from movies where movieid not in (select PlayerID from imdb_MovieLens_Comedy_01.);" );
//		ResultSet rs = st1.executeQuery("select distinct PlayerID from Players;" );
		//	ResultSet rs = st1.executeQuery("select distinct PlayerID from Premier_League_2011.Scores where PlayerID not in (select PlayerID from Premier_League_2011.Scores) ;" );
			
			//`Premier_League_Strikers_00`.`OutlierDetector`
		//	ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM imdb_MovieLens_Drama_Sep29_00.selected where class=2;"  );
		//	ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM Premier_League_MidStr_00.PlayersClass where class=4 and PlayerID in(select PlayerID from  Premier_League_MidStr_00.PlayerFrequence where matchNum>=5);"  );
			//ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM  Premier_League_Synthetic_Bernoulli_Feature.Players;"  );
			//ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM Premier_League_Strikers_00.Feb4Players "  );
			//Premier_League_MidStr SELECT PlayerID  FROM Premier_League_MidStr_00.Feb4Players 
		//	ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM  Premier_League_Synthetic_Bernoulli_Feature.Players;"  );
			ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM Premier_League_MidStr_00.Feb4Players"  );
		//	ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM imdb_MovieLens_Drama_Sep29_00.selected;"  );
			while (rs.next()) {
				PlayerList.add(""+rs.getInt(1));

			}
			  
			st1.close(); 
		
		
		return PlayerList;
		
	}
}
