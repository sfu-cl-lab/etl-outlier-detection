import java.awt.PageAttributes.OriginType;
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
public class BiGrams {
	static Connection con4;
	static String ResultDB,dbOriginal,dbBN,dbSchema,GenericDB;
	static String dbUsername;
	static String dbPassword;
	static String dbaddress,FileTitleFreq,FileTitleIDF;
	static String outlier="1"; //0 for non outlieres, 1 for outliers
	static String TotalNumberOfDocument="272";
	static double Percentages=0.4;
	static String FileTitleIDFPerc;
	
	
	public static void main(String[] args) throws Exception {
			setVarsFromConfig();
			connectDB();
			disconnectDB1();
			createNodeTable();
			init();
			ComputeBiGrams();
			ComputeTfIDFBiGrams();
			//TopNPercentIDF();
	}
	
	static void init(){
		
		try{ 
		//	delete(new File(FileTitleFreq+"/"));

		}catch (Exception e){
			
			
		}

		if(outlier.equals("1")){
			System.out.println("We are yhere");
			FileTitleFreq="CSVOutlier/Bigrams/";
			FileTitleIDF="CSVOutlier/IDF/";
			FileTitleIDFPerc="CSVOutlier/IDF"+Percentages+"/";
		}
		else{
			//FileTitle="CSVNormal/";
			System.out.println("outlierr="+outlier);
			System.out.println("We are not yhere");
			FileTitleFreq="CSVNormal/Bigrams/";
			FileTitleIDF="CSVNormal/IDF/";
			FileTitleIDFPerc="CSVNormal/IDF"+Percentages+"/";

		}
		System.out.println(FileTitleFreq);
		new File(FileTitleFreq+"/" + File.separator).mkdirs();
		System.out.println(FileTitleIDF);
		new File(FileTitleIDF+"/" + File.separator).mkdirs();
		System.out.println(FileTitleIDFPerc);
		new File(FileTitleIDFPerc+"/" + File.separator).mkdirs();
	}
	
	static void delete(File f) throws IOException {
		if (f.isDirectory()) {
			for (File c : f.listFiles())
				delete(c);
		}
		if (!f.delete())
			throw new FileNotFoundException("Failed to delete file: " + f);
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
		disconnectDB1();
		return Nodes;
		
	}

	public static void ComputeBiGrams() throws SQLException, IOException {
		// TODO Auto-generated method stub
		connectDB();
		RandomAccessFile csvFreq = new RandomAccessFile(FileTitleFreq+"/" + File.separator + File.separator + dbOriginal + ".csv", "rw");
		//Statement st=con2.createStatement();
		for(int i=0;i<PlayerTeamW("43").size();i++){
			
			Statement st=con4.createStatement();
			String PlayerID = PlayerTeamW("43").get(i);
			String csvString="";
			if(outlier.equals("1")){
			 csvString = "Outlier,";
			}
			else{
				 csvString = "Normal,";
			}
			st.execute("Drop table if exists "+dbOriginal+"_"+PlayerID+"_flat.BigramVector");
			System.out.println("create table "+dbOriginal+"_"+PlayerID+"_flat.BigramVector (id INT NOT NULL, " +
					"Node1 VARCHAR(45) NULL, Node2 VARCHAR(45) NULL, MULT VARCHAR(45) NULL, Primary key(id))");
			st.execute("create table "+dbOriginal+"_"+PlayerID+"_flat.BigramVector (id INT NOT NULL, " +
					"Node1 VARCHAR(45) NULL, Node2 VARCHAR(45) NULL, MULT VARCHAR(45) NULL, Primary key(id))");
			int counter=0;
			for(int j=0; j<createNodeTable().size();j++){
				
				String Node1=createNodeTable().get(j);
				//
				ResultSet rsNode1=st.executeQuery("select distinct `"+Node1+"` from "+dbOriginal+"_CT.`a,b,c_CT`  " );
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
					ResultSet rsNode2=st.executeQuery("select distinct `"+Node2+"` from "+dbOriginal+"_CT.`a,b,c_CT`  ");
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
							
							String Node2V=Node2Values.get(jj);
							String NodeandValues2=Node2+Node2V;
							System.out.println("select sum(MULT) from "+dbOriginal+"_"+PlayerID+"_CT.`a,b,c_CT`" +
									" where `"+ Node1+"` = "+Node1V+" and `"+Node2+"` = "+Node2V);
							ResultSet sumM = st.executeQuery("select sum(MULT) from "+dbOriginal+"_"+PlayerID+"_CT.`a,b,c_CT`" +
									" where `"+ Node1+"` = '"+Node1V+"' and `"+Node2+"` = '"+Node2V+"'");
							
							sumM.next();
							String sumMult = sumM.getString(1);
							if(sumMult!=null){
								ResultSet exists=st.executeQuery("Select * from "+dbOriginal+"_"+PlayerID+"_flat.BigramVector where Node1='"+NodeandValues2
										+"' and Node2='"+NodeandValues1+"'");
								
								if(!exists.next()){
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
									System.out.println("Do Nothing");
								}
							}
							else{
								ResultSet exists=st.executeQuery("Select * from "+dbOriginal+"_"+PlayerID+"_flat.BigramVector where Node1='"+NodeandValues2
										+"' and Node2='"+NodeandValues1+"'");
								
								if(!exists.next()){
								csvString += "0" + ",";
								counter++;
								System.out.println("insert into "+dbOriginal+"_"+PlayerID+"_flat.BigramVector values" +
									"("+ counter+", '"+
									NodeandValues1+"', '"+NodeandValues2+"', "+0+")");
								st.execute("insert into "+dbOriginal+"_"+PlayerID+"_flat.BigramVector values" +
									"("+ counter+", '"+
									NodeandValues1+"', '"+NodeandValues2+"', "+0+")");
							}
								else{
									System.out.println("Do Nothing");
								}
								
								
							}
							
						}
					}
						
				
					
				//	st.execute("");
					
					
					}
				}
			}
			csvString = csvString.substring(0, csvString.length() - 1);
			csvFreq.writeBytes(csvString + "\n");
			
		}
		disconnectDB1();
	}
	
	public static void ComputeBiGramsWODoublicate() throws SQLException, IOException {
		connectDB();
		RandomAccessFile csvFreq = new RandomAccessFile(FileTitleFreq+"/" + File.separator + File.separator + dbOriginal + ".csv", "rw");
	for(int i=0;i<PlayerTeamW("43").size();i++){
			
			Statement st=con4.createStatement();
			String PlayerID = PlayerTeamW("43").get(i);
			String csvString="";
			if(outlier.equals("1")){
			 csvString = "Outlier,";
			}
			else{
				 csvString = "Normal,";
			}
			ResultSet RSnode1 = st.executeQuery("select Node1, Node2 from "+dbOriginal+"_"+PlayerID+"_flat.BigramVector;");
			ArrayList<String> Node1Values = new ArrayList<String>();
			ArrayList<String> Node2Values = new ArrayList<String>();
			while ( RSnode1.next() )
			{
				String Node1Value = RSnode1.getString( 1 );
				Node1Values.add( Node1Value );
				
				String Node2Value = RSnode1.getString( 2 );
				Node2Values.add( Node2Value );
			}
			
			RSnode1.close();
			
			//st.execute("delete from "+dbOriginal+"_"+PlayerID+"_flat.BigramVector where Node1='"+);
			
	
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
			connectDB();
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
	//		ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM imdb_MovieLens_Drama_Sep29_00.selected where class=2;"  );
		//	ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM Premier_League_Synthetic_Bernoulli_SV.Players where PlayerID=239;"  );
		//	ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM Premier_League_MidStr_00.PlayersClass where class=4 and PlayerID in(select PlayerID from  Premier_League_MidStr_00.PlayerFrequence where matchNum>=5);"  );
		//	ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM  Premier_League_Synthetic_Bernoulli_Feature.Players where PlayerID<241 ;"  );
			//SELECT * FROM Premier_League_Strikers_00.PlayerClass
		//	ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM Premier_League_Strikers_00.Feb4Players where PlayerID=10425 ;"  );
			ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM Premier_League_Strikers_00.Feb4Players where class=1 ;");
			//ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM Premier_League_Strikers_00.Feb4Players where class=4 ;"  );
			//ResultSet rs = st1.executeQuery("SELECT PlayerID  FROM Premier_League_MidStr_00.Feb4Players where class=6 ;"  );
			while (rs.next()) {
				PlayerList.add(""+rs.getInt(1));

			}
			  
			st1.close(); 
		
		
		return PlayerList;
		
	}
		
/*		public static void ComputeTfIDFBiGrams() throws SQLException, IOException {
			// TODO Auto-generated method stub
			connectDB();
			RandomAccessFile csvFreq = new RandomAccessFile(FileTitleIDF+"/" + File.separator + File.separator + dbOriginal + ".csv", "rw");
			//Statement st=con2.createStatement();
			for(int i=0;i<PlayerTeamW("43").size();i++){
				
				Statement st=con4.createStatement();
				String PlayerID = PlayerTeamW("43").get(i);
				String csvString="";
				if(outlier.equals("1")){
				 csvString = "Outlier,";
				}
				else{
					 csvString = "Normal,";
				}
				st.execute("Drop table if exists "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVector");
				System.out.println("create table "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVector (id INT NOT NULL, " +
						"Node1 VARCHAR(45) NULL, Node2 VARCHAR(45) NULL, MULT VARCHAR(45) NULL, Primary key(id))");
				st.execute("create table "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVector (id INT NOT NULL, " +
						"Node1 VARCHAR(45) NULL, Node2 VARCHAR(45) NULL, MULT VARCHAR(45) NULL, Primary key(id))");
				int counter=0;
				for(int j=0; j<createNodeTable().size();j++){
					
					String Node1=createNodeTable().get(j);
					//
					ResultSet rsNode1=st.executeQuery("select distinct `"+Node1+"` from "+dbOriginal+"_CT.`a,b,c_CT` where `" +
							Node1+"` <> 'N/A'");
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
						ResultSet rsNode2=st.executeQuery("select distinct `"+Node2+"` from "+dbOriginal+"_CT.`a,b,c_CT` where `"
								+Node2+"` <> 'N/A'");
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
								
								String Node2V=Node2Values.get(jj);
								String NodeandValues2=Node2+Node2V;
								System.out.println("select sum(MULT) from "+dbOriginal+"_"+PlayerID+"_CT.`a,b,c_CT`" +
										" where `"+ Node1+"` = "+Node1V+" and `"+Node2+"` = "+Node2V);
							
								
								ResultSet sumIDf = st.executeQuery("select MULT from "+dbOriginal+"_CT.IDFs" +
										" where  Node1='"+ NodeandValues1+"' and Node2= '" + NodeandValues2+"'");
								sumIDf.next();
								String sumIDF=sumIDf.getString(1);
								
							
								ResultSet sumM = st.executeQuery("select sum(MULT) from "+dbOriginal+"_"+PlayerID+"_CT.`a,b,c_CT`" +
										" where `"+ Node1+"` = '"+Node1V+"' and `"+Node2+"` = '"+Node2V+"'");
								sumM.next();
								String sumMult = sumM.getString(1);
								if(sumMult!=null){
									//csvString += finalValue + ",";
									//counter++;
									double finalValue=Integer.parseInt(sumMult)*Integer.parseInt(TotalNumberOfDocument)/Integer.parseInt(sumIDF);
									csvString += finalValue + ",";
									counter++;
								System.out.println("insert into "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVector values" +
										"("+ counter+", '"+
										NodeandValues1+"', '"+NodeandValues2+"', "+finalValue+")");
								st.execute("insert into "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVector values" +
										"("+ counter+", '"+
										NodeandValues1+"', '"+NodeandValues2+"', "+finalValue+")");
								}
								else{
									csvString += "0" + ",";
									
								}
								
							}
						}
							
					
						
					//	st.execute("");
						
						
						}
					}
				}
				csvString = csvString.substring(0, csvString.length() - 1);
				csvFreq.writeBytes(csvString + "\n");
				
			}
		}*/
		
		public static void ComputeTfIDFBiGrams() throws SQLException, IOException {
			connectDB();
		//	int limit=(int)(Integer.parseInt(TotalNumberOfDocument)*(1-Percentages));
			RandomAccessFile csvFreq = new RandomAccessFile(FileTitleIDF+"/" + File.separator + File.separator + dbOriginal + ".csv", "rw");
			//Statement st=con2.createStatement();
			Statement st=con4.createStatement();
			//where Node1='dribble_eff(Players0,MatchComp0)0'
			ResultSet RSnode1 = st.executeQuery("select distinct Node1, Node2 from "+dbOriginal+"_CT.IDFs ;");
			ArrayList<String> Node1Values = new ArrayList<String>();
			ArrayList<String> Node2Values = new ArrayList<String>();
			while ( RSnode1.next() )
			{
				String Node1Value = RSnode1.getString( 1 );
				Node1Values.add( Node1Value );
				
				String Node2Value = RSnode1.getString( 2 );
				Node2Values.add( Node2Value );
			}
			
			RSnode1.close();
			
			for(int i=0;i<PlayerTeamW("43").size();i++){
				//Statement st=con4.createStatement();
				String PlayerID = PlayerTeamW("43").get(i);
				String csvString="";
				if(outlier.equals("1")){
				 csvString = "Outlier,";
				}
				else{
					 csvString = "Normal,";
				}
				st.execute("Drop table if exists "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVector");
				System.out.println("create table "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVector (id INT NOT NULL, " +
						"Node1 VARCHAR(45) NULL, Node2 VARCHAR(45) NULL, MULT VARCHAR(45) NULL, Primary key(id))");
				st.execute("create table "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVector (id INT NOT NULL, " +
						"Node1 VARCHAR(45) NULL, Node2 VARCHAR(45) NULL, MULT VARCHAR(45) NULL, Primary key(id))");
				int counter=0;
				for(int j=0; j<Node1Values.size();j++){
				
						
						ResultSet sumM = st.executeQuery("select MULT from "+dbOriginal+"_"+PlayerID+"_flat.BigramVector" +
								" where  Node1='"+ Node1Values.get(j)+"' and Node2= '" + Node2Values.get(j)+"'");
						
						if(sumM.next()){
						String sumMult = sumM.getString(1);
						if(!sumMult.equals("0")){
							System.out.println("select MULT from "+dbOriginal+"_CT.IDFs" +
									" where  Node1='"+ Node1Values.get(j)+"' and Node2= '" + Node2Values.get(j)+"'");
						ResultSet sumIDf = st.executeQuery("select MULT from "+dbOriginal+"_CT.IDFs" +
								" where  Node1='"+ Node1Values.get(j)+"' and Node2= '" + Node2Values.get(j)+"'");

						sumIDf.next();
						String sumIDF=sumIDf.getString(1);
						System.out.println("SumIDF"+sumIDF);
						System.out.println("sumMult"+sumMult);
					//	if(sumMult!=null){
							//csvString += finalValue + ",";
							//counter++;
						System.out.println("total:"+Double.parseDouble(TotalNumberOfDocument));
						System.out.println("ntotal:"+Double.parseDouble(sumIDF));
						System.out.println("devision="+Double.parseDouble(TotalNumberOfDocument)/Double.parseDouble(sumIDF));
						System.out.println(Math.log10(Double.parseDouble(TotalNumberOfDocument)/Double.parseDouble(sumIDF)));
							double finalValue=Integer.parseInt(sumMult)*Math.log10(Double.parseDouble(TotalNumberOfDocument)/Double.parseDouble(sumIDF));
							csvString += finalValue + ",";
							counter++;
						System.out.println("insert into "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVector values" +
								"("+ counter+", '"+
								Node1Values.get(j)+"', '"+Node2Values.get(j)+"', "+finalValue+")");
						st.execute("insert into "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVector values" +
								"("+ counter+", '"+
								Node1Values.get(j)+"', '"+Node2Values.get(j)+"', "+finalValue+")");
						}
						else{
							counter++;
							csvString += "0" + ",";
							st.execute("insert into "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVector values" +
									"("+ counter+", '"+
									Node1Values.get(j)+"', '"+Node2Values.get(j)+"', "+0+")");
						}
						}
					
				//		else{
					//		csvString += "0" + ",";
							
					//	}
						
					
				}
				csvString = csvString.substring(0, csvString.length() - 1);
				csvFreq.writeBytes(csvString + "\n");
			}
		}

		public static void TopNPercentIDF() throws SQLException, IOException {
			connectDB();
			int limit=(int)(Integer.parseInt(TotalNumberOfDocument)*(1-Percentages));
			RandomAccessFile csvFreq = new RandomAccessFile(FileTitleIDFPerc+"/" + File.separator + File.separator + dbOriginal + ".csv", "rw");
			//Statement st=con2.createStatement();
			Statement st=con4.createStatement();
			
			ResultSet RSnode1 = st.executeQuery("select distinct Node1, Node2 from "+dbOriginal+"_CT.IDFs where MULT<="+limit);
			ArrayList<String> Node1Values = new ArrayList<String>();
			ArrayList<String> Node2Values = new ArrayList<String>();
			while ( RSnode1.next() )
			{
				String Node1Value = RSnode1.getString( 1 );
				Node1Values.add( Node1Value );
				
				String Node2Value = RSnode1.getString( 2 );
				Node2Values.add( Node2Value );
			}
			
			RSnode1.close();
			
			for(int i=0;i<PlayerTeamW("43").size();i++){
				//Statement st=con4.createStatement();
				String PlayerID = PlayerTeamW("43").get(i);
				String csvString="";
				if(outlier.equals("1")){
				 csvString = "Outlier,";
				}
				else{
					 csvString = "Normal,";
				}
				st.execute("Drop table if exists "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVectorPerCentages");
				System.out.println("create table "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVectorPerCentages (id INT NOT NULL, " +
						"Node1 VARCHAR(45) NULL, Node2 VARCHAR(45) NULL, MULT VARCHAR(45) NULL, Primary key(id))");
				st.execute("create table "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVectorPerCentages (id INT NOT NULL, " +
						"Node1 VARCHAR(45) NULL, Node2 VARCHAR(45) NULL, MULT VARCHAR(45) NULL, Primary key(id))");
				int counter=0;
				for(int j=0; j<Node1Values.size();j++){
				
						
					ResultSet sumM = st.executeQuery("select MULT from "+dbOriginal+"_"+PlayerID+"_flat.BigramVector" +
							" where  Node1='"+ Node1Values.get(j)+"' and Node2= '" + Node2Values.get(j)+"'");

					if(sumM.next()){
					String sumMult = sumM.getString(1);
					ResultSet sumIDf = st.executeQuery("select MULT from "+dbOriginal+"_CT.IDFs" +
							" where  Node1='"+ Node1Values.get(j)+"' and Node2= '" + Node2Values.get(j)+"'");

					sumIDf.next();
					String sumIDF=sumIDf.getString(1);
					System.out.println("SumIDF"+sumIDF);
					System.out.println("sumMult"+sumMult);
				//	if(sumMult!=null){
						//csvString += finalValue + ",";
						//counter++;
					System.out.println(Math.log10(Double.parseDouble(TotalNumberOfDocument)/Double.parseDouble(sumIDF)));
						double finalValue=Integer.parseInt(sumMult)*Math.log10(Double.parseDouble(TotalNumberOfDocument)/Double.parseDouble(sumIDF));
						csvString += finalValue + ",";
						counter++;
					System.out.println("insert into "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVectorPerCentages values" +
							"("+ counter+", '"+
							Node1Values.get(j)+"', '"+Node2Values.get(j)+"', "+finalValue+")");
					st.execute("insert into "+dbOriginal+"_"+PlayerID+"_flat.TFIDFVectorPerCentages values" +
							"("+ counter+", '"+
							Node1Values.get(j)+"', '"+Node2Values.get(j)+"', "+finalValue+")");
					}
					else{
						csvString += "0" + ",";
						
					}
						
					
				}
				csvString = csvString.substring(0, csvString.length() - 1);
				csvFreq.writeBytes(csvString + "\n");
			}
		}

}
