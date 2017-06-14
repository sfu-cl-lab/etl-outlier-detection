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
import org.jaxen.function.SubstringAfterFunction;


public class PlayerFlat {
	static Connection con1,con2;
	static String ResultDB,dbOriginal,dbBN,dbSchema,GenericDB;
	static String dbUsername;
	static String dbPassword;
	static String dbaddress, outlier;
	static String FileTitleFreq="";
	static String FileTitleLog="";
	static String FileTitleLoglikelihood="";
	static String FileTitlePrior="";
	static String FileTitleMI="";
	static String FileTitleMIPrior="";
	static String FileTitleCPFlat="";
	static String sumIDF;
	static String sumCP;
	static String sumPrior;
	static String ComputationMode;//ComputationMode=1 -> freq  ComputationMode=2->log ComputationMode=3->Loglikelihood computationMode=4-> MID
	public static void main(String[] args) throws Exception {
		//read config file
		 PlayerFlat();
		//createSingleFamily("dribble_eff(Teams0,MatchComp0)_a,b,c_local_CT","dribble_eff(Teams0,MatchComp0)");

		
	}
	
	 public static void PlayerFlat() throws Exception {
			setVarsFromConfig();
			connectDB();
			init();
			FetchCTTables();
			disconnectDB2();
	 }
	static void init(){
	
		try{ 
			delete(new File(FileTitleFreq+dbOriginal+"/"));
			delete(new File(FileTitleLog+dbOriginal+"/"));
			delete(new File(FileTitleLoglikelihood+dbOriginal+"/"));
			delete(new File(FileTitleMI+dbOriginal+"/"));
			delete(new File(FileTitlePrior+dbOriginal+"/"));
			delete(new File(FileTitleMIPrior+dbOriginal+"/"));
			delete(new File(FileTitleCPFlat+dbOriginal+"/"));
		}catch (Exception e){
			
			
		}

		if(outlier.equals("1")){
			System.out.println("We are yhere");
			FileTitleFreq="CSVOutlier/freq/";
			FileTitleLog="CSVOutlier/log/";
			FileTitleLoglikelihood="CSVOutlier/Loglikelihood/";
			FileTitleMI="CSVOutlier/freqIDF/";
			FileTitlePrior="CSVOutlier/priorIDF/";
			FileTitleMIPrior="CSVOutlier/CPIDF/";
			FileTitleCPFlat="CSVOutlier/CPFlat/";
			
		}
		else{
			//FileTitle="CSVNormal/";
			System.out.println("outlierr="+outlier);
			System.out.println("We are not yhere");
			FileTitleFreq="CSVNormal/freq/";
			FileTitleLog="CSVNormal/log/";
			FileTitleLoglikelihood="CSVNormal/Loglikelihood/";
			FileTitleMI="CSVNormal/freqIDF/";
			FileTitlePrior="CSVNormal/priorIDF/";
			FileTitleMIPrior="CSVNormal/CPIDF/";
			FileTitleCPFlat="CSVNormal/CPFlat/";
		}
		new File(FileTitleFreq+dbOriginal+"/" + File.separator).mkdirs();
		new File(FileTitleLog+dbOriginal+"/" + File.separator).mkdirs();
		new File(FileTitleLoglikelihood+dbOriginal+"/" + File.separator).mkdirs();
		new File(FileTitleMI+dbOriginal+"/" + File.separator).mkdirs();
		new File(FileTitlePrior+dbOriginal+"/" + File.separator).mkdirs();
		new File(FileTitleMIPrior+dbOriginal+"/" + File.separator).mkdirs();
		new File(FileTitleCPFlat+dbOriginal+"/" + File.separator).mkdirs();
	}
	static void delete(File f) throws IOException {
		if (f.isDirectory()) {
			for (File c : f.listFiles())
				delete(c);
		}
		if (!f.delete())
			throw new FileNotFoundException("Failed to delete file: " + f);
	}

	public static void FetchCTTables() throws SQLException, IOException{
		connectDB();
		Statement st=con2.createStatement();
		st.execute("drop schema if exists "+ResultDB);
		st.execute("Create schema "+ResultDB);
		st.execute("Drop table if exists "+ResultDB+".ColumnCount");
		
		st.execute("Create table if not exists "+ResultDB+".ColumnCount(  `ctName`  VARCHAR(45) NOT NULL , " +
				
				
				" `colCount` int NULL ,  PRIMARY KEY (`ctName`) ) ");
		
		st.execute("Drop table if exists "+ResultDB+".CPColumnCount");
		
		st.execute("Create table if not exists "+ResultDB+".CPColumnCount(  `cpName`  VARCHAR(45) NOT NULL , " +
				
				
				" `colCount` int NULL ,  PRIMARY KEY (`cpName`) ) ");
		st.execute("drop view if exists "+dbBN+".Players_Nodes");
		st.execute("Create view "+dbBN+".Players_Nodes as select * from "+dbBN+".FNodes_pvars where pvid='movies0';");
		st.execute("Drop view if exists "+dbBN+".CTNodes;");
		System.out.println("Create view "+dbBN+".CTNodes as select child as Node from "+dbBN+".Final_Path_BayesNets where child in (select FID from "
				+dbBN+".FNodes_pvars where pvid='movies0') union select parent as node from "+dbBN+".Final_Path_BayesNets where " +
				"parent in (select FID from "
		+dbBN+".FNodes_pvars where pvid='movies0')");
		st.execute("Create view "+dbBN+".CTNodes as select child as Node from "+dbBN+".Final_Path_BayesNets where child in (select FID from "
				+dbBN+".FNodes_pvars where pvid='movies0') union select parent as node from "+dbBN+".Final_Path_BayesNets where " +
						"parent in (select FID from "
				+dbBN+".FNodes_pvars where pvid='movies0')");
		
		ResultSet CT=st.executeQuery("select distinct Node from CTNodes ;");
		ArrayList<String> fids = new ArrayList<String>();
		while ( CT.next() )
		{
			String fid = CT.getString( 1 );
			fids.add( fid );
		}
		
		CT.close();
		for(int i=0; i<fids.size();i++){
			 String s= fids.get(i).substring(1, fids.get(i).length() - 1);
			st.execute("create table "+ResultDB+".`"+s+"_a,b,c_local_CT` like "+dbSchema+".`"+s+"_a,b,c_local_CT`");
			
			st.execute("insert into "+ResultDB+".`"+s+"_a,b,c_local_CT` select * from "+dbSchema+".`"+s+"_a,b,c_local_CT`");
			st.execute("update "+ResultDB+".`"+s+"_a,b,c_local_CT` set MULT=MULT/2;");
			String ctTable=s+"_a,b,c_local_CT";
			st.execute("create table "+ResultDB+".`"+s+"_local_CP` like "+dbSchema+".`"+s+"_local_CP`");
			st.execute("insert into "+ResultDB+".`"+s+"_local_CP` select * from "+dbSchema+".`"+s+"_local_CP`");
			String cpTable=s+"_local_CP";
			System.out.println("Select * from information_schema.COLUMNS where " +
					" TABLE_SCHEMA ='"+ResultDB+"' and TABLE_NAME='"+s+"_local_CP' and COLUMN_NAME='a'");
			ResultSet rs = st.executeQuery("Select * from information_schema.COLUMNS where " +
					" TABLE_SCHEMA ='"+ResultDB+"' and TABLE_NAME='"+s+"_local_CP' and COLUMN_NAME='a'");
			if(rs.isBeforeFirst()){
				st.execute("Alter table "+ResultDB+".`"+s+"_local_CP` drop column a");
			}
			
			ResultSet rs2 = st.executeQuery("Select * from information_schema.COLUMNS where " +
					" TABLE_SCHEMA ='"+ResultDB+"' and TABLE_NAME='"+s+"_local_CP' and COLUMN_NAME='b'");
			if(rs2.isBeforeFirst()){
				st.execute("Alter table "+ResultDB+".`"+s+"_local_CP` drop column b");
			}
			st.execute("alter table "+ResultDB+".`"+s+"_local_CP` add a varchar(1)");
			st.execute("alter table "+ResultDB+".`"+s+"_local_CP` add b varchar(1)");
			st.execute("update "+ResultDB+".`"+s+"_local_CP` set a='T'");
			st.execute("update "+ResultDB+".`"+s+"_local_CP` set b='T'");
			/*lter table `mytable` 
change column username username varchar(255) after `somecolumn`;*/
			//	System.out.println("alter table "+ResultDB+".`"+s+"_local_CP` change column CP bigint(21) after `b`" );
			st.execute("alter table "+ResultDB+".`"+s+"_local_CP` modify column CP FLOAT(7,6) after `b` ");
			st.execute("alter table "+ResultDB+".`"+s+"_local_CP` modify column prior FLOAT(7,6) after `CP` ");
			
			ResultSet rsSumMult=st.executeQuery("Select sum(Mult) from `"+GenericDB+"_flat`.`"+ctTable+" ;");
			rsSumMult.next();
			sumIDF=rsSumMult.getString(1);
			
			ResultSet rsSumCP=st.executeQuery("Select sum(CP) from `"+GenericDB+"_flat`.`"+cpTable+" ;");
			rsSumCP.next();
			sumCP=rsSumCP.getString(1);
			
			ResultSet rsSumPrior=st.executeQuery("Select sum(prior) from `"+GenericDB+"_flat`.`"+cpTable+" ;");
			rsSumPrior.next();
			sumPrior=rsSumPrior.getString(1);
			
			SmoothCT(ctTable, s);
			SmoothCP(cpTable,s);
			createSingleFamilyCP(cpTable,s);
			createSingleFamilyCount(ctTable,s);
			//createSingleFamily(ctTable,s); Feb20
			showColumns(ctTable,s);
			showColumnsCP(cpTable,s);
			WriteinCSV(s);
		
		}
		disconnectDB2();
	}
	public static void SmoothCT(String ctTable, String s)throws SQLException {
		// TODO Auto-generated method stub
		ConnectDBResult();  
		Statement st = con1.createStatement();
		String Query="show columns from `"+ctTable+"`";
				
		System.out.println(Query);
		ResultSet rst = st.executeQuery(Query);
		ArrayList<String> colList = new ArrayList<String>();
		while(rst.next()) colList.add(rst.getString(1));
		colList.remove(0);
		String whereList="";
		String SecondwhereList="";
		/*CREATE  TABLE `Premier_League_Strikers_103955_flat`.`ColumnCount` (
  `ctName` INT NOT NULL ,
  `columnCount` VARCHAR(45) NULL ,
  PRIMARY KEY (`ctName`) );*/


		for(int i=0; i<colList.size();i++){
			String sr=colList.get(i);
			sr="`"+sr+"`";
			System.out.println(sr);
			whereList=whereList+", "+sr;
			if(!sr.equals("`a`")&&!sr.equals("`b`")){
				SecondwhereList=sr+"='N/A' or "+SecondwhereList;
				
			}
		}
		whereList=whereList.substring(1, whereList.length());
		SecondwhereList=SecondwhereList.substring(0, SecondwhereList.length()-4);
		System.out.println("Secondwherelist= "+SecondwhereList);
		System.out.println("Wherelist"+whereList);
		
		
		
		String subOrigin=dbOriginal.replaceAll("\\d+", "");
		 String Origin=subOrigin.substring(0, subOrigin.length()-1);
	
		// Origin=Origin+"10";
		 System.out.println("Originnn"+Origin);
		System.out.println("dbOrigin="+dbOriginal+" Origin="+Origin+"  subOrigin="+subOrigin);
		String subQuery="create table if not exists `"+ctTable+"_smoothed` as "+" select distinct "+whereList+" from `"+Origin+"_CT`.`a,b,c_CT` " +
				"where ("+whereList+") not in (select "+whereList+" from `"+ctTable+"`) group by "+whereList;
		System.out.println("Delete from `"+ctTable+"_smoothed` where "+SecondwhereList);
	
		
	System.out.println(subQuery);
		st.execute(subQuery);
	//	st.execute("Delete from `"+ctTable+"_smoothed` where "+SecondwhereList);//comment this to have more combination Dec 29
		st.execute("Alter table `"+ctTable+"_smoothed` add MULT bigint(21);");
		st.execute("Alter table `"+ctTable+"_smoothed` modify MULT bigint(21) first");
		st.execute("Update `"+ctTable+"_smoothed` set MULT=0");
		
		st.execute("insert into `"+ctTable+"` select * from `"+ctTable+"_smoothed`");
		


	disconnectDB1();
	}
	
	public static void SmoothCP(String cpTable, String s)throws SQLException {
		// TODO Auto-generated method stub
		ConnectDBResult();  
		Statement st = con1.createStatement();
		String Query="show columns from `"+cpTable+"`";
				
		System.out.println(Query);
		ResultSet rst = st.executeQuery(Query);
		ArrayList<String> colList = new ArrayList<String>();
		while(rst.next()) colList.add(rst.getString(1));
		colList.remove("CP");
		colList.remove("prior");
		String whereList="";
		String SecondwhereList="";
		/*CREATE  TABLE `Premier_League_Strikers_103955_flat`.`ColumnCount` (
  `ctName` INT NOT NULL ,
  `columnCount` VARCHAR(45) NULL ,
  PRIMARY KEY (`ctName`) );*/


		for(int i=0; i<colList.size();i++){
			String sr=colList.get(i);
			sr="`"+sr+"`";
			System.out.println(sr);
			whereList=whereList+", "+sr;
			if(!sr.equals("`a`")&&!sr.equals("`b`")){
				SecondwhereList=sr+"='N/A' or "+SecondwhereList;
				
			}
		}
		whereList=whereList.substring(1, whereList.length());
		SecondwhereList=SecondwhereList.substring(0, SecondwhereList.length()-4);
		System.out.println("Secondwherelist= "+SecondwhereList);
		System.out.println("Wherelist"+whereList);
		
		
		
		String subOrigin=dbOriginal.replaceAll("\\d+", "");
		 String Origin=subOrigin.substring(0, subOrigin.length()-1);
		// Origin=Origin+"10";
		System.out.println("dbOrigin="+dbOriginal+" Origin="+Origin+"  subOrigin="+subOrigin);
		String subQuery="create table if not exists `"+cpTable+"_smoothed` as "+" select "+whereList+" from `"+Origin+"_CT`.`a,b,c_CT` " +
				"where ("+whereList+") not in (select "+whereList+" from `"+cpTable+"`) group by "+whereList;
		System.out.println("Delete from `"+cpTable+"_smoothed` where "+SecondwhereList);
	
		
	System.out.println(subQuery);
		st.execute(subQuery);
		//st.execute("Delete from `"+cpTable+"_smoothed` where "+SecondwhereList);
		st.execute("Alter table `"+cpTable+"_smoothed` add CP bigint(21);");
		st.execute("Alter table `"+cpTable+"_smoothed` add prior bigint(21);");
		//st.execute("Alter table `"+cpTable+"_smoothed` modify CP bigint(21) first");
		st.execute("Update `"+cpTable+"_smoothed` set CP=0");
		st.execute("Update `"+cpTable+"_smoothed` set prior=0");
		System.out.println("insert into `"+cpTable+"` select * from `"+cpTable+"_smoothed`");
		st.execute("insert into `"+cpTable+"` select * from `"+cpTable+"_smoothed`");
		


	disconnectDB1();
	}
public static void showColumns(String ctTable, String s) throws SQLException{
	ConnectDBResult();  
	Statement st = con1.createStatement();
	String QueryS="show columns from `"+s+"_a,b,c_flat`";
	
	System.out.println(QueryS);
	ResultSet rstS = st.executeQuery(QueryS);
	ArrayList<String> colListS = new ArrayList<String>();
	while(rstS.next()) colListS.add(rstS.getString(1));
	System.out.println(" insert into ColumnCount values ('"+s+"', "+colListS.size()+")");
	st.execute(" insert into ColumnCount values ('"+s+"', "+colListS.size()+")");
disconnectDB1();
}

public static void showColumnsCP(String cpTable, String s) throws SQLException{
	ConnectDBResult();  
	Statement st = con1.createStatement();
	String QueryS="show columns from `"+s+"_CP_flat`";
	
	System.out.println(QueryS);
	ResultSet rstS = st.executeQuery(QueryS);
	ArrayList<String> colListS = new ArrayList<String>();
	while(rstS.next()) colListS.add(rstS.getString(1));
	System.out.println(" insert into CPColumnCount values ('"+s+"', "+colListS.size()+")");
	st.execute(" insert into CPColumnCount values ('"+s+"', "+colListS.size()+")");
disconnectDB1();
}
	public static void WriteinCSV(String ctTable) throws SQLException, IOException {
		// TODO Auto-generated method stub
		ConnectDBResult();  
		Statement st = con1.createStatement();
		Statement stCP = con1.createStatement();
		Statement stPrior = con1.createStatement();
		String queryString= "SELECT * FROM `"+ctTable+"_a,b,c_flat` ;";
		ResultSet rs4 = st.executeQuery(queryString);
		System.out.print("query string : "+queryString);
		ArrayList<String> columns = getColumns(rs4);
		
		String queryStringCP= "SELECT * FROM `"+ctTable+"_CP_flat` ;";
		ResultSet rsCP = stCP.executeQuery(queryStringCP);
		System.out.print("query string : "+queryStringCP);
		ArrayList<String> columnsCP = getColumns(rsCP);
		
		String queryStringPrior= "SELECT * FROM `"+ctTable+"_Prior_flat` ;";
		ResultSet rsPrior = stPrior.executeQuery(queryStringPrior);
		System.out.print("query string : "+queryStringPrior);
		ArrayList<String> columnsPrior = getColumns(rsPrior);
	//	String csvHeader = StringUtils.join(columns, "\t");
		//System.out.println("\nCSV Header : " + csvHeader+ "\n");

		//  create csv file
	
		RandomAccessFile csvFreq = new RandomAccessFile(FileTitleFreq+dbOriginal+"/" + File.separator + File.separator + ctTable + ".csv", "rw");

		RandomAccessFile csvLog = new RandomAccessFile(FileTitleLog+dbOriginal+"/" + File.separator + File.separator + ctTable + ".csv", "rw");
		RandomAccessFile csvLoglikelihood = new RandomAccessFile(FileTitleLoglikelihood+dbOriginal+"/" + File.separator + File.separator + ctTable + ".csv", "rw");
		RandomAccessFile csvMI = new RandomAccessFile(FileTitleMI+dbOriginal+"/" + File.separator + File.separator + ctTable + ".csv", "rw");
		RandomAccessFile csvPrior = new RandomAccessFile(FileTitlePrior+dbOriginal+"/" + File.separator + File.separator + ctTable + ".csv", "rw");
		RandomAccessFile csvMIPrior = new RandomAccessFile(FileTitleMIPrior+dbOriginal+"/" + File.separator + File.separator + ctTable + ".csv", "rw");
		RandomAccessFile csvCPFlat= new RandomAccessFile(FileTitleCPFlat+dbOriginal+"/" + File.separator + File.separator + ctTable + ".csv", "rw");
		//	csv.writeBytes(csvHeader + "\n");
		
		ResultSet rs5 = st.executeQuery(queryString);
		
		FrequencyFormat( rs5, csvFreq,columns);
		
		ResultSet rs6 = st.executeQuery(queryString);
		LogFormat(rs6, csvLog, columns);
		
		ResultSet rs7 = st.executeQuery(queryString);
		LoglikelihoodFormat(rs7,rsCP, csvLoglikelihood,columnsCP,columns );
		
		ResultSet rs8 = st.executeQuery(queryString);
		String queryStringCTG= "SELECT * FROM `"+GenericDB+"_flat`.`"+ctTable+"_a,b,c_flat` ;";
		ResultSet rsCP2 = stCP.executeQuery(queryStringCTG);
		ArrayList<String> columnsCP2 = getColumns(rsCP2);
		
	
		freqIDF(rs8,rsCP2, csvMI,columnsCP2,columns,sumIDF);
		
		String queryStringCPG= "SELECT * FROM `"+GenericDB+"_flat`.`"+ctTable+"_CP_flat` ;";
		ResultSet rs9 = st.executeQuery(queryStringCPG);
		String queryStringCPS= "SELECT * FROM `"+ctTable+"_CP_flat` ;";
		ResultSet rsCP3 = stCP.executeQuery(queryStringCPS);
		ArrayList<String> columnsCP3 = getColumns(rs9);
		
		CpIDF(rsCP3,rs9, csvMIPrior,columnsCP3,columns,sumCP);
		
		
		String queryStringPriorG= "SELECT * FROM `"+GenericDB+"_flat`.`"+ctTable+"_Prior_flat` ;";
		System.out.println("SELECT * FROM `"+GenericDB+"_flat`.`"+ctTable+"_Prior_flat` ;");
		ResultSet rs10 = st.executeQuery(queryStringPriorG);
		String queryStringPriorS= "SELECT * FROM `"+ctTable+"_Prior_flat` ;";
		System.out.println("SELECT * FROM `"+ctTable+"_Prior_flat` ;");
		ResultSet rsCP4 = stCP.executeQuery(queryStringPriorS);
		ArrayList<String> columnsCP4 = getColumns(rs10);
		
		PriorIDF(rsCP4,rs10, csvPrior,columnsCP4,columns,sumPrior);
		
		

		ResultSet rsCPf = stCP.executeQuery(queryStringCP);
		//ArrayList<String> columnsCPflat = getColumns(cpflatst);
		cpFlatGenerator(rsCPf, csvCPFlat,columns);
		
	/*	ResultSet rs9 = st.executeQuery(queryString);
		ResultSet rsCP3 = stCP.executeQuery(queryStringCP);
		ArrayList<String> columnsCP3 = getColumns(rsCP3);
		CpIDF(rs8,rsCP2,rsPrior, csvMIPrior,columnsCP3,columns,columnsPrior);*/
		
		
	/*	
		ResultSet rsPrior2 = stPrior.executeQuery(queryStringPrior);
		PriorIDF(rsPrior2,csvPrior,columnsPrior);
	*/
		
		
		//  add to ids for further use
	
		
		//  close statements
		st.close();
		disconnectDB1();
	}
	public static void cpFlatGenerator(ResultSet rs,
			RandomAccessFile csvFreq, ArrayList<String> columns) throws IOException, NumberFormatException, SQLException {
			while(rs.next())
			{
				String csvString = "";
				for(String col : columns)
				{
					if(rs.getString(col)!=null){
					double value=Double.parseDouble(rs.getString(col) );
					String valueToString=String.valueOf(round(value, 2));
					csvString += valueToString + ",";
					
					}
					else{
						double value=0;
						String valueToString=String.valueOf(round(value, 2));
						csvString += valueToString + ",";
					}
					
				}
				
				csvString = csvString.substring(0, csvString.length() - 1);
				csvFreq.writeBytes(csvString + "\n");
			}
			csvFreq.close();
			
		
	}
/*Feb 20 
 * 
 * 	public static void freqIDF(ResultSet rs, ResultSet rsCP, RandomAccessFile csv, ArrayList<String> columnsCP, ArrayList<String> columns) throws NumberFormatException, SQLException, IOException {
		// TODO Auto-generated method stub
		while(rs.next())
		{
			String csvString = "";
			for(String col : columnsCP)
			{
				//csvString += rs5.getString(col) + ",";
				if(rs.getString(col)!=null){
				double freq=Double.parseDouble(rs.getString(col) );
				double freqG=Double.parseDouble(rsCP.getString(col));
				double logValue=0;
				double cpPrior=freq/freqG;
				if(cpPrior!=0){
					logValue=log2(cpPrior);
				}
				else{
					logValue=0;
				}		
				double loglikelihood=logValue;
				String valueToString=String.valueOf(round(loglikelihood, 2));		
				csvString += valueToString + ",";
				}
				else{
					double value=0;
					String valueToString=String.valueOf(round(value, 2));
					csvString += valueToString + ",";
				}
				
				
			}
		//	System.out.println(csvString+" csvStringggg");
			csvString = csvString.substring(0, csvString.length() - 1);
			csv.writeBytes(csvString + "\n");
		}
		
		csv.close();
	}

 * 
 * 
 * Feb20*/
	public static void freqIDF(ResultSet rs, ResultSet rsCP, RandomAccessFile csv, ArrayList<String> columnsCP, ArrayList<String> columns, String SumIDF) throws NumberFormatException, SQLException, IOException {
		// TODO Auto-generated method stub
		while(rs.next())
		{
			String csvString = "";
			for(String col : columnsCP)
			{
				//csvString += rs5.getString(col) + ",";
				if(rs.getString(col)!=null){
				double freq=Double.parseDouble(rs.getString(col) );
				double freqG=Double.parseDouble(rsCP.getString(col));
				double sumMult=Double.parseDouble(SumIDF);
				double logValue=0;
				double cpPrior=freqG/sumMult;
				if(cpPrior!=0){
					logValue=log2(cpPrior);
				}
				else{
					logValue=0;
				}		
				double loglikelihood=logValue*freq;
				String valueToString=String.valueOf(round(loglikelihood, 2));		
				csvString += valueToString + ",";
				}
				else{
					double value=0;
					String valueToString=String.valueOf(round(value, 2));
					csvString += valueToString + ",";
				}
				
				
			}
		//	System.out.println(csvString+" csvStringggg");
			csvString = csvString.substring(0, csvString.length() - 1);
			csv.writeBytes(csvString + "\n");
		}
		
		csv.close();
	}
/*
 * feb20 
 * 
 * 	public static void CpIDF(ResultSet rs, ResultSet rsCP,RandomAccessFile csv, ArrayList<String> columnsCP, ArrayList<String> columns) throws NumberFormatException, SQLException, IOException {
		// TODO Auto-generated method stub
		while(rs.next())
		{
			System.out.println("hello");
			String csvString = "";
			for(String col : columnsCP)
			{
				//csvString += rs5.getString(col) + ",";
				if(rs.getString(col)!=null){
				double CP=Double.parseDouble(rs.getString(col) );
				double CPG=Double.parseDouble(rsCP.getString(col));
				double logValue=0;
				double cpPrior=CP/CPG;
				if(cpPrior!=0){
					logValue=log2(cpPrior);
				}
				else{
					logValue=0;
				}		
				double loglikelihood=logValue;
				String valueToString=String.valueOf(round(loglikelihood, 2));		
				csvString += valueToString + ",";
				}
				else{
					double value=0;
					String valueToString=String.valueOf(round(value, 2));
					csvString += valueToString + ",";
				}
			}
	
			
			
		//	System.out.println(csvString+" csvStringggg");
			csvString = csvString.substring(0, csvString.length() - 1);
			csv.writeBytes(csvString + "\n");
		}
		
		csv.close();
	}
 * 
 * feb20*/
	public static void CpIDF(ResultSet rs, ResultSet rsCP,RandomAccessFile csv, ArrayList<String> columnsCP, ArrayList<String> columns,String SumCP) throws NumberFormatException, SQLException, IOException {
		// TODO Auto-generated method stub
		while(rs.next())
		{
			System.out.println("hello");
			String csvString = "";
			for(String col : columnsCP)
			{
				//csvString += rs5.getString(col) + ",";
				if(rs.getString(col)!=null){
				double CP=Double.parseDouble(rs.getString(col) );
				double CPG=Double.parseDouble(rsCP.getString(col));
				double sumcp=Double.parseDouble(sumCP);
				double logValue=0;
				double cpPrior=CPG/sumcp;
				if(cpPrior!=0){
					logValue=log2(cpPrior);
				}
				else{
					logValue=0;
				}		
				double loglikelihood=CP*logValue;
				String valueToString=String.valueOf(round(loglikelihood, 2));		
				csvString += valueToString + ",";
				}
				else{
					double value=0;
					String valueToString=String.valueOf(round(value, 2));
					csvString += valueToString + ",";
				}
			}
	
			
			
		//	System.out.println(csvString+" csvStringggg");
			csvString = csvString.substring(0, csvString.length() - 1);
			csv.writeBytes(csvString + "\n");
		}
		
		csv.close();
	}

	public static void LoglikelihoodFormat(ResultSet rs, ResultSet rsCP,RandomAccessFile csv, ArrayList<String> columnsCP,ArrayList<String> columns) throws NumberFormatException, SQLException, IOException {
		while(rs.next())
		{
			String csvString = "";
			for(String col : columns)
			{
				//csvString += rs5.getString(col) + ",";
				if(rs.getString(col)!=null){
				double freq=Double.parseDouble(rs.getString(col) );
				double CP=Double.parseDouble(rsCP.getString(col));
				double logValue=0;
				if(CP!=0){
					logValue=log2(CP);
				}
				else{
					logValue=0;
				}		
				double loglikelihood=logValue*freq;
				String valueToString=String.valueOf(round(loglikelihood, 2));		
				csvString += valueToString + ",";
				}
				else{
					double value=0;
					String valueToString=String.valueOf(round(value, 2));
					csvString += valueToString + ",";
				}
				
			}
			
			csvString = csvString.substring(0, csvString.length() - 1);
			csv.writeBytes(csvString + "\n");
		}
		
		csv.close();
		
	}

	public static double logb( double a, double b )
	{
	return Math.log(a) / Math.log(b);
	}
	
	public static double log2( double a )
	{
	return logb(a,2);
	}
	public static void FrequencyFormat(ResultSet rs,RandomAccessFile csv,ArrayList<String> columns) throws IOException, NumberFormatException, SQLException{
		while(rs.next())
		{
			String csvString = "";
			for(String col : columns)
			{
				if(rs.getString(col)!=null){
				double value=Double.parseDouble(rs.getString(col) );
				String valueToString=String.valueOf(round(value, 2));
				csvString += valueToString + ",";
				
				}
				else{
					double value=0;
					String valueToString=String.valueOf(round(value, 2));
					csvString += valueToString + ",";
				}
				
			}
			
			csvString = csvString.substring(0, csvString.length() - 1);
			csv.writeBytes(csvString + "\n");
		}
		csv.close();
	}

	public static void LogFormat(ResultSet rs,RandomAccessFile csv,ArrayList<String> columns) throws IOException, NumberFormatException, SQLException{
		while(rs.next())
		{
			String csvString = "";
			for(String col : columns)
			{
				//csvString += rs5.getString(col) + ",";
				if(rs.getString(col)!=null){
				double value=Double.parseDouble(rs.getString(col) );
				double logValue=0;
				if(value!=0){
					logValue=log2(value);
				}
				else{
					logValue=0;
				}		
				String valueToString=String.valueOf(round(logValue, 2));		
				csvString += valueToString + ",";
				}
				else{
					double value=0;
					String valueToString=String.valueOf(round(value, 2));
					csvString += valueToString + ",";
				}
				
			}
			
			csvString = csvString.substring(0, csvString.length() - 1);
			csv.writeBytes(csvString + "\n");
		}
		
		csv.close();
		
	}
	
/*Feb 20			public static void PriorIDF(ResultSet rs, ResultSet rsCP,RandomAccessFile csv, ArrayList<String> columnsCP, ArrayList<String> columns) throws IOException, NumberFormatException, SQLException{
		while(rs.next())
		{
			System.out.println("hello");
			String csvString = "";
			for(String col : columnsCP)
			{
				//csvString += rs5.getString(col) + ",";
				if(rs.getString(col)!=null){
				double CP=Double.parseDouble(rs.getString(col) );
				double CPG=Double.parseDouble(rsCP.getString(col));
				double logValue=0;
				double cpPrior=CP/CPG;
				if(cpPrior!=0){
					logValue=log2(cpPrior);
				}
				else{
					logValue=0;
				}		
				double loglikelihood=logValue;
				String valueToString=String.valueOf(round(loglikelihood, 2));		
				csvString += valueToString + ",";
				}
				else{
					double value=0;
					String valueToString=String.valueOf(round(value, 2));
					csvString += valueToString + ",";
				}
			}
	
			
			
		//	System.out.println(csvString+" csvStringggg");
			csvString = csvString.substring(0, csvString.length() - 1);
			csv.writeBytes(csvString + "\n");
		}
		
		csv.close();
		
	} Feb 20*/
	public static void PriorIDF(ResultSet rs, ResultSet rsCP,RandomAccessFile csv, ArrayList<String> columnsCP, ArrayList<String> columns, String sumPrior) throws IOException, NumberFormatException, SQLException{
		while(rs.next())
		{
			System.out.println("hello");
			String csvString = "";
			for(String col : columnsCP)
			{
				//csvString += rs5.getString(col) + ",";
				if(rs.getString(col)!=null){
				double CP=Double.parseDouble(rs.getString(col) );
				double CPG=Double.parseDouble(rsCP.getString(col));
				double suPrior=Double.parseDouble(sumPrior);
				double logValue=0;
				double cpPrior=CPG/suPrior;
				if(cpPrior!=0){
					logValue=log2(cpPrior);
				}
				else{
					logValue=0;
				}		
				double loglikelihood=CP*logValue;
				String valueToString=String.valueOf(round(loglikelihood, 2));		
				csvString += valueToString + ",";
				}
				else{
					double value=0;
					String valueToString=String.valueOf(round(value, 2));
					csvString += valueToString + ",";
				}
			}
	
			
			
		//	System.out.println(csvString+" csvStringggg");
			csvString = csvString.substring(0, csvString.length() - 1);
			csv.writeBytes(csvString + "\n");
		}
		
		csv.close();
		
	}
/*	public static void LogLikelihoodFormat(ResultSet rs,RandomAccessFile csv,ArrayList<String> columns) throws IOException, NumberFormatException, SQLException{
		while(rs.next())
		{
			String csvString = "";
			for(String col : columns)
			{
				//csvString += rs5.getString(col) + ",";
				double value=Double.parseDouble(rs.getString(col) );
				double logValue=0;
				if(value!=0){
					logValue=log2(value);
				}
				else{
					logValue=0;
				}		
				String valueToString=String.valueOf(round(logValue, 2));		
				csvString += valueToString + ",";
			
				
			}
			
			csvString = csvString.substring(0, csvString.length() - 1);
			csv.writeBytes(csvString + "\n");
		}
		
		csv.close();
		
	}*/
	
	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    long factor = (long) Math.pow(10, places);
	    value = value * factor;
	    long tmp = Math.round(value);
	    return (double) tmp / factor;
	}

public static ArrayList<String> getColumns(ResultSet rs) throws SQLException {
	ArrayList<String> cols = new ArrayList<String>();
	ResultSetMetaData metaData = rs.getMetaData();
	rs.next();

	int columnCount = metaData.getColumnCount();
	for (int i = 1; i <= columnCount; i++) {
		cols.add(metaData.getColumnLabel(i));
	}
	return cols;
}
	/*create view Players_Nodes as select * from FNodes_pvars where pvid = 'movies0';
select child as node from Final_Path_BayesNets where child in (select FID from FNodes_pvars where pvid = 'movies0')
union 
select parent as node from Final_Path_BayesNets where parent in (select FID from FNodes_pvars where pvid = 'movies0');*/
	public static void createSingleFamily(String ctTableName, String nodeName) throws SQLException{
		ConnectDBResult();  
		Statement st = con1.createStatement();
		
		String freqTable = ctTableName + "_f";
		st.execute("drop table if exists `" + freqTable + "`;" );
		st.execute("create table `" + freqTable + "` as select * from `"+ ctTableName + "`;");
		st.execute("alter table `" + freqTable + "` add freq double;");
		
		ResultSet sumM = st.executeQuery("select sum(MULT) from `" + ctTableName + "`");
		
		sumM.next();
		String sumMult = sumM.getString(1);
		
		st.execute("SET SQL_SAFE_UPDATES = 0;");
		
		String sql = "update `" + freqTable + "` set freq = MULT/" + sumMult +";";
		System.out.println(sql);		
		st.execute(sql);		
		
		ArrayList<String> cols = new ArrayList<String>();
		
		ResultSet columnSet = st.executeQuery("show columns from `" + ctTableName +"`;");
		
		while(columnSet.next()){
			String col = columnSet.getString(1);
			cols.add(col);	
			//System.out.print(col);
		}
		cols.remove("MULT");
		cols.remove("freq");
		System.out.print(cols);
		
		Statement st1 = con1.createStatement();
		for(int i=0; i<cols.size();i++){
			System.out.println("create table if not exists `" + cols.get(i) +"_v`  as select distinct `" + cols.get(i) + "` from `" + ctTableName +"`;");
			st1.execute("create table if not exists `" + cols.get(i) +"_v`  as select distinct `" + cols.get(i) + "` from `" + ctTableName +"`;");
		}
		String tableName = ctTableName.substring(0,ctTableName.length()-9);
		String newTableName = tableName + "_values";
		st1.execute("drop table if exists `" + newTableName + "` ;");
		String sql1 = "create table `" + newTableName + "` as select * from ";		
		for(int j=0; j<cols.size()-1;j++) {
			sql1 += " `" + cols.get(j) + "_v` cross join ";
		}
		sql1 += "`" + cols.get(cols.size()-1) +"_v`;";
		System.out.println(sql1);
		st1.execute(sql1);
		
		st1.execute("alter table `" + newTableName + "` add id INT AUTO_INCREMENT primary key;");
		
		// add feature name
		String featureName=nodeName;
		st1.execute("alter table `" + newTableName + "` add features varchar(60);");
		String sql2 = "update `" + newTableName + "` set features = concat(\"" + featureName + "\",id );";
		System.out.println(sql2);
		st1.execute(sql2);
		
		String flatTableName = tableName + "_flat";
		st1.execute("drop table if exists `" + flatTableName + "`;");
		
		ResultSet featureNames = st1.executeQuery("select features from `" + newTableName + "`;" );
		ArrayList<String> featureList = new ArrayList<String>();
		while(featureNames.next()) {
			String oneFeature = featureNames.getString(1);
			featureList.add(oneFeature);
		}
		System.out.println(featureList);
		for(int i=0; i<featureList.size(); i++) {
			String sql4 = "alter table `" + newTableName +"` add column `" + featureList.get(i) + "` double;";
			st1.execute(sql4);
			String sql5 = "update `" + newTableName +"` set `" + featureList.get(i) + "` = if( features = \"" + featureList.get(i) + "\", 1, 0);";
			st1.execute(sql5);
			System.out.println("*********" + sql5);
		}
		
		String sql6 = "create table `" + flatTableName + "` as select ";
		for(int j=0; j<featureList.size()-1; j++) {
			sql6 += " sum(t2.`" + featureList.get(j) +"` * freq) as `" + featureList.get(j) +  "`, ";
		}
		sql6 += " sum(t2.`" + featureList.get(featureList.size()-1) + "` * freq) as `" + featureList.get(featureList.size()-1) +  "` from  `" + freqTable +"` as t1 join `" + newTableName +"` as t2 on ";
		for(int i=0; i<cols.size()-1; i++) {
			sql6 += " t1.`" + cols.get(i) + "` = t2.`" +  cols.get(i) + "` and ";
		}
		sql6 += " t1.`" + cols.get(cols.size()-1) + "` = t2.`" +  cols.get(cols.size()-1) + "`;";
		
		System.out.println("*********" + sql6);
		st1.execute(sql6);
		
		String sql7 = "update `" + flatTableName + "` as t1, `" + freqTable +"` as t2 set t1.`" + nodeName + "` = t2.`" + nodeName + "` where ";
		sql7 += " t2.`" + nodeName + "` != 'N/A' ;";
		System.out.println(sql7);
		st1.equals(sql7);	
		disconnectDB1();
			
		
	}
	public static void createSingleFamilyCount(String ctTableName, String nodeName) throws SQLException{
		ConnectDBResult();  
		Statement st = con1.createStatement();
		
		String freqTable = ctTableName + "_f";
		st.execute("drop table if exists `" + freqTable + "`;" );
		st.execute("create table `" + freqTable + "` as select * from `"+ ctTableName + "`;");
		st.execute("alter table `" + freqTable + "` add freq double;");
		
		ResultSet sumM = st.executeQuery("select sum(MULT) from `" + ctTableName + "`");
		
		sumM.next();
		String sumMult = sumM.getString(1);
		
		st.execute("SET SQL_SAFE_UPDATES = 0;");
		
		String sql = "update `" + freqTable + "` set freq = MULT";
		System.out.println(sql);		
		st.execute(sql);		
		
		ArrayList<String> cols = new ArrayList<String>();
		
		ResultSet columnSet = st.executeQuery("show columns from `" + ctTableName +"`;");
		
		while(columnSet.next()){
			String col = columnSet.getString(1);
			cols.add(col);	
			//System.out.print(col);
		}
		cols.remove("MULT");
		cols.remove("freq");
		System.out.print(cols);
		
		Statement st1 = con1.createStatement();
		for(int i=0; i<cols.size();i++){
			System.out.println("create table if not exists `" + cols.get(i) +"_v`  as select distinct `" + cols.get(i) + "` from `" + ctTableName +"`;");
			st1.execute("create table if not exists `" + cols.get(i) +"_v`  as select distinct `" + cols.get(i) + "` from `" + ctTableName +"`;");
		}
		String tableName = ctTableName.substring(0,ctTableName.length()-9);
		String newTableName = tableName + "_values";
		st1.execute("drop table if exists `" + newTableName + "` ;");
		String sql1 = "create table `" + newTableName + "` as select * from ";		
		for(int j=0; j<cols.size()-1;j++) {
			sql1 += " `" + cols.get(j) + "_v` cross join ";
		}
		sql1 += "`" + cols.get(cols.size()-1) +"_v`;";
		System.out.println(sql1);
		st1.execute(sql1);
		
		st1.execute("alter table `" + newTableName + "` add id INT AUTO_INCREMENT primary key;");
		
		// add feature name
		String featureName=nodeName;
		st1.execute("alter table `" + newTableName + "` add features varchar(60);");
		String sql2 = "update `" + newTableName + "` set features = concat(\"" + featureName + "\",id );";
		System.out.println(sql2);
		st1.execute(sql2);
		
		String flatTableName = tableName + "_flat";
		st1.execute("drop table if exists `" + flatTableName + "`;");
		
		ResultSet featureNames = st1.executeQuery("select features from `" + newTableName + "`;" );
		ArrayList<String> featureList = new ArrayList<String>();
		while(featureNames.next()) {
			String oneFeature = featureNames.getString(1);
			featureList.add(oneFeature);
		}
		System.out.println(featureList);
		for(int i=0; i<featureList.size(); i++) {
			String sql4 = "alter table `" + newTableName +"` add column `" + featureList.get(i) + "` double;";
			st1.execute(sql4);
			String sql5 = "update `" + newTableName +"` set `" + featureList.get(i) + "` = if( features = \"" + featureList.get(i) + "\", 1, 0);";
			st1.execute(sql5);
			System.out.println("*********" + sql5);
		}
		
		String sql6 = "create table `" + flatTableName + "` as select ";
		for(int j=0; j<featureList.size()-1; j++) {
			sql6 += " sum(t2.`" + featureList.get(j) +"` * freq) as `" + featureList.get(j) +  "`, ";
		}
		sql6 += " sum(t2.`" + featureList.get(featureList.size()-1) + "` * freq) as `" + featureList.get(featureList.size()-1) +  "` from  `" + freqTable +"` as t1 join `" + newTableName +"` as t2 on ";
		for(int i=0; i<cols.size()-1; i++) {
			sql6 += " t1.`" + cols.get(i) + "` = t2.`" +  cols.get(i) + "` and ";
		}
		sql6 += " t1.`" + cols.get(cols.size()-1) + "` = t2.`" +  cols.get(cols.size()-1) + "`;";
		
		System.out.println("*********" + sql6);
		st1.execute(sql6);
		
		String sql7 = "update `" + flatTableName + "` as t1, `" + freqTable +"` as t2 set t1.`" + nodeName + "` = t2.`" + nodeName + "` where ";
		sql7 += " t2.`" + nodeName + "` != 'N/A' ;";
		System.out.println(sql7);
		st1.equals(sql7);	
		disconnectDB1();
			
		
	}
	public static void createSingleFamilyCP(String cpTableName, String nodeName) throws SQLException{
		ConnectDBResult();  
		Statement st = con1.createStatement();
		
		String freqTable = cpTableName + "_f";
		st.execute("drop table if exists `" + freqTable + "`;" );
		st.execute("create table `" + freqTable + "` as select * from `"+ cpTableName + "`;");

		
		st.execute("SET SQL_SAFE_UPDATES = 0;");
		
		ArrayList<String> cols = new ArrayList<String>();
		
		ResultSet columnSet = st.executeQuery("show columns from `" + cpTableName +"`;");
		
		while(columnSet.next()){
			String col = columnSet.getString(1);
			cols.add(col);	
			//System.out.print(col);
		}
		cols.remove("CP");
		cols.remove("prior");
		System.out.print(cols);
		
		Statement st1 = con1.createStatement();
		for(int i=0; i<cols.size();i++){
			System.out.println("create table if not exists `" + cols.get(i) +"_CP_v`  as select distinct `" + cols.get(i) + "` from `" + cpTableName +"`;");
			st1.execute("create table if not exists `" + cols.get(i) +"_CP_v`  as select distinct `" + cols.get(i) + "` from `" + cpTableName +"`;");
		}
		String tableName = cpTableName.substring(0,cpTableName.length()-9);
		String newTableName = tableName + "_CP_values";
		st1.execute("drop table if exists `" + newTableName + "` ;");
		String sql1 = "create table `" + newTableName + "` as select * from ";		
		for(int j=0; j<cols.size()-1;j++) {
			sql1 += " `" + cols.get(j) + "_CP_v` cross join ";
		}
		sql1 += "`" + cols.get(cols.size()-1) +"_CP_v`;";
		System.out.println(sql1);
		st1.execute(sql1);
		
		st1.execute("alter table `" + newTableName + "` add id INT AUTO_INCREMENT primary key;");
		
		// add feature name
		String featureName=nodeName;
		st1.execute("alter table `" + newTableName + "` add features varchar(60);");
		String sql2 = "update `" + newTableName + "` set features = concat(\"" + featureName + "\",id );";
		System.out.println(sql2);
		st1.execute(sql2);
		
		String flatTableName = tableName + "_CP_flat";
		String PriorflatTableName = tableName + "_Prior_flat";
		st1.execute("drop table if exists `" + flatTableName + "`;");
		
		ResultSet featureNames = st1.executeQuery("select features from `" + newTableName + "`;" );
		ArrayList<String> featureList = new ArrayList<String>();
		while(featureNames.next()) {
			String oneFeature = featureNames.getString(1);
			featureList.add(oneFeature);
		}
		System.out.println(featureList);
		for(int i=0; i<featureList.size(); i++) {
			String sql4 = "alter table `" + newTableName +"` add column `" + featureList.get(i) + "` double;";
			st1.execute(sql4);
			String sql5 = "update `" + newTableName +"` set `" + featureList.get(i) + "` = if( features = \"" + featureList.get(i) + "\", 1, 0);";
			st1.execute(sql5);
			System.out.println("*********" + sql5);
		}
		
		String sql6 = "create table `" + flatTableName + "` as select ";
		for(int j=0; j<featureList.size()-1; j++) {
			sql6 += " sum(t2.`" + featureList.get(j) +"` * CP) as `" + featureList.get(j) +  "`, ";
		}
		sql6 += " sum(t2.`" + featureList.get(featureList.size()-1) + "` * CP) as `" + featureList.get(featureList.size()-1) +  "` from  `" + freqTable +"` as t1 join `" + newTableName +"` as t2 on ";
		for(int i=0; i<cols.size()-1; i++) {
			sql6 += " t1.`" + cols.get(i) + "` = t2.`" +  cols.get(i) + "` and ";
		}
		sql6 += " t1.`" + cols.get(cols.size()-1) + "` = t2.`" +  cols.get(cols.size()-1) + "`;";
		
		System.out.println("*********" + sql6);
		st1.execute(sql6);
		
		String sql7 = "update `" + flatTableName + "` as t1, `" + freqTable +"` as t2 set t1.`" + nodeName + "` = t2.`" + nodeName + "` where ";
		sql7 += " t2.`" + nodeName + "` != 'N/A' ;";
		System.out.println(sql7);
		st1.equals(sql7);
		String freqtableNAmePrior=freqTable+"_Prior";
		st.execute("drop table if exists `" + freqtableNAmePrior + "`;" );
		System.out.println("create table `"+freqtableNAmePrior+"` as select distinct `"+tableName+"`, prior from `"+freqTable+"` where prior<> 0");
		st.execute("create table `"+freqtableNAmePrior+"` as select distinct `"+tableName+"`, prior from `"+freqTable+"` where prior<> 0");
		System.out.println("this select "+"Select distinct `"+tableName+"` from  "+GenericDB+"_flat.`"+freqtableNAmePrior+"` where `"+
				tableName+"` not in (select `"+tableName+"` from `"+freqtableNAmePrior+"`)");
		ResultSet nonMember=st.executeQuery("Select distinct `"+tableName+"` from  "+GenericDB+"_flat.`"+freqtableNAmePrior+"` where `"+
				tableName+"` not in (select `"+tableName+"` from `"+freqtableNAmePrior+"`)");
		ArrayList<String> nom= new ArrayList<String>();
		while(nonMember.next()){
			
			String n=nonMember.getString(1);
			nom.add(n);
		}
		
		for(int i=0; i<nom.size();i++){
			System.out.println("This insert : "+"insert into `"+freqtableNAmePrior+"` values ("+nom.get(i)+", 0);");
		st.execute("insert into `"+freqtableNAmePrior+"` values ('"+nom.get(i)+"', 0);");	
		}
		
		ResultSet rs=st.executeQuery("select distinct `"+tableName+"` from `"+freqtableNAmePrior+"`");
		ArrayList<String> nodes= new ArrayList<String>();
		while(rs.next()){
			String n = rs.getString(1);
			nodes.add(n);
			
		}
		String Query="Create table  `"+ PriorflatTableName+"` (";
		for(int i=0; i<nodes.size(); i++){
			Query+="`"+tableName+nodes.get(i)+"`"+" Varchar(20),";
			
		}
		Query=Query.substring(0,Query.length()-1);
		Query=Query+")";
		
		/*insert into Premier_League_MidStr_12813_flat.`FirstGoal(movies0,MatchComp0)_Prior_flat`
		 (`FirstGoal(movies0,MatchComp0)1`,`FirstGoal(movies0,MatchComp0)0`)
		 select f1.prior,f2.prior 
		 from (select prior from`FirstGoal(movies0,MatchComp0)_local_CP_f_Prior` where `FirstGoal(movies0,MatchComp0)`=1) as f1

		 cross join (select prior from`FirstGoal(movies0,MatchComp0)_local_CP_f_Prior` where `FirstGoal(movies0,MatchComp0)`=0) as f2;*/
		String InsertQuery="Insert into `"+PriorflatTableName+"` (";
		for(int i=0; i<nodes.size(); i++){
			if(i!=nodes.size()-1){
				InsertQuery +="`"+tableName+nodes.get(i)+"`,";
			}
			else{
				InsertQuery +="`"+tableName+nodes.get(i)+"`) select ";
			}
			
		}
		
	
		for(int i=0; i<nodes.size(); i++){
			if(i!=nodes.size()-1){
				InsertQuery +="f"+i+".prior,";
			}
			else{
				InsertQuery +="f"+i+".prior from ";
			}
			
		}
		
		for(int i=0; i<nodes.size(); i++){
			if(i!=nodes.size()-1){
				InsertQuery +="( select prior from  `"+freqtableNAmePrior+"` where `"+tableName+"`='"+nodes.get(i)+"') as "+" f"+i+ " cross join" ;
			}
			else{
				InsertQuery +="( select prior from  `"+freqtableNAmePrior+"` where `"+tableName+"`='"+nodes.get(i)+"') as "+" f"+i+ ";" ;
			}
			
		}
		System.out.println("Insert Query"+InsertQuery);
		System.out.println(Query);
		st.execute(Query);
		st.execute(InsertQuery);
		/*String sql8 = "create table `" + PriorflatTableName + "` as select ";
		for(int j=0; j<featureList.size()-1; j++) {
			sql8 += " sum(t2.`" + featureList.get(j) +"` * prior) as `" + featureList.get(j) +  "`, ";
		}
		sql8 += " sum(t2.`" + featureList.get(featureList.size()-1) + "` * prior) as `" + featureList.get(featureList.size()-1) +  "` from  `" + freqtableNAmePrior +"` as t1 join `" + newTableName +"` as t2 on ";
		for(int i=0; i<cols.size()-1; i++) {
			sql8 += " t1.`" + cols.get(i) + "` = t2.`" +  cols.get(i) + "` and ";
		}
		sql8 += " t1.`" + cols.get(cols.size()-1) + "` = t2.`" +  cols.get(cols.size()-1) + "`;";
		
		System.out.println("*********" + sql8);
		st1.execute(sql8);
		
		String sql9 = "update `" + PriorflatTableName + "` as t1, `" + freqtableNAmePrior +"` as t2 set t1.`" + nodeName + "` = t2.`" + nodeName + "` where ";
		sql9 += " t2.`" + nodeName + "` != 'N/A' ;";
		System.out.println(sql9);
		st1.equals(sql9);*/
		disconnectDB1();
			
		
	}
	public static void setVarsFromConfig(){
		Config2 conf = new Config2();
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
		
	}
	
	public static void connectDB() throws SQLException  {

	
	
	String CONN_STR_DB = "jdbc:" + dbaddress + "/" + dbBN;
	try {
		java.lang.Class.forName("com.mysql.jdbc.Driver");
	} catch (Exception ex) {
		System.err.println("Unable to load MySQL JDBC driver");
	}
	System.out.println(dbUsername+"UserName");
	con2 = (Connection) DriverManager.getConnection(CONN_STR_DB, dbUsername, dbPassword);
}
	
	public static void ConnectDBResult() throws SQLException  {
		String CONN_STR = "jdbc:" + dbaddress + "/" + ResultDB;
		try {
			java.lang.Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception ex) {
			System.err.println("Unable to load MySQL JDBC driver");
		}
		con1 = (Connection) DriverManager.getConnection(CONN_STR, dbUsername, dbPassword);
	
		
	}
	
	private static int disconnectDB2()
	{
		try
		{
			con2.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to close database connection." );
			e.printStackTrace();
			return -1;
		}
		
		return 0;
	}

	private static int disconnectDB1()
	{
		try
		{
			con1.close();
		}
		catch ( SQLException e )
		{
			System.out.println( "Failed to close database connection." );
			e.printStackTrace();
			return -1;
		}
		
		return 0;
	}


}
/*		ConnectDBResult();
Statement st = con1.createStatement();

String freqTable = ctTableName + "_f";
st.execute("drop table if exists `" + freqTable + "`;" );
st.execute("create table `" + freqTable + "` as select * from `"+ ctTableName + "`;");
st.execute("alter table `" + freqTable + "` add freq double;");

ResultSet sumM = st.executeQuery("select sum(MULT) from `" + ctTableName + "`");

sumM.next();
String sumMult = sumM.getString(1);

st.execute("SET SQL_SAFE_UPDATES = 0;");

String sql = "update `" + freqTable + "` set freq = MULT/" + sumMult +";";
System.out.println(sql);		
st.execute(sql);

ArrayList<String> cols = new ArrayList<String>();
ArrayList<String> colsWithoutNode = new ArrayList<String>();

ResultSet columnSet = st.executeQuery("show columns from `" + ctTableName +"`;");

while(columnSet.next()){
	String col = columnSet.getString(1);
	cols.add(col);	
	//System.out.print(col);
}
cols.remove("MULT");
cols.remove("freq");
System.out.print(cols);
colsWithoutNode = cols;
colsWithoutNode.remove(nodeName);
System.out.println(colsWithoutNode);

Statement st1 = con1.createStatement();
for(int i=0; i<cols.size();i++){
st1.execute("drop table if exists  `" + cols.get(i) +"_v`");
	System.out.println("create table if not exists `" + cols.get(i) +"_v`  as select distinct `" + cols.get(i) + "` from `" + ctTableName +"`;");
	st1.execute("create table if not exists `" + cols.get(i) +"_v`  as select distinct `" + cols.get(i) + "` from `" + ctTableName +"`;");
}
String tableName = ctTableName.substring(0,ctTableName.length()-9);
String newTableName = tableName + "_values";
st1.execute("drop table if exists `" + newTableName + "` ;");
String sql1 = "create table `" + newTableName + "` as select * from ";
for(int j=0; j<colsWithoutNode.size()-1;j++) {
	sql1 += " `" + colsWithoutNode.get(j) + "_v` cross join ";
}
//System.out.println("!!!!!!!!!!!!"+colsWithoutNode.size());
sql1 += "`" + colsWithoutNode.get(colsWithoutNode.size()-1) +"_v`;";
System.out.println(sql1);
st1.execute(sql1);

st1.execute("alter table `" + newTableName + "` add id INT AUTO_INCREMENT primary key;");

// add feature name
String featureName=nodeName;
st1.execute("alter table `" + newTableName + "` add features varchar(60);");
String sql2 = "update `" + newTableName + "` set features = concat(\"" + featureName + "\",id );";
System.out.println(sql2);
st1.execute(sql2);

String flatTableName = tableName + "_flat";
st1.execute("drop table if exists `" + flatTableName + "`;");

ResultSet featureNames = st1.executeQuery("select features from `" + newTableName + "`;" );
ArrayList<String> featureList = new ArrayList<String>();
while(featureNames.next()) {
	String oneFeature = featureNames.getString(1);
	featureList.add(oneFeature);
}
System.out.println(featureList);
for(int i=0; i<featureList.size(); i++) {
	String sql4 = "alter table `" + newTableName +"` add column `" + featureList.get(i) + "` double;";
	st1.execute(sql4);
	String sql5 = "update `" + newTableName +"` set `" + featureList.get(i) + "` = if( features = \"" + featureList.get(i) + "\", 1, 0);";
	st1.execute(sql5);
	System.out.println("*********" + sql5);
}

String sql6 = "create table `" + flatTableName + "` as select t1.`" + nodeName + "`, ";
for(int j=0; j<featureList.size()-1; j++) {
	sql6 += " (t2.`" + featureList.get(j) +"` * freq) as `" + featureList.get(j) +  "`, ";
}
sql6 += " (t2.`" + featureList.get(featureList.size()-1) + "` * freq) as `" + featureList.get(featureList.size()-1) +  "` from  `" + freqTable +"` as t1 join `" + newTableName +"` as t2 on ";
for(int i=0; i<colsWithoutNode.size()-1; i++) {
	sql6 += " t1.`" + colsWithoutNode.get(i) + "` = t2.`" +  colsWithoutNode.get(i) + "` and ";
}
sql6 += " t1.`" + colsWithoutNode.get(colsWithoutNode.size()-1) + "` = t2.`" +  colsWithoutNode.get(colsWithoutNode.size()-1) + "`;";

System.out.println("*********" + sql6);
st1.execute(sql6);

String sql7 = "update `" + flatTableName + "` as t1, `" + freqTable +"` as t2 set t1.`" + nodeName + "` = t2.`" + nodeName + "` where ";
sql7 += " t2.`" + nodeName + "` != 'N/A' ;";
System.out.println(sql7);
st1.equals(sql7);*/