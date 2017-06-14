/* Author: Nicole,Li
 * @Nov 5, 2014
 */
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class PlayerNoClassLabel {
	static Connection con1;
	static String databaseName;
	static String dbUsername;
	static String dbPassword;
	static String dbaddress;


	public static void main(String[] args) throws Exception {
		//read config file
		setVarsFromConfig();
		connectDB();
		createSingleFamily("dribble_eff(Players0,MatchComp0)_a,b,c_local_CT","dribble_eff(Players0,MatchComp0)");
		
	}
	
	public static void createSingleFamily(String ctTableName, String nodeName) throws SQLException{
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
		
	}
	
	public static void setVarsFromConfig(){
		Config conf = new Config();
		databaseName = conf.getProperty("dbname");
		dbUsername = conf.getProperty("dbusername");
		dbPassword = conf.getProperty("dbpassword");
		dbaddress = conf.getProperty("dbaddress");
		System.out.println("haha");
		
	}
	
	public static void connectDB() throws SQLException  {

		String CONN_STR = "jdbc:" + dbaddress + "/" + databaseName;
		try {
			java.lang.Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception ex) {
			System.err.println("Unable to load MySQL JDBC driver");
		}
		con1 = (Connection) DriverManager.getConnection(CONN_STR, dbUsername, dbPassword);
	}
}
