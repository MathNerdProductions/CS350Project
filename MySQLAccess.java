/*
 * MySQLAccess.java
 * CS350 Project
 * @author David Worth
 * @author Joshua Sachs
 * @date 09/2014
 * @modified 10/25/2014
 * @description Connects to and manages the database for the project.
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import java.sql.ResultSetMetaData;

public class MySQLAccess {
    private Connection connect = null;
    private Statement statement = null;
    private PreparedStatement prepared = null;
    private ResultSet result = null;
    
    public void readDatabase() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        
        connect = DriverManager.getConnection("jdbc:mysql://138.86.122.233/STACDB" , "se2014", "se2014");
        statement = connect.createStatement();
        
        
    }
    
    /*
     * ResultSet runQuery
     * @author David Worth
     * @param String cmd
     * 		The SQL command
     * 		@value INSERT
     * 		@value SELECT
     * 		@value UPDATE
     * 		@value DELETE
     * 
     * @param String table
     * 		The table in the database to run the command on
     * 
     * @param String[] keys
     * 		An array of the table columns to be included in the statement.
     * 
     * @param String[] values
     * 		An array of the values corresponding to the table columns supplied in @keys
     * 
     * @param String[] whereKeys
     * 		An array of the table columns to be included in the WHERE clause.
     * 
     * @param String[] whereVals
     * 		An array of the values corresponding to the table colums supplied in @whereKeys
     * 
     * @param int limitStart
     *      First row index to include
     * 
     * @param int limitCount
     *      Number of rows to include
     *      Set to 0 to include all rows.
     * 
     * @return ResultSet
     * 		The result set returned by the database for the query.
     * 		
     * Example queries:
     * INSERT
     *     runQuery("INSERT", "table", ["id", "fname", "lname"], [null, "Bob", "Smith"], null, null, 0, 30);
     * Unbiased SELECT
     *     runQuery("SELECT", "table", ["id", "classID"], null, null, null, 0, 30);
     * Biased SELECT
     *     runQuery("SELECT", "table", ["id", "classID"], null, ["fname", "lname"], ["Bob", "Smith"], 0, 30);
     * Unbiased UPDATE
     *     runQuery("UPDATE", "table", ["device"], [null], null, null, 0, 30);
     * Biased UPDATE
     *     runQuery("UPDATE", "table", ["device"], ["FF:FF:FF:FF:FF:FF"], ["fname", "lname"], ["Bob", "Smith"], 0, 30);
     * DELETE
     *     runQuery("DELETE", "table", null, null, ["fname", "lname"], ["Bob", "Smith"], 0, 30);
     *     
     * Notes:
     * - Please note that this method uses prepared statements to prevent SQL injection, which means that you must enumerate each column you wish to interact with. This means no SELECT * statments.
     * - Please also note that certain parameters have no use for certain CMDs.
     *    - @whereKeys, @whereValues, @limitStart, @limitCount are useless for INSERT
     *    - @keys and @values are useless are DELETE
     *    - @values is useless for SELECT
     * - Note that WHERE clauses only support AND cases right now.
     * 
     * @exception Exception
     * 		Will contain a detailed explanation of the error.
     */
    public ResultSet runQuery(String cmd, String table, String[] keys, String[] values, String[] whereKeys, String[] whereValues, int limitStart, int limitCount) throws Exception
    {	
    	PreparedStatement stmt;
    	
    	if(!(cmd.equals("INSERT") || cmd.equals("SELECT") || cmd.equals("UPDATE") || cmd.equals("DELETE")))
    	{
    		throw new Exception("CMD UNSUPPORTED! CMD must be INSERT, SELECT, UPDATE, or DELETE.");
    	}
    	
    	String sql = "";
    	if(cmd.equals("INSERT"))
    	{
    		//Build Insert statement
    		sql += "INSERT INTO `"+table+"` (";
    		
    		for(int i = 0; i < keys.length; i++)
    		{
    			sql += "`"+keys[i]+"`";
    			if(i!=keys.length-1)
    				sql+=",";
    		}
    		
    		sql += ") VALUES(";
    		
    		for(int i = 0; i < values.length; i++)
    		{
    			sql += "?";
    			if(i!=values.length-1)
    				sql+=", ";
    		}
    		
    		sql += ");";
    	}
    	else if(cmd.equals("SELECT"))
    	{
    		sql += "SELECT ";
    		
    		if((whereKeys != null && whereValues != null) && whereKeys.length != whereValues.length)
    			throw new Exception("INVALID WHERE CLAUSE: Key/value pairs invalid. Make sure that you have the same number of keys as values.");
    		
    		for(int i = 0; i < keys.length; i++)
    		{
    			sql += "`"+keys[i]+"`";
    			if(i!=keys.length-1)
    				sql+=",";
    		}
    		
    		sql += "FROM `" + table + "`";
    		
    		if(whereKeys != null && whereKeys.length != 0)
    		{
    			sql += " WHERE ";
    			
    			for(int i = 0; i < whereKeys.length; i++)
    			{
    				sql += "`" + whereKeys[i] + "`=? ";
    				
    				if(i != whereKeys.length-1)
    				{
    					sql += "AND ";
    				}
    			}
    		}
    		
    		//Test limit
    		if(limitCount != 0)
    		{
    			sql += "LIMIT "+limitStart+","+limitCount;
    		}
    		
    		sql += ";";
    	}
    	else if(cmd.equals("UPDATE"))
    	{
    		sql += "UPDATE `"+table+"` SET ";
    		
    		for(int i = 0; i < keys.length; i++)
    		{
    			sql += "`"+keys[i]+"`=?";
    			if(i!=keys.length-1)
    				sql+=",";
    		}
    		
    		if(whereKeys.length != 0)
    		{
    			sql += " WHERE ";
    			
    			for(int i = 0; i < whereKeys.length; i++)
    			{
    				sql += "`" + whereKeys[i] + "`=? ";
    				
    				if(i != whereKeys.length-1)
    				{
    					sql += "AND ";
    				}
    			}
    		}
    		
    		//Test limit
    		if(limitCount != 0)
    		{
    			sql += "LIMIT "+limitCount;
    		}
    		
    		sql += ";";
    	}
    	else if(cmd.equals("DELETE"))
    	{
    		sql += "DELETE FROM `"+table+"`";
    		
    		if(whereKeys.length != 0)
    		{
    			sql += " WHERE ";
    			
    			for(int i = 0; i < whereKeys.length; i++)
    			{
    				sql += "`" + whereKeys[i] + "`=? ";
    				
    				if(i != whereKeys.length-1)
    				{
    					sql += "AND ";
    				}
    			}
    		}
    		
    		//Test limit
    		if(limitCount != 0)
    		{
    			sql += "LIMIT "+limitCount;
    		}
    		
    		sql += ";";
    	}
    	
    	try
    	{
    		stmt = connect.prepareStatement(sql);
    	}
    	catch(SQLException e)
    	{
    		e.printStackTrace();
    		throw new Exception("SQL Error!");
    	}
    	
    	if(stmt == null)
    		throw new Exception("SQL Error!");
    	
    	System.out.println(sql);
    	System.out.println(stmt.getParameterMetaData().getParameterCount());
    	
    	if(cmd.equals("INSERT"))
    	{
    		//Apply @values
    		for(int i = 0; i < values.length; i++)
    		{
    			stmt.setString(i+1, values[i]);
    		}
    		
    		stmt.executeUpdate();
    		return null;
    	}
    	else if(cmd.equals("SELECT"))
    	{
    		//Apply @whereValues
    		if(whereValues != null)
    		{
    			for(int i = 0; i < whereValues.length; i++)
    			{
    				stmt.setString(i+1, whereValues[i]);
    			}
    		}
    	}
    	else if(cmd.equals("UPDATE"))
    	{
    		//Apply @values
    		for(int i = 0; i < values.length; i++)
    		{
    			stmt.setString(i+1, values[i]);
    		}
    		
    		//Apply @whereValues
    		if(whereValues != null)
    		{
    			for(int i = 0; i < whereValues.length; i++)
    			{
    				stmt.setString(i+values.length+1, whereValues[i]);
    			}
    		}
    		
    		stmt.executeUpdate();
    		return null;
    	}
    	else if(cmd.equals("DELETE"))
    	{
    		//Apply @whereValues
    		if(whereValues != null)
    		{
    			for(int i = 0; i < whereValues.length; i++)
    			{
    				stmt.setString(i+1, whereValues[i]);
    			}
    		}
    		
    		stmt.executeUpdate();
    		return null;
    	}
    	
    	return stmt.executeQuery();
    }
    
    public void runQuery2() throws Exception {
        statement = connect.createStatement();
       String sql = "select * from users";
       ResultSet result = statement.executeQuery(sql);
       
       while (result.next()){
           String firstName = result.getString("FName");
           String lastName = result.getString("LName");
           
           String userName = result.getString("UName");
           String password = result.getString("Password");
           
       }
       result.close();
       statement.close();
       
       System.out.println("firstName + lastName + userName + password");
       
       
       
    }
    
    /*
     * ResultSet checkUser
     * @author David Worth
     * @param String user
     * 		The username to check
     * 
     * @return ResultSet
     * 		The user information
     * 
     * @exception Exception
     * 		Throws exception if there is an error running the SQL query
     */
    public ResultSet checkUser(String user) throws Exception
    {
    	String[] keys = {"ID", "FName", "LName", "UName", "Password", "RegTime"};
    	String[] whereKeys = {"UName"};
    	String[] whereVals = {user};
    	return runQuery("SELECT", "Users", keys, null, whereKeys, whereVals, 0, 1);
    }
    
    /*
     * ResultSet checkAdmin
     * @author David Worth
     * @param String user
     * 		The username to check
     * 
     * @return ResultSet
     * 		The user information
     * 
     * @exception Exception
     * 		Throws exception if there is an error running the SQL query
     */
    public ResultSet checkAdmin(String user) throws Exception
    {
    	String[] keys = {"ID", "FName", "LName", "UName", "Password", "RegTime"};
    	String[] whereKeys = {"UName"};
    	String[] whereVals = {user};
        return runQuery("SELECT", "Admins", keys, null, whereKeys, whereVals, 0, 1);
    }
    
    /*
     * boolean createUser
     * @author David Worth
     * @param String user
     * 		The username to add
     * 
     * @param String pass
     * 		The password to add
     * 
     * @param String first
     * 		The first name of the user
     * 
     * @param String last
     * 		The last name of the user
     * 
     * @param String email
     * 		UNUSED: there is no column in the database for email
     * 		The user's email address
     * 
     * @return boolean
     * 		Returns true if the user is created successfully
     */
    public boolean createUser(String user, String pass, String first, String last, String email)
    {
    	String[] keys = {"FName", "LName", "UName", "Password"};
    	String[] vals = {first, last, user, pass};
    	
    	try
    	{
    		runQuery("INSERT", "Users", keys, vals, null, null, 0, 30);
    	}catch(Exception e)
    	{
    		e.printStackTrace();
    		System.out.println(e);
    		return false;
    	}
    	
    	return true;
    }
    
    /*
     * boolean createAdmin
     * @author David Worth
     * @param String user
     * 		The username to add
     * 
     * @param String pass
     * 		The password to add
     * 
     * @param String first
     * 		The first name of the user
     * 
     * @param String last
     * 		The last name of the user
     * 
     * @param String email
     * 		UNUSED: there is no column in the database for email
     * 		The user's email address
     * 
     * @return boolean
     * 		Returns true if the user is created successfully
     */
    public boolean createAdmin(String user, String pass, String first, String last, String email)
    {
    	String[] keys = {"FName", "LName", "UName", "Password"};
    	String[] vals = {first, last, user, pass};
    	
    	try
    	{
    		runQuery("INSERT", "Users", keys, vals, null, null, 0, 30);
    	}catch(Exception e)
    	{
    		e.printStackTrace();
    		System.out.println(e);
    		return false;
    	}
    	
    	return true;
    }
    
    /*
     * boolean createClass
     * @author David Worth
     * @param String name
     * 		the name of the class
     * 
     * @param int adminID
     * 		the ID of the administrator of the class
     * 
     * @param String institution
     * 		the string representation of the institution
     * 
     * @param String meetTimes
     * 		the formatted string representation of the meet times
     * 
     * @return boolean
     * 		Returns true if the class is created successfully.
     */
    public boolean createClass(String name, int adminID, String institution, String meetTimes)
    {
    	String[] keys = {"ClassName", "AdminID", "Institution", "MeetTimes"};
    	String[] vals = {name, Integer.toString(adminID), institution, meetTimes};
    	
    	try
    	{
    		runQuery("INSERT", "Classes", keys, vals, null, null, 0, 30);
    	}catch(Exception e)
    	{
    		e.printStackTrace();
    		System.out.println(e);
    		return false;
    	}
    	
    	return true;
    }
    
    /*
     * boolean deleteClass
     * @author David Worth
     * @param int classID
     * 		the ID of the class to be deleted
     * 
     * @return boolean
     * 		Returns true if the class is deleted successfully.
     */
    public boolean deleteClass(int classID)
    {
    	String[] whereKeys = {"ClassID"};
    	String[] whereVals = {Integer.toString(classID)};
    	
    	try
    	{
    		runQuery("DELETE", "Classes", null, null, whereKeys, whereVals, 0, 30);
    	}catch(Exception e)
    	{
    		e.printStackTrace();
    		System.out.println(e);
    		return false;
    	}
    	
    	return true;
    }
    
    /*
     * ResultSet searchClasses
     * @author David Worth
     * 
     * @Type 1:
     * @param int classID
     * 		the ID of the class to search for
     * 
     * @Type 2:
     * @param int classID
     * 		the ID of the class to search for
     * 
     * @param String className
     * 		the string representationn of the class name
     * 
     * @Type 3:
     * @param int classID
     * 		the ID of the class to search for
     * 
     * @param int adminID
     * 		the ID of the administrator
     * 
     * @Type 4:
     * @param int classID
     * 		the ID of the class to search for
     * 
     * @param String className
     * 		the string representation of the class name
     * 
     * @param int adminID
     * 		the ID of the administrator
     * 
     * @Type 5:
     * @param String className
     * 		the string representation of the class name
     * 
     * @Type 6:
     * @param String className
     * 		the string representation of the class name
     * 
     * @param int adminID
     * 		the ID of the administrator
     * 
     * @return ResultSet
     * 		Returns the result of the query, including all matched classes.
     */
    public ResultSet searchClasses(int classID)
    {
    	String[] whereKeys = {"ClassID"};
    	String[] whereVals = {Integer.toString(classID)};
    	
    	try
    	{
    		return runQuery("SELECT", "Classes", null, null, whereKeys, whereVals, 0, 30);
    	}catch(Exception e)
    	{
    		e.printStackTrace();
    		System.out.println(e);
    		return null;
    	}
    }
    
    public ResultSet searchClasses(int classID, String className)
    {
    	String[] whereKeys = {"ClassID", "ClassName"};
    	String[] whereVals = {Integer.toString(classID), className};
    	
    	try
    	{
    		return runQuery("SELECT", "Classes", null, null, whereKeys, whereVals, 0, 30);
    	}catch(Exception e)
    	{
    		e.printStackTrace();
    		System.out.println(e);
    		return null;
    	}
    }
    
    public ResultSet searchClasses(int classID, int adminID)
    {
    	String[] whereKeys = {"ClassID", "AdminID"};
    	String[] whereVals = {Integer.toString(classID), Integer.toString(adminID)};
    	
    	try
    	{
    		return runQuery("SELECT", "Classes", null, null, whereKeys, whereVals, 0, 30);
    	}catch(Exception e)
    	{
    		e.printStackTrace();
    		System.out.println(e);
    		return null;
    	}
    }
    
    public ResultSet searchClasses(int classID, String className, int adminID)
    {
    	String[] whereKeys = {"ClassID", "ClassName", "AdminID"};
    	String[] whereVals = {Integer.toString(classID), className, Integer.toString(adminID)};
    	
    	try
    	{
    		return runQuery("SELECT", "Classes", null, null, whereKeys, whereVals, 0, 30);
    	}catch(Exception e)
    	{
    		e.printStackTrace();
    		System.out.println(e);
    		return null;
    	}
    }
    
    public ResultSet searchClasses(String className)
    {
    	String[] whereKeys = {"ClassName"};
    	String[] whereVals = {className};
    	
    	try
    	{
    		return runQuery("SELECT", "Classes", null, null, whereKeys, whereVals, 0, 30);
    	}catch(Exception e)
    	{
    		e.printStackTrace();
    		System.out.println(e);
    		return null;
    	}
    }
    
    public ResultSet searchClasses(String className, int adminID)
    {
    	String[] whereKeys = {"ClassName", "AdminID"};
    	String[] whereVals = {className, Integer.toString(adminID)};
    	
    	try
    	{
    		return runQuery("SELECT", "Classes", null, null, whereKeys, whereVals, 0, 30);
    	}catch(Exception e)
    	{
    		e.printStackTrace();
    		System.out.println(e);
    		return null;
    	}
    }
    
    /*
     * ResultSet searchClassesWithAdminID
     * @author David Worth
     * @param int adminID
     * 		the ID of the administrator
     * 
     * @return ResultSet
     * 		Returns the result of the query, including all matched classes.
     */
    public ResultSet searchClassesWithAdminID(int adminID)
    {
    	String[] whereKeys = {"AdminID"};
    	String[] whereVals = {Integer.toString(adminID)};
    	
    	try
    	{
    		return runQuery("SELECT", "Classes", null, null, whereKeys, whereVals, 0, 30);
    	}catch(Exception e)
    	{
    		e.printStackTrace();
    		System.out.println(e);
    		return null;
    	}
    }
    
  public static void dump(ResultSet rs) throws SQLException
  {
	  System.out.println("Dumping...");
	  
	  ResultSetMetaData rsmd = rs.getMetaData();
	  int columnsNumber = rsmd.getColumnCount();
	  while(rs.next())
	  {
		  for (int i = 1; i <= columnsNumber; i++) {
	            if (i > 1) System.out.print(",  ");
	            String columnValue = rs.getString(i);
	            System.out.print(rsmd.getColumnName(i) + ": "+columnValue);
	        }
		  System.out.println();
	  }
  }
  
  public static void main(String[] args)
  {
	  MySQLAccess my = new MySQLAccess();
	  
	  
	  
	  try {
		my.readDatabase();
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  
	  try{
		  my.createUser("newtest", "newpass", "New Test", "User", "newtest@user.com");
		  my.createAdmin("NewAdmin", "NewAdminPass", "New", "Admin", "new@admin.com");
		  my.createClass("Test Class", 5, "UNC", "M:0900-1000");
	  }catch(Exception e)
	  {
		  System.out.println(e);
		  e.printStackTrace();
	  }
	  
	  try{
		  String[] keys = {"ID",  "FName", "LName", "UName", "Password", "RegTime"};
		  ResultSet rs = my.runQuery("SELECT", "Users", keys, null, null, null, 0, 0);
		  dump(rs);
		  
		  String[] keys2 = {"ClassID", "ClassName", "Institution", "AdminID", "MeetTimes"};
		  rs = my.runQuery("SELECT", "Classes", keys2, null, null, null, 0, 0);
		  dump(rs);
		  
		  rs = my.runQuery("SELECT", "Admins", keys, null, null, null, 0, 0);
		  dump(rs);
	  }catch(Exception e)
	  {
		  System.out.println(e);
		  e.printStackTrace();
	  }
  }
}
