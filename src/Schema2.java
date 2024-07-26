import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class Schema2 {
	
//	CREATE TABLE Employee(Fname CHAR(20), Minit CHAR(10), Lname CHAR(20), ssn INT PRIMARY KEY, Bdate date, address CHAR(20), sex CHARACTER(1), salary INT, Super_snn INT REFERENCES Employee(ssn), dno INT);

	 public static long insertEmployee(String Fname, String Minit,String Lname,int ssn, Date Bdate, String address, String sex, int salary, int superSSN, int dno, Connection conn) {
         String SQL = "INSERT INTO Employee(Fname,Minit,Lname,ssn,Bdate,address,sex,salary,Super_snn,dno) "
                 + "VALUES(?,?,?,?,?,?,?,?,?,?);";
      
         long id = 0;
        try{
        	 conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);
     
                pstmt.setString(1, Fname);
                pstmt.setString(2, Minit);
                pstmt.setString(3, Lname);
                pstmt.setInt(4, ssn);
                pstmt.setDate(5, Bdate);
                pstmt.setString(6, address);
                pstmt.setString(7, sex);
                pstmt.setInt(8, salary);
                pstmt.setInt(9, superSSN);
                pstmt.setInt(10, dno);

             int affectedRows = pstmt.executeUpdate();
             System.out.println("Number of affected rows is " + affectedRows);
             // check the affected rows 
             if (affectedRows > 0) {
                 // get the ID back
                 try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                     if (rs.next()) {
                         id = rs.getLong(4);
                         pstmt.close();
                         conn.commit();
                     }
                 } catch (SQLException ex) {
                	 ex.printStackTrace();
                     System.out.println(ex.getMessage());
                 }
             }
         } catch (SQLException ex) {
             System.out.println(ex.getMessage());
             ex.printStackTrace();
         }
         return id;
     }
//	 CREATE TABLE Department(Dname CHAR(20), Dnumber INT PRIMARY KEY, Mgr_snn int REFERENCES employee, Mgr_start_date date );

	 public static long insertDepartment(String Dname, int Dnumber,int MgrSSN, Date startDate, Connection conn) {
         String SQL = "INSERT INTO Department(Dname,Dnumber,Mgr_snn,Mgr_start_date) "
                 + "VALUES(?,?,?,?);";
      
         long id = 0;
        try{
        	 conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);
     
                pstmt.setString(1, Dname);
                pstmt.setInt(2, Dnumber);
                pstmt.setInt(3, MgrSSN);
                pstmt.setDate(4, startDate);
                

             int affectedRows = pstmt.executeUpdate();
             System.out.println("Number of affected rows is " + affectedRows);
             // check the affected rows 
             if (affectedRows > 0) {
                 // get the ID back
                 try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                     if (rs.next()) {
                         id = rs.getLong(2);
                         pstmt.close();
                         conn.commit();
                     }
                 } catch (SQLException ex) {
                	 ex.printStackTrace();
                     System.out.println(ex.getMessage());
                 }
             }
         } catch (SQLException ex) {
             System.out.println(ex.getMessage());
             ex.printStackTrace();
         }
         return id;
     }
//	 CREATE TABLE Dept_locations(Dnumber integer REFERENCES Department, Dlocation CHAR(20), PRIMARY KEY(Dnumber,Dlocation));
	 public static long insertDeptLocations(int Dnumber,String Dlocation, Connection conn) {
         String SQL = "INSERT INTO Dept_locations(Dnumber,Dlocation) "
                 + "VALUES(?,?);";
      
         long id = 0;
        try{
        	 conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);
     
                pstmt.setString(2, Dlocation);
                pstmt.setInt(1, Dnumber);
                
                

             int affectedRows = pstmt.executeUpdate();
             System.out.println("Number of affected rows is " + affectedRows);
             // check the affected rows 
             if (affectedRows > 0) {
                 // get the ID back
                 try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                     if (rs.next()) {
                         id = rs.getLong(1);
                         pstmt.close();
                         conn.commit();
                     }
                 } catch (SQLException ex) {
                	 ex.printStackTrace();
                     System.out.println(ex.getMessage());
                 }
             }
         } catch (SQLException ex) {
             System.out.println(ex.getMessage());
             ex.printStackTrace();
         }
         return id;
     }

//	 CREATE TABLE Project(Pname CHAR(20), Pnumber INT PRIMARY KEY, Plocation CHAR(50), Dnumber INT REFERENCES Department);
	 public static long insertProject(String Pname, int Pnumber,String pLocation, int Dnumber, Connection conn) {
         String SQL = "INSERT INTO Project(Pname,Pnumber,Plocation,Dnumber) "
                 + "VALUES(?,?,?,?);";
      
         long id = 0;
        try{
        	 conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);
     
                pstmt.setString(1, Pname);
                pstmt.setInt(2, Pnumber);
                pstmt.setString(3, pLocation);
                pstmt.setInt(4, Dnumber);
                

             int affectedRows = pstmt.executeUpdate();
             System.out.println("Number of affected rows is " + affectedRows);
             // check the affected rows 
             if (affectedRows > 0) {
                 // get the ID back
                 try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                     if (rs.next()) {
                         id = rs.getLong(2);
                         pstmt.close();
                         conn.commit();
                     }
                 } catch (SQLException ex) {
                	 ex.printStackTrace();
                     System.out.println(ex.getMessage());
                 }
             }
         } catch (SQLException ex) {
             System.out.println(ex.getMessage());
             ex.printStackTrace();
         }
         return id;
     }
//	 CREATE TABLE Works_on(Essn int REFERENCES Employee, Pno int REFERENCES Project, Hours int, PRIMARY KEY(Essn,Pno));

	 public static long insertWorksOn(int Essn,int pNo, int hours, Connection conn) {
         String SQL = "INSERT INTO Works_on(Essn,Pno,Hours) "
                 + "VALUES(?,?,?);";
      
         long id = 0;
        try{
        	 conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);
     
                pstmt.setInt(2, pNo);
                pstmt.setInt(1, Essn);
                pstmt.setInt(3, hours);

                

             int affectedRows = pstmt.executeUpdate();
             System.out.println("Number of affected rows is " + affectedRows);
             // check the affected rows 
             if (affectedRows > 0) {
                 // get the ID back
                 try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                     if (rs.next()) {
                         id = rs.getLong(1);
                         pstmt.close();
                         conn.commit();
                     }
                 } catch (SQLException ex) {
                	 ex.printStackTrace();
                     System.out.println(ex.getMessage());
                 }
             }
         } catch (SQLException ex) {
             System.out.println(ex.getMessage());
             ex.printStackTrace();
         }
         return id;
     }
//	 CREATE TABLE Dependent(Essn INT REFERENCES Employee, Dependent_name CHAR(20), sex CHARACTER(1), Bdate date, Relationship CHAR(20), PRIMARY KEY(Essn, Dependent_name));
	 public static long insertDependent(int Essn, String dependentName,String sex, Date Bdate,String relationship, Connection conn) {
         String SQL = "INSERT INTO Dependent(Essn,Dependent_name,sex,Bdate,Relationship) "
                 + "VALUES(?,?,?,?,?);";
      
         long id = 0;
        try{
        	 conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);
     
                pstmt.setInt(1, Essn);
                pstmt.setString(2, dependentName);
                pstmt.setString(3, sex);
                pstmt.setDate(4, Bdate);
                pstmt.setString(5, relationship);


             int affectedRows = pstmt.executeUpdate();
             System.out.println("Number of affected rows is " + affectedRows);
             // check the affected rows 
             if (affectedRows > 0) {
                 // get the ID back
                 try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                     if (rs.next()) {
                         id = rs.getLong(1);
                         pstmt.close();
                         conn.commit();
                     }
                 } catch (SQLException ex) {
                	 ex.printStackTrace();
                     System.out.println(ex.getMessage());
                 }
             }
         } catch (SQLException ex) {
             System.out.println(ex.getMessage());
             ex.printStackTrace();
         }
         return id;
     }
	 
	 /////////////////////////////////////////////// Data Population Methods //////////////////////////////////////////////////////////////
	 @SuppressWarnings("deprecation")
	 public static void populateEmployee(Connection conn) {
		 	int count=2; 
		 	int countDep=1;
		 	int salary=10000;
		 	int c=1;
	        for (int i = 1; i <= 16000; i++) { // Adjust to 16000 employees
	        	int empCounter=2;
	        	if (i==2000 || i==3000 || i==4000 || i==5000 || i==6000 || i==7000 || i==8000 || i==9000 || i==10000 || i==11000 || i==12000 || i==13000 || i==14000 || i==15000 || i==16000) {
	        		count++;
	        		c++;
	        	}
	        	if(i==16000) {
	        		salary=12000;
	        	}
	        	if(i%154==0) {
	        		countDep++;
	        		salary=salary+10000;
	        	}
	        	if(countDep==5) {
	        		salary=1000000;
	        	}
	        	if(countDep==6) {
	        		salary=60000;
	        	}
	        	String sex="M";
	        	if (i>8000) {
	        		sex="F";
	        	}
	        	if(i>153 && i<=753) {
	        		empCounter=1;
	        	}
	        	if (i>753) {
	        		empCounter=count;
	        	}
	            if (insertEmployee("employee" + i, "M" + i, "employee" + empCounter, i, new Date(22, 1, 1999), "address" + i, sex, salary, c, countDep, conn) == 0) {
	                System.err.println("Insertion of employee record " + i + " failed");
	                break;
	            }
	        }
	    }

	    // Method to populate Department table
	    public static void populateDepartment(Connection conn) {
	    	int counter=151;
	        for (int i = 1; i <= 150; i++) { // Adjust to 150 departments
	        	counter--;
	            if (insertDepartment("Department" + i, i, counter, new Date(1, 1, 1990), conn) == 0) {
	                System.err.println("Insertion of department record " + i + " failed");
	                break;
	            }
	        }
	    }

	    // Method to populate Dept_Locations table
	    public static void populateDeptLocations(Connection conn) {
	    	 int departCount=1;
	    	 for (int i = 1; i <= 150; i++) { // Adjust to 150 department locations
	    		 if(i==10 || i==20 || i==30 || i==40 || i==50 || i==60 ||i==70 || i==80 || i==90 || i==100 || i==110 || i==120 || i==130 || i==140 || i==150) {
	    			 departCount++;
	    		 }
	    		 if (insertDeptLocations(i, "Location" + departCount, conn) == 0) {
	    			 System.err.println("Insertion of department location record " + i + " failed");
	    			 break;
	            }
	        }
	    }

	    // Method to populate Project table
	    public static void populateProject(Connection conn) {
	        int departCount=1;
	        for (int i = 1; i <= 9200; i++) { // Adjust to 9200 projects
	        	if(i==2400) {
	        		departCount=4;
	        	}
	        	if(i==2400 || i==3000 || i==3600 ||i==4200 || i==4800 || i==5400 || i==6000 || i==6600 || i==7200 || i==8000 || i==8400 || i==9000) {
	    			 departCount++;
	    		 }
	            if (insertProject("Project" + i, i, "Location" + i, departCount, conn) == 0) {
	                System.err.println("Insertion of project record " + i + " failed");
	                break;
	            } else {
	                System.out.println("Insertion of project record " + i + " was successful");
	            }
	        }
	    }
	   
	    // Method to populate Works_on table
	    public static void populateWorksOn(Connection conn) {
	        int pNumber=1;
	        int theHours=20;
	        int pNo=0;
	        for (int i = 1; i <= 16000; i++) { // Adjust to 16000 works on entries
	        	if (i==1000 ||  i==2000 || i==3000 || i==4000 || i==5000 || i==6000 || i==7000 || i==8000 || i==9000 || i==10000 || i==11000 || i==12000 || i==13000 || i==14000 || i==15000 || i==16000) {
	        		theHours=theHours+2;
	        	}
	            int Essn = i;
	            if (pNo==9199) {
	            	pNo=0;
	            }
	            pNo++;
	            if (insertWorksOn(Essn, pNo, theHours, conn) == 0) {
	                System.err.println("Insertion of works on record " + i + " failed");
	                break;
	            }
	        }
	    }

	    // Method to populate Dependent table
	    public static void populateDependent(Connection conn) {
	        for (int i = 1; i <= 600; i++) { // Adjust to 10000 dependents
	            String result = "M";
	            if (insertDependent(i, "employee" + i, result,new Date(1,1,1999),"child", conn) == 0) {
	                System.err.println("Insertion of dependent record " + i + " failed");
	                break;
	            }
	        }
	    }

	    // Main method to insert schema data
	    public static void insertSchema2(Connection connection) {
	        populateEmployee(connection);
	        populateDepartment(connection);
	        populateDeptLocations(connection);
	        populateProject(connection);
	        populateWorksOn(connection);
	        populateDependent(connection);
	    }

	    public static void main(String[] argv) {

	        System.out.println("-------- PostgreSQL JDBC Connection Testing ------------");

	        try {

	            Class.forName("org.postgresql.Driver");

	        } catch (ClassNotFoundException e) {

	            System.out.println("Where is your PostgreSQL JDBC Driver? "
	                    + "Include in your library path!");
	            e.printStackTrace();
	            return;

	        }

	        System.out.println("PostgreSQL JDBC Driver Registered!");

	        Connection connection = null;

	        try {

	            connection = DriverManager.getConnection(
	                    "jdbc:postgresql://127.0.0.1:5432/schema2", "postgres",
	                    "0000");
	            insertSchema2(connection);

	        } catch (SQLException e) {

	            System.out.println("Connection Failed! Check output console");
	            e.printStackTrace();
	            return;

	        }

	        if (connection != null) {
	            System.out.println("You made it, take control your database now!");
	        } else {
	            System.out.println("Failed to make connection!");
	        }
	    }
	}