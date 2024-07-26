import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;

public class Schema1 {
	 static int inst_id=1;
	 static int stud_id=1;
	 static int course_id=1;
	 static String[] DepName = {
			    "CSEN", "Finance", "Engineering", "Marketing", "Sales", "IT", "CustomerSvc",
			    "Logistics", "Legal", "R&D", "QualityAssurance", "Production",
			    "Procurement", "Design", "Maintenance", "Operations", "Training", "Security", "Admin",
			    "PR", "Compliance", "BizDev", "ProdMgmt", "RiskMgmt",
			    "Comms", "InvestorRels", "EH&S", "StratPlanning",
			    "Facilities", "InternalAudit", "SupplyChain", "TechSupport", "Accounting", "MediaRels",
			    "ContentCreation", "Advertising", "DataAnalytics", "GovAffairs", "RegAffairs",
			    "CorpStrategy", "ClinicalRsch", "Purchasing", "Distribution", "Warehouse", "EventPlan",
			    "CommunityEngage", "TalentAcq", "NetOps", "UserExp", "CloudSvcs",
			    "Innovation", "ServiceMgmt", "ProjMgmt", "Infrastructure", "E-commerce", "Analytics",
			    "BizIntel", "TrainingDev", "FieldOps","corporate"
			};
    static String[] Names = {
    	    "Ahmed", "Backar", "Charlie", "David", "Ethan", "Frank", "George", "Henry", "Isaac", "Jack",
    	    "Kevin", "Liam", "Mason", "Nathan", "Owen", "Paul", "Quincy", "Robert", "Sam", "Tom",
    	    "Ulysses", "Victor", "William", "Xander", "Yusuf", "Zach", "Brandon", "Carter", "Daniel", "Edward",
    	    "Finn", "Gavin", "Hunter", "Ian", "James", "Kyle", "Lucas", "Matthew", "Noah", "Oliver"
    	};
	// //////////////////////////////////////////// Table Insertion Methods
	// ///////////////////////////////////////////////////////////////
	public static long insertDepartment(int building, String deptName,
			int budget, Connection conn) {
		String SQL = "INSERT INTO department(dep_name,building,budget) "
				+ "VALUES(?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL,
					Statement.RETURN_GENERATED_KEYS);

			pstmt.setInt(2, building);
			pstmt.setString(1, deptName);
			pstmt.setInt(3, budget);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	public static long insertInstructor(int ID, String name, int salary,
			String deptName, Connection conn) {
		String SQL = "INSERT INTO instructor(ID,name,salary,dep_name)"
				+ "VALUES(?,?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL,
					Statement.RETURN_GENERATED_KEYS);

			pstmt.setString(2, name);
			pstmt.setInt(1, ID);
			pstmt.setInt(3, salary);
			pstmt.setString(4, deptName);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	public static long insertClassroom(int building, int roomNo, int capacity,
			Connection conn) {
		String SQL = "INSERT INTO classroom(building,room_number,capacity)"
				+ "VALUES(?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL,
					Statement.RETURN_GENERATED_KEYS);

			pstmt.setInt(2, roomNo);
			pstmt.setInt(1, building);
			pstmt.setInt(3, capacity);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	public static long insertTimeSlot(int ID, String day, Time start, Time end,
			Connection conn) {
		String SQL = "INSERT INTO time_slot(id,day,start,end_time)"
				+ "VALUES(?,?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL,
					Statement.RETURN_GENERATED_KEYS);

			pstmt.setString(2, day);
			pstmt.setInt(1, ID);
			pstmt.setTime(3, start);
			pstmt.setTime(4, end);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	public static long insertStudent(int ID, String name, int credit,
			String deptName, int instID, Connection conn) {
		String SQL = "INSERT INTO student(id,name,tot_credit,department,advisor_id)"
				+ "VALUES(?,?,?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL,
					Statement.RETURN_GENERATED_KEYS);

			pstmt.setString(2, name);
			pstmt.setInt(1, ID);
			pstmt.setInt(3, credit);
			pstmt.setString(4, deptName);
			pstmt.setInt(5, instID);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	// CREATE TABLE course(course_id INT PRIMARY KEY, title VARCHAR(20), credits
	// INT, department VARCHAR(20) REFERENCES department);
	public static long insertCourse(int ID, String title, int credit,
			String deptName, Connection conn) {
		String SQL = "INSERT INTO course(course_id,title,credits,department)"
				+ "VALUES(?,?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL,
					Statement.RETURN_GENERATED_KEYS);

			pstmt.setString(2, title);
			pstmt.setInt(1, ID);
			pstmt.setInt(3, credit);
			pstmt.setString(4, deptName);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	// CREATE TABLE pre_requiste(course_id INT, prereq_id INT,PRIMARY
	// KEY(course_id, prereq_id));
	public static long insertPrerequiste(int ID, int preID, Connection conn) {
		String SQL = "INSERT INTO pre_requiste(course_id,prereq_id)"
				+ "VALUES(?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL,
					Statement.RETURN_GENERATED_KEYS);

			pstmt.setInt(2, preID);
			pstmt.setInt(1, ID);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	// CREATE TABLE section(section_id INT PRIMARY KEY, semester INT, year INT,
	// instructor_id INT REFERENCES instructor, course_id INT REFERENCES
	// course,classroom_building INT REFERENCES classroom(building),
	// classroom_room_no INT REFERENCES classroom(room_number));

	public static long insertSection(int ID, int semester, int year,
			int instID, int courseID, int classroomBuilding,
			int classroomRoomNo, Connection conn) {
		String SQL = "INSERT INTO section(section_id,semester,year,instructor_id,course_id,classroom_building,classroom_room_no)"
				+ "VALUES(?,?,?,?,?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL,
					Statement.RETURN_GENERATED_KEYS);

			pstmt.setInt(2, semester);
			pstmt.setInt(1, ID);
			pstmt.setInt(3, year);
			pstmt.setInt(4, instID);
			pstmt.setInt(5, courseID);
			pstmt.setInt(6, classroomBuilding);
			pstmt.setInt(7, classroomRoomNo);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	// CREATE TABLE takes(student_id INT REFERENCES student, section_id INT
	// REFERENCES section, grade real, PRIMARY KEY(student_id, section_id));
	public static long insertTakes(int ID, int secID, double grade,
			Connection conn) {
		String SQL = "INSERT INTO takes(student_id,section_id,grade)"
				+ "VALUES(?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL,
					Statement.RETURN_GENERATED_KEYS);

			pstmt.setInt(2, secID);
			pstmt.setInt(1, ID);
			pstmt.setDouble(3, grade);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	// CREATE TABLE section_time(time_slot INT REFERENCES time_slot, section_id
	// INT REFERENCES section, PRIMARY KEY(time_slot, section_id));
	public static long insertSectionTime(int timeSlot, int secID,
			Connection conn) {
		String SQL = "INSERT INTO section_time(time_slot,section_id)"
				+ "VALUES(?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL,
					Statement.RETURN_GENERATED_KEYS);

			pstmt.setInt(2, secID);
			pstmt.setInt(1, timeSlot);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	// ///////////////////////////////////////// Data Population Method
	// //////////////////////////////////////////////////////
	public static void populateDepartment(Connection conn) {
		for (int i = 0; i < 60; i++) {
			int building=((int)(Math.random()*3))+1;
			int budget= ((int)(Math.random()*80000))+20000;
			if (insertDepartment(building,  DepName[i], budget, conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
		}
	}

	public static void populateInstructor(Connection conn) {
		for(int i=0;i<60;i++) {
			int rand = ((int)(Math.random()*26))+10;
			for (int j = 0; j < rand; j++) {
				if (insertInstructor(inst_id, Names[(int)(Math.random()*40)] +" "+ Names[(int)(Math.random()*40)], ((int)(Math.random()*10000))+5000, DepName[i] , conn) == 0) {
					System.err.println("insertion of record " + i + " failed");
					break;
				} else
					System.out.println("insertion was successful");
				inst_id++;
			}
		}
	}

	public static void populateClassroom(Connection conn) {
		for (int i = 1; i < 10000; i++) {
			if (insertClassroom(i, i, 100 + i, conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
		}
	}

	@SuppressWarnings("deprecation")
	public static void populateTimeSlot(Connection conn) {
		for (int i = 1; i < 10000; i++) {
			if (insertTimeSlot(i, "day" + i, new Time(12, 0, 0), new Time(13,
					0, 0), conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
		}
	}

	public static void populateStudent(Connection conn) {
		for(int i=0;i<60;i++) {
			for (int j = 0; j < 1100; j++) {
				int credit=(int)(Math.random()*201);
				int inst=((int)(Math.random()*inst_id));
				if(inst==0) {
					inst=1;
				}
				if (insertStudent(stud_id, Names[(int)(Math.random()*40)] +" "+ Names[(int)(Math.random()*40)], credit, DepName[i] , inst, conn) == 0) {
					System.err.println("insertion of record " + stud_id + " failed");
					break;
				} else
					System.out.println("insertion was successful");
				stud_id++;
			}
		}
	}

	public static void populateCourse(Connection conn) {
		for (int i = 0; i < 60; i++) {
			int rand=((int)(Math.random()*20))+25;
			for(int j=0;j<rand;j++) {	
				if (insertCourse(course_id, DepName[i].substring(0,2)+(j+1),(int)(Math.random()*200),DepName[i], conn) == 0) {
						System.err.println("insertion of record " + i + " failed");
						break;
					} else
						System.out.println("insertion was successful");
				course_id++;
				}
		}
	}

	public static void populatePrerequiste(Connection conn) {
		for (int i = 1; i < 10000; i++) {
			if (insertPrerequiste(i, i, conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
		}
	}

	public static void populateSection(Connection conn) {
		
		for (int i = 1; i < 10000; i++) {
			int building= (int)(Math.random()*3) +1;
			int room= (int)(Math.random()*200) +1;
			if(i<20) {
				int courseid=(int)(Math.random()*25)+1;
				int instid=(int)(Math.random()*10)+1;
				if (insertSection(i, 1, 2024, instid, courseid, building, room, conn) == 0) {
					System.err.println("insertion of record " + i + " failed");
					break;
				} else
					System.out.println("insertion was successful");
				
			}
			else {
				int courseid2=(int)(Math.random()*1200);
				if(courseid2==0) {
					courseid2=1;
				}
				int instid2=(int)(Math.random()*600)+1;
				int sem=(int)(Math.random()*11);
				int year =(int)(Math.random()*19)+2006;
			if (insertSection(i, sem, year, instid2, courseid2, building, room, conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
		
			}
		}
	}

	public static void populateTakes(Connection conn) {
		double j = 0.7;
		for (int i = 1; i < stud_id; i++) {
			if (j > 5)
				j = 0.7;
			if(i<900) {
				int rand_sec=((int)Math.random()*20)+1;
				if (insertTakes(i, rand_sec, j, conn) == 0) {
					System.err.println("insertion of record " + i + " failed");
					break;
				} else
					System.out.println("insertion was successful");
				j += 0.3;
			}else {
				int rand_sec2=((int)Math.random()*179)+21;
			if (insertTakes(i, rand_sec2, j, conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
			j += 0.3;
			}
			}
	}

	public static void populateSectionTime(Connection conn) {
		for (int i = 1; i < 10000; i++) {
			if (insertSectionTime(i, i, conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
		}
	}

	public static void insertSchema1(Connection connection) {
		populateDepartment(connection);
		populateInstructor(connection);
		populateClassroom(connection);
		populateTimeSlot(connection);
		populateStudent(connection);
		populateCourse(connection);
		populatePrerequiste(connection);
		populateSection(connection);
		populateTakes(connection);
		populateSectionTime(connection);
	}

	public static void main(String[] argv) {

		System.out.println("-------- PostgreSQL "
				+ "JDBC Connection Testing ------------");

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
					"jdbc:postgresql://127.0.0.1:5432/schema1", "postgres",
					"0000");
			insertSchema1(connection);

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