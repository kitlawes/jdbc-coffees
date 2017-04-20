import java.sql.*;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        Connection myConnection = null;
        try {

            System.out.println("Connecting to database");
            Properties connectionProps = new Properties();
            connectionProps.put("user", "");
            connectionProps.put("password", "");
            myConnection = DriverManager.getConnection("jdbc:derby:database;create=true", connectionProps);

            System.out.println("\nCreating COFFEES table");
            String update = "create table COFFEES "
                    + "(COF_NAME varchar(32) NOT NULL, "
                    + "SUP_ID int NOT NULL, "
                    + "PRICE numeric(10,2) NOT NULL, "
                    + "SALES integer NOT NULL, "
                    + "TOTAL integer NOT NULL, "
                    + "PRIMARY KEY (COF_NAME))";
            executeUpdate(myConnection, update);
            
            System.out.println("\nPopulating COFFEES table");
            update = "insert into COFFEES values('Colombian', 00101, 7.99, 0, 0)";
            executeUpdate(myConnection, update);
            update = "insert into COFFEES values('French_Roast', 00049, 8.99, 0, 0)";
            executeUpdate(myConnection, update);
            update = "insert into COFFEES values('Espresso', 00150, 9.99, 0, 0)";
            executeUpdate(myConnection, update);
            update = "insert into COFFEES values('Colombian_Decaf', 00101, 8.99, 0, 0)";
            executeUpdate(myConnection, update);
            update = "insert into COFFEES values('French_Roast_Decaf', 00049, 9.99, 0, 0)";
            executeUpdate(myConnection, update);

            System.out.println("\nContents of COFFEES table:");
            Statement stmt = null;
            try {
                stmt = myConnection.createStatement();
                ResultSet rs = stmt.executeQuery("select COF_NAME, SUP_ID, PRICE, SALES, TOTAL from COFFEES");
                while (rs.next()) {
                    String coffeeName = rs.getString("COF_NAME");
                    int supplierID = rs.getInt("SUP_ID");
                    float price = rs.getFloat("PRICE");
                    int sales = rs.getInt("SALES");
                    int total = rs.getInt("TOTAL");
                    System.out.println(coffeeName
                            + ", " + supplierID
                            + ", " + price
                            + ", " + sales
                            + ", " + total);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }

            System.out.println("\nDropping COFFEES table:");
            update = "DROP TABLE COFFEES";
            executeUpdate(myConnection, update);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection(myConnection);
        }
    }

    public static void executeUpdate(Connection con, String update) throws SQLException {
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            stmt.executeUpdate(update);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    public static void closeConnection(Connection connArg) {
        System.out.println("\nReleasing all open resources ...");
        try {
            if (connArg != null) {
                connArg.close();
                connArg = null;
            }
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }

}
