import java.sql.*;
import java.util.HashMap;
import java.util.Map;
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

            System.out.println("\nCreating SUPPLIERS table");
            String update = "create table SUPPLIERS "
                    + "(SUP_ID integer NOT NULL, "
                    + "SUP_NAME varchar(40) NOT NULL, "
                    + "STREET varchar(40) NOT NULL, "
                    + "CITY varchar(20) NOT NULL, "
                    + "STATE char(2) NOT NULL, "
                    + "ZIP char(5), "
                    + "PRIMARY KEY (SUP_ID))";
            executeUpdate(myConnection, update);

            System.out.println("\nPopulating SUPPLIERS table");
            update = "insert into SUPPLIERS values(49, 'Superior Coffee', '1 Party Place', 'Mendocino', 'CA', '95460')";
            executeUpdate(myConnection, update);
            update = "insert into SUPPLIERS values(101, 'Acme, Inc.', '99 Market Street', 'Groundsville', 'CA', '95199')";
            executeUpdate(myConnection, update);
            update = "insert into SUPPLIERS values(150, 'The High Ground', '100 Coffee Lane', 'Meadows', 'CA', '93966')";
            executeUpdate(myConnection, update);

            System.out.println("\nCreating COFFEES table");
            update = "create table COFFEES "
                    + "(COF_NAME varchar(32) NOT NULL, "
                    + "SUP_ID int NOT NULL, "
                    + "PRICE numeric(10,2) NOT NULL, "
                    + "SALES integer NOT NULL, "
                    + "TOTAL integer NOT NULL, "
                    + "PRIMARY KEY (COF_NAME), "
                    + "FOREIGN KEY (SUP_ID) REFERENCES SUPPLIERS (SUP_ID))";
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
            printCoffeesTable(myConnection);

            System.out.println("\nRaising coffee prices by 25%");
            Statement stmt = null;
            try {
                stmt = myConnection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet uprs = stmt.executeQuery("SELECT * FROM COFFEES");
                while (uprs.next()) {
                    float f = uprs.getFloat("PRICE");
                    uprs.updateFloat("PRICE", f * 1.25f);
                    uprs.updateRow();
                }
            } catch (SQLException e) {
                printSQLException(e);
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
            printCoffeesTable(myConnection);

            System.out.println("\nInserting a new row:");
            stmt = null;
            try {
                stmt = myConnection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet uprs = stmt.executeQuery("SELECT * FROM COFFEES");
                uprs.moveToInsertRow();
                uprs.updateString("COF_NAME", "Kona");
                uprs.updateInt("SUP_ID", 150);
                uprs.updateFloat("PRICE", 10.99f);
                uprs.updateInt("SALES", 0);
                uprs.updateInt("TOTAL", 0);
                uprs.insertRow();
                uprs.beforeFirst();
            } catch (SQLException e) {
                printSQLException(e);
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
            printCoffeesTable(myConnection);

            System.out.println("\nUpdating sales of coffee per week:");
            HashMap<String, Integer> salesCoffeeWeek = new HashMap<String, Integer>();
            salesCoffeeWeek.put("Colombian", 175);
            salesCoffeeWeek.put("French_Roast", 150);
            salesCoffeeWeek.put("Espresso", 60);
            salesCoffeeWeek.put("Colombian_Decaf", 155);
            salesCoffeeWeek.put("French_Roast_Decaf", 90);
            PreparedStatement updateSales = null;
            PreparedStatement updateTotal = null;
            try {
                myConnection.setAutoCommit(false);
                updateSales = myConnection.prepareStatement("update COFFEES set SALES = ? where COF_NAME = ?");
                updateTotal = myConnection.prepareStatement("update COFFEES set TOTAL = TOTAL + ? where COF_NAME = ?");
                for (Map.Entry<String, Integer> e : salesCoffeeWeek.entrySet()) {
                    updateSales.setInt(1, e.getValue().intValue());
                    updateSales.setString(2, e.getKey());
                    updateSales.executeUpdate();
                    updateTotal.setInt(1, e.getValue().intValue());
                    updateTotal.setString(2, e.getKey());
                    updateTotal.executeUpdate();
                    myConnection.commit();
                }
            } catch (SQLException e) {
                printSQLException(e);
                if (myConnection != null) {
                    try {
                        System.err.print("Transaction is being rolled back");
                        myConnection.rollback();
                    } catch (SQLException excep) {
                        printSQLException(excep);
                    }
                }
            } finally {
                if (updateSales != null) {
                    updateSales.close();
                }
                if (updateTotal != null) {
                    updateTotal.close();
                }
                myConnection.setAutoCommit(true);
            }
            printCoffeesTable(myConnection);

            System.out.println("\nModifying prices by percentage");
            String coffeeName = "Colombian";
            float priceModifier = 0.10f;
            float maximumPrice = 9.00f;
            myConnection.setAutoCommit(false);
            Statement getPrice = null;
            Statement updatePrice = null;
            ResultSet rs = null;
            String query = "SELECT COF_NAME, PRICE FROM COFFEES WHERE COF_NAME = '" + coffeeName + "'";
            try {
                Savepoint save1 = myConnection.setSavepoint();
                getPrice = myConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                updatePrice = myConnection.createStatement();
                if (!getPrice.execute(query)) {
                    System.out.println("Could not find entry for coffee named " + coffeeName);
                } else {
                    rs = getPrice.getResultSet();
                    rs.first();
                    float oldPrice = rs.getFloat("PRICE");
                    float newPrice = oldPrice + (oldPrice * priceModifier);
                    System.out.println("Old price of " + coffeeName + " is " + oldPrice);
                    System.out.println("New price of " + coffeeName + " is " + newPrice);
                    System.out.println("Performing update...");
                    updatePrice.executeUpdate("UPDATE COFFEES SET PRICE = " + newPrice + " WHERE COF_NAME = '" + coffeeName + "'");
                    printCoffeesTable(myConnection);
                    if (newPrice > maximumPrice) {
                        System.out.println("\nThe new price, " + newPrice + ", is greater than the maximum " + "price, " + maximumPrice + ". Rolling back the transaction...");
                        myConnection.rollback(save1);
                        printCoffeesTable(myConnection);
                    }
                    myConnection.commit();
                }
            } catch (SQLException e) {
                printSQLException(e);
            } finally {
                if (getPrice != null) {
                    getPrice.close();
                }
                if (updatePrice != null) {
                    updatePrice.close();
                }
                myConnection.setAutoCommit(true);
            }

            System.out.println("\nDropping COFFEES table:");
            update = "DROP TABLE COFFEES";
            executeUpdate(myConnection, update);

            System.out.println("\nDropping SUPPLIERS table:");
            update = "DROP TABLE SUPPLIERS";
            executeUpdate(myConnection, update);

        } catch (SQLException e) {
            printSQLException(e);
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
            printSQLException(e);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    public static void printCoffeesTable(Connection con) throws SQLException {
        System.out.println("\nContents of COFFEES table:");
        Statement stmt = null;
        try {
            stmt = con.createStatement();
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
            printSQLException(e);
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
            printSQLException(sqle);
        }
    }

    public static void printSQLException(SQLException ex) {
        for (Throwable e : ex) {
            if (e instanceof SQLException) {
                if (ignoreSQLException(((SQLException) e).getSQLState()) == false) {
                    e.printStackTrace(System.err);
                    System.err.println("SQLState: " + ((SQLException) e).getSQLState());
                    System.err.println("Error Code: " + ((SQLException) e).getErrorCode());
                    System.err.println("Message: " + e.getMessage());
                    Throwable t = ex.getCause();
                    while (t != null) {
                        System.out.println("Cause: " + t);
                        t = t.getCause();
                    }
                }
            }
        }
    }

    public static boolean ignoreSQLException(String sqlState) {
        if (sqlState == null) {
            System.out.println("The SQL state is not defined!");
            return false;
        }
        // X0Y32: Jar file already exists in schema
        if (sqlState.equalsIgnoreCase("X0Y32"))
            return true;
        // 42Y55: Table already exists in schema
        if (sqlState.equalsIgnoreCase("42Y55"))
            return true;
        return false;
    }

}
