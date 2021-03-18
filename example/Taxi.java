import java.sql.*;

public class Taxi {
    private static void createDB(Connection conn) {
        long start = System.currentTimeMillis();

        try {
            Statement s = conn.createStatement();
            s.execute("CREATE TABLE yellow_tripdata_2016_01 (   " +
                    "        VendorID bigint," +
                    "        tpep_pickup_datetime timestamp," +
                    "        tpep_dropoff_datetime timestamp," +
                    "        passenger_count bigint," +
                    "        trip_distance double," +
                    "        pickup_longitude double," +
                    "        pickup_latitude double," +
                    "        RatecodeID bigint," +
                    "        store_and_fwd_flag string," +
                    "        dropoff_longitude double," +
                    "        dropoff_latitude double," +
                    "        payment_type bigint," +
                    "        fare_amount double," +
                    "        extra double," +
                    "        mta_tax double," +
                    "        tip_amount double," +
                    "        tolls_amount double," +
                    "        improvement_surcharge double," +
                    "        total_amount double" +
                    "    )");
            s.close();
            System.out.println("Created table in " + (System.currentTimeMillis() - start) + " ms.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void loadDB(Connection conn, String csvPath) {
        long start = System.currentTimeMillis();

        try {
            Statement s = conn.createStatement();
            long update = s.executeLargeUpdate("COPY OFFSET 2 INTO yellow_tripdata_2016_01 " +
                    "FROM '" + csvPath + "' " +
                    "delimiters ',','\\n'  best effort");
            System.out.println("Loaded data (" + update + " tuples) in " + (System.currentTimeMillis() - start) + " ms.");
            s.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void distinct(Connection conn) {
        long start = System.currentTimeMillis();

        try {
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("SELECT" +
                    "        COUNT(DISTINCT VendorID)," +
                    "        COUNT(DISTINCT passenger_count)," +
                    "        COUNT(DISTINCT trip_distance)," +
                    "        COUNT(DISTINCT RatecodeID)," +
                    "        COUNT(DISTINCT store_and_fwd_flag)," +
                    "        COUNT(DISTINCT payment_type)," +
                    "        COUNT(DISTINCT fare_amount)," +
                    "        COUNT(DISTINCT extra)," +
                    "        COUNT(DISTINCT mta_tax)," +
                    "        COUNT(DISTINCT tip_amount)," +
                    "        COUNT(DISTINCT tolls_amount)," +
                    "        COUNT(DISTINCT improvement_surcharge)," +
                    "        COUNT(DISTINCT total_amount)" +
                    "    FROM yellow_tripdata_2016_01");

            System.out.println("Distinct in " + (System.currentTimeMillis() - start) + " ms.");
            s.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void frequency(Connection conn) {
        long start = System.currentTimeMillis();

        try {
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("SELECT" +
                    "            MIN(cnt)," +
                    "            AVG(cnt)," +
                    "            MEDIAN(cnt)," +
                    "            MAX(cnt)" +
                    "        FROM" +
                    "        (" +
                    "            SELECT " +
                    "                COUNT(*) as cnt" +
                    "            FROM yellow_tripdata_2016_01" +
                    "            GROUP BY  " +
                    "                EXTRACT(DOY FROM tpep_pickup_datetime)," +
                    "                EXTRACT(HOUR FROM tpep_pickup_datetime)" +
                    "        ) stats");

            System.out.println("Frequency in " + (System.currentTimeMillis() - start) + " ms.");
            s.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void regression (Connection conn) {
        long start = System.currentTimeMillis();

        try {
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("SELECT" +
                    "                AVG(fare_amount) + 3 * STDDEV_SAMP(fare_amount) as max_fare," +
                    "                AVG(trip_distance) + 3 * STDDEV_SAMP(trip_distance) as max_distance" +
                    "            FROM yellow_tripdata_2016_01");

            double max_fare = 0, max_distance = 0;
            if(rs.next()) {
                max_fare = rs.getDouble(1);
                max_distance = rs.getDouble(2);
            }

            rs = s.executeQuery("SELECT" +
                    "   (SUM(trip_distance * fare_amount) - SUM(trip_distance) * SUM(fare_amount) / COUNT(*)) / " +
                    "   (SUM(trip_distance * trip_distance) - SUM(trip_distance) * SUM(trip_distance) / COUNT(*)) AS beta," +
                    "   AVG(fare_amount) AS avg_fare_amount," +
                    "   AVG(trip_distance) AS avg_trip_distance" +
                    "   FROM yellow_tripdata_2016_01" +
                    "   WHERE" +
                    "   fare_amount > 0 AND" +
                    "   fare_amount < " + max_fare + " AND" +
                    "   trip_distance > 0 AND" +
                    "   trip_distance < " + max_distance);

            double beta, avg_fare_amount, avg_trip_distance;
            if(rs.next()) {
                beta = rs.getDouble(1);
                avg_fare_amount = rs.getDouble(2);
                avg_trip_distance = rs.getDouble(3);
                System.out.println("Regression results: " + beta + " " + avg_fare_amount + " " + avg_trip_distance);
            }

            System.out.println("Regression in " + (System.currentTimeMillis() - start) + " ms.");
            s.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Please input the yellow_tripdata_2016_01.csv dataset");
            return;
        }
        String csvPath = args[0];

        Connection conn = null;
        try {
            //In-memory database
            conn = DriverManager.getConnection("jdbc:monetdb:memory:",null);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (conn == null) {
            System.out.println("Could not connect to memory database");
            return;
        }

        createDB(conn);
        loadDB(conn,csvPath);
        distinct(conn);
        frequency(conn);
        regression(conn);

        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
