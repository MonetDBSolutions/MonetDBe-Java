import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class MultiThreadRun implements Runnable {
    String url;
    boolean log;
    String tableName;
    String threadName;
    int threadNum;
    Connection c;

    public MultiThreadRun(String url, boolean log, int threadNum) {
        this.log = log;
        this.tableName = "a_" + threadNum;
        this.threadName = Thread.currentThread().getName();
        this.threadNum = threadNum;
        if (log)
            this.url = url + "?logfile=/Users/bernardo/Monet/MonetDBe-Java/logfile";
        else
            this.url = url;
    }

    public MultiThreadRun(Connection c, int threadNum) {
        this.threadNum = threadNum;
        this.tableName = "a_" + threadNum;
        this.threadName = Thread.currentThread().getName();
        this.c = c;
    }

    @Override
    public void run() {
        System.out.println("Running thread " + threadName + "." + threadNum);
        try {
            if (this.c == null) {
                System.out.println("Thread " + threadName + "." + threadNum + ":Starting new connection");
                this.c = DriverManager.getConnection(this.url);
            }
            Statement stat = c.createStatement();

            if (log) {
                stat.execute("CALL logging.setflushlevel('debug');");
                stat.execute("CALL logging.setlayerlevel('MDB_ALL','debug')");
            }

            ResultSet rs = null;
            System.out.println("Thread " + threadName + "." + threadNum + ": Create table " + tableName);
            stat.execute("CREATE TABLE " + tableName + " (a INT)");
            System.out.println("Thread " + threadName + "." + threadNum + ": Insert into");
            stat.execute("INSERT INTO " + tableName + " VALUES (1)");
            System.out.println("Thread " + threadName + "." + threadNum + ": Query");
            rs = stat.executeQuery("SELECT * FROM " + tableName);
            while(rs.next())
                System.out.println("Thread " + threadName + "." + threadNum + ": " + tableName + " -> " + rs.getObject(1));


            rs = stat.executeQuery("SELECT * FROM  a_0");
            while(rs.next())
                System.out.println("Thread " + threadName + "." + threadNum + ": a_0 -> " + rs.getObject(1));
            System.out.println("Thread " + threadName + "." + threadNum + ": Closing connection");
            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}

public class MultiThreadAccess {
    public static void main(String[] args) {
        boolean useSameConnection = false;

        String urlMemory = "jdbc:monetdb:memory:";
        String urlFile = "jdbc:monetdb:file:/tmp/test/";
        Connection c = null;

        int n = 2;
        ExecutorService executor = Executors.newFixedThreadPool(n);
        try {
            if (useSameConnection)
                c = DriverManager.getConnection(urlMemory);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < n; i++) {
            Runnable t = null;
            if (useSameConnection)
                t = new MultiThreadRun(c,i);
            else
                t = new MultiThreadRun(urlMemory,false,i);

            executor.execute(t);
        }
        executor.shutdown();
    }
}
