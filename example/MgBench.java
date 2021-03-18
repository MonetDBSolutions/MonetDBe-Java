import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class MgBench {
    private static HashMap<Integer,String> create = new HashMap<Integer, String>() {{
        put(1,"CREATE TABLE logs ( log_time      TIMESTAMP NOT NULL, machine_name  VARCHAR(25) NOT NULL, machine_group VARCHAR(15) NOT NULL, cpu_idle      FLOAT, cpu_nice      FLOAT, cpu_system    FLOAT, cpu_user      FLOAT, cpu_wio       FLOAT, disk_free     FLOAT, disk_total    FLOAT, part_max_used FLOAT, load_fifteen  FLOAT, load_five     FLOAT, load_one      FLOAT, mem_buffers   FLOAT, mem_cached    FLOAT, mem_free      FLOAT, mem_shared    FLOAT, swap_free     FLOAT, bytes_in      FLOAT, bytes_out     FLOAT );");
        put(2,"CREATE TABLE logs ( log_time    TIMESTAMP NOT NULL, client_ip   VARCHAR(15) NOT NULL, request     VARCHAR(1000) NOT NULL, status_code SMALLINT NOT NULL, object_size BIGINT NOT NULL );");
        put(3,"CREATE TABLE logs ( log_time     TIMESTAMP NOT NULL, device_id    CHAR(15) NOT NULL, device_name  VARCHAR(25) NOT NULL, device_type  VARCHAR(15) NOT NULL, device_floor SMALLINT NOT NULL, event_type   VARCHAR(15) NOT NULL, event_unit   CHAR(1), event_value  FLOAT );");
    }};

    private static HashMap<Integer,String> load = new HashMap<Integer, String>() {{
        put(1,"COPY 1000000 OFFSET 2 RECORDS INTO logs FROM '%s' USING DELIMITERS ',' NULL AS ''");
        put(2,"COPY 75748119 OFFSET 2 RECORDS INTO logs FROM '%s' USING DELIMITERS ',' NULL AS ''");
        put(3,"COPY 108957040 OFFSET 2 RECORDS INTO logs FROM '%s' USING DELIMITERS ',' NULL AS ''");
    }};

    private static HashMap<Integer,String> bench1 = new HashMap<Integer, String>() {{
        put(1,"SELECT machine_name,\n" +
                "        MIN(cpu) AS cpu_min,\n" +
                "        MAX(cpu) AS cpu_max,\n" +
                "        AVG(cpu) AS cpu_avg,\n" +
                "        MIN(net_in) AS net_in_min,\n" +
                "        MAX(net_in) AS net_in_max,\n" +
                "        AVG(net_in) AS net_in_avg,\n" +
                "        MIN(net_out) AS net_out_min,\n" +
                "        MAX(net_out) AS net_out_max,\n" +
                "        AVG(net_out) AS net_out_avg\n" +
                "        FROM (\n" +
                "        SELECT machine_name,\n" +
                "                COALESCE(cpu_user, 0.0) AS cpu,\n" +
                "                COALESCE(bytes_in, 0.0) AS net_in,\n" +
                "                COALESCE(bytes_out, 0.0) AS net_out\n" +
                "        FROM logs\n" +
                "        WHERE machine_name IN ('anansi','aragog','urd')\n" +
                "            AND log_time >= TIMESTAMP '2017-01-11 00:00:00'\n" +
                "        ) AS r\n" +
                "        GROUP BY machine_name;");
        put(2,"SELECT machine_name,\n" +
                "       log_time\n" +
                "FROM logs\n" +
                "WHERE (machine_name LIKE 'cslab%' OR\n" +
                "       machine_name LIKE 'mslab%')\n" +
                "  AND load_one IS NULL\n" +
                "  AND log_time >= TIMESTAMP '2017-01-10 00:00:00'\n" +
                "ORDER BY machine_name,\n" +
                "         log_time;");
        put(3,"SELECT dt,\n" +
                "       hr,\n" +
                "       AVG(load_fifteen) AS load_fifteen_avg,\n" +
                "       AVG(load_five) AS load_five_avg,\n" +
                "       AVG(load_one) AS load_one_avg,\n" +
                "       AVG(mem_free) AS mem_free_avg,\n" +
                "       AVG(swap_free) AS swap_free_avg\n" +
                "FROM (\n" +
                "  SELECT CAST(log_time AS DATE) AS dt,\n" +
                "         EXTRACT(HOUR FROM log_time) AS hr,\n" +
                "         load_fifteen,\n" +
                "         load_five,\n" +
                "         load_one,\n" +
                "         mem_free,\n" +
                "         swap_free\n" +
                "  FROM logs\n" +
                "  WHERE machine_name = 'babbage'\n" +
                "    AND load_fifteen IS NOT NULL\n" +
                "    AND load_five IS NOT NULL\n" +
                "    AND load_one IS NOT NULL\n" +
                "    AND mem_free IS NOT NULL\n" +
                "    AND swap_free IS NOT NULL\n" +
                "    AND log_time >= TIMESTAMP '2017-01-01 00:00:00'\n" +
                ") AS r\n" +
                "GROUP BY dt,\n" +
                "         hr\n" +
                "ORDER BY dt,\n" +
                "         hr;");
        put(4,"SELECT machine_name,\n" +
                "       COUNT(*) AS spikes\n" +
                "FROM logs\n" +
                "WHERE machine_group = 'Servers'\n" +
                "  AND cpu_wio > 0.99\n" +
                "  AND log_time >= TIMESTAMP '2016-12-01 00:00:00'\n" +
                "  AND log_time < TIMESTAMP '2017-01-01 00:00:00'\n" +
                "GROUP BY machine_name\n" +
                "ORDER BY spikes DESC\n" +
                "LIMIT 10;");
        put(5,"SELECT machine_name,\n" +
                "       dt,\n" +
                "       MIN(mem_free) AS mem_free_min\n" +
                "FROM (\n" +
                "  SELECT machine_name,\n" +
                "         CAST(log_time AS DATE) AS dt,\n" +
                "         mem_free\n" +
                "  FROM logs\n" +
                "  WHERE machine_group = 'DMZ'\n" +
                "    AND mem_free IS NOT NULL\n" +
                ") AS r\n" +
                "GROUP BY machine_name,\n" +
                "         dt\n" +
                "HAVING MIN(mem_free) < 10000\n" +
                "ORDER BY machine_name,\n" +
                "         dt;");
        put(6,"SELECT dt,\n" +
                "       hr,\n" +
                "       SUM(net_in) AS net_in_sum,\n" +
                "       SUM(net_out) AS net_out_sum,\n" +
                "       SUM(net_in) + SUM(net_out) AS both_sum\n" +
                "FROM (\n" +
                "  SELECT CAST(log_time AS DATE) AS dt,\n" +
                "         EXTRACT(HOUR FROM log_time) AS hr,\n" +
                "         COALESCE(bytes_in, 0.0) / 1000000000.0 AS net_in,\n" +
                "         COALESCE(bytes_out, 0.0) / 1000000000.0 AS net_out\n" +
                "  FROM logs\n" +
                "  WHERE machine_name IN ('allsorts','andes','bigred','blackjack','bonbon',\n" +
                "      'cadbury','chiclets','cotton','crows','dove','fireball','hearts','huey',\n" +
                "      'lindt','milkduds','milkyway','mnm','necco','nerds','orbit','peeps',\n" +
                "      'poprocks','razzles','runts','smarties','smuggler','spree','stride',\n" +
                "      'tootsie','trident','wrigley','york')\n" +
                ") AS r\n" +
                "GROUP BY dt,\n" +
                "         hr\n" +
                "ORDER BY both_sum DESC\n" +
                "LIMIT 10;");
    }};

    private static HashMap<Integer,String> bench2 = new HashMap<Integer, String>() {{
        put(1,"SELECT *\n" +
                "FROM logs\n" +
                "WHERE status_code >= 500\n" +
                "  AND log_time >= TIMESTAMP '2012-12-18 00:00:00'\n" +
                "ORDER BY log_time;\n" +
                "    \"\"\"},\n" +
                "    {'bench': 2, 'query': 2, \n" +
                "    'sql': \"\"\"\n" +
                "SELECT *\n" +
                "FROM logs\n" +
                "WHERE status_code >= 200\n" +
                "  AND status_code < 300\n" +
                "  AND request LIKE '%/etc/passwd%'\n" +
                "  AND log_time >= TIMESTAMP '2012-05-06 00:00:00'\n" +
                "  AND log_time < TIMESTAMP '2012-05-20 00:00:00';");
        put(3,"SELECT top_level,\n" +
                "       AVG(LENGTH(request) - LENGTH(REPLACE(request, '/', ''))) AS depth\n" +
                "FROM (\n" +
                "  SELECT SUBSTRING(request FROM 1 FOR len) AS top_level,\n" +
                "         request\n" +
                "  FROM (\n" +
                "    SELECT POSITION('/' IN SUBSTRING(request FROM 2)) AS len,\n" +
                "           request\n" +
                "    FROM logs\n" +
                "    WHERE status_code >= 200\n" +
                "      AND status_code < 300\n" +
                "      AND log_time >= TIMESTAMP '2012-12-01 00:00:00'\n" +
                "  ) AS r\n" +
                "  WHERE len > 0\n" +
                ") AS s\n" +
                "WHERE top_level IN ('/about','/courses','/degrees','/events',\n" +
                "                    '/grad','/industry','/news','/people',\n" +
                "                    '/publications','/research','/teaching','/ugrad')\n" +
                "GROUP BY top_level\n" +
                "ORDER BY top_level;");
        put(4,"SELECT client_ip,\n" +
                "       COUNT(*) AS num_requests\n" +
                "FROM logs\n" +
                "WHERE log_time >= TIMESTAMP '2012-10-01 00:00:00'\n" +
                "GROUP BY client_ip\n" +
                "HAVING COUNT(*) >= 100000\n" +
                "ORDER BY num_requests DESC;\n" +
                "    \"\"\"},\n" +
                "    {'bench': 2, 'query': 5, \n" +
                "    'sql': \"\"\"\n" +
                "SELECT mo,\n" +
                "       COUNT(DISTINCT client_ip)\n" +
                "FROM (\n" +
                "  SELECT EXTRACT(MONTH FROM log_time) AS mo,\n" +
                "         client_ip\n" +
                "  FROM logs\n" +
                ") AS r\n" +
                "GROUP BY mo\n" +
                "ORDER BY mo;");
        put(6,"SELECT AVG(bandwidth) / 1000000000.0 AS avg_bandwidth,\n" +
                "       MAX(bandwidth) / 1000000000.0 AS peak_bandwidth\n" +
                "FROM (\n" +
                "  SELECT log_time,\n" +
                "         SUM(object_size) AS bandwidth\n" +
                "  FROM logs\n" +
                "  GROUP BY log_time\n" +
                ") AS r;");
    }};

    private static HashMap<Integer,String> bench3 = new HashMap<Integer, String>() {{
        put(1,"SELECT *\n" +
                "FROM logs\n" +
                "WHERE event_type = 'temperature'\n" +
                "  AND event_value <= 32.0\n" +
                "  AND log_time >= TIMESTAMP '2019-11-29 17:00:00';");
        put(2,"WITH power_hourly AS (\n" +
                "  SELECT EXTRACT(HOUR FROM log_time) AS hr,\n" +
                "         device_id,\n" +
                "         device_name,\n" +
                "         CASE WHEN device_name LIKE 'coffee%' THEN 'coffee'\n" +
                "              WHEN device_name LIKE 'printer%' THEN 'printer'\n" +
                "              WHEN device_name LIKE 'projector%' THEN 'projector'\n" +
                "              WHEN device_name LIKE 'vending%' THEN 'vending'\n" +
                "              ELSE 'other'\n" +
                "         END AS device_category,\n" +
                "         device_floor,\n" +
                "         event_value\n" +
                "  FROM logs\n" +
                "  WHERE event_type = 'power'\n" +
                "    AND log_time >= TIMESTAMP '2019-11-01 00:00:00'\n" +
                ")\n" +
                "SELECT hr,\n" +
                "       device_id,\n" +
                "       device_name,\n" +
                "       device_category,\n" +
                "       device_floor,\n" +
                "       power_avg,\n" +
                "       category_power_avg\n" +
                "FROM (\n" +
                "  SELECT hr,\n" +
                "         device_id,\n" +
                "         device_name,\n" +
                "         device_category,\n" +
                "         device_floor,\n" +
                "         AVG(event_value) AS power_avg,\n" +
                "         (SELECT AVG(event_value)\n" +
                "          FROM power_hourly\n" +
                "          WHERE device_id <> r.device_id\n" +
                "            AND device_category = r.device_category\n" +
                "            AND hr = r.hr) AS category_power_avg\n" +
                "  FROM power_hourly AS r\n" +
                "  GROUP BY hr,\n" +
                "           device_id,\n" +
                "           device_name,\n" +
                "           device_category,\n" +
                "           device_floor\n" +
                ") AS s\n" +
                "WHERE power_avg >= category_power_avg * 2.0;");
        put(3,"WITH room_use AS (\n" +
                "  SELECT dow,\n" +
                "         hr,\n" +
                "         device_name,\n" +
                "         AVG(motions) AS in_use\n" +
                "  FROM (      \n" +
                "    SELECT dt,\n" +
                "           dow,\n" +
                "           hr,\n" +
                "           device_name,\n" +
                "           COUNT(*) AS motions\n" +
                "    FROM (\n" +
                "      SELECT CAST(log_time AS DATE) AS dt,\n" +
                "             EXTRACT(DOW FROM log_time) AS dow,\n" +
                "             EXTRACT(HOUR FROM log_time) AS hr,\n" +
                "             device_name\n" +
                "      FROM logs\n" +
                "      WHERE device_name LIKE 'room%'\n" +
                "        AND event_type = 'motion_start'\n" +
                "        AND log_time >= TIMESTAMP '2019-09-01 00:00:00'\n" +
                "    ) AS r\n" +
                "    WHERE dow IN (1,2,3,4,5)\n" +
                "      AND hr BETWEEN 9 AND 16\n" +
                "    GROUP BY dt,\n" +
                "             dow,\n" +
                "             hr,\n" +
                "             device_name\n" +
                "  ) AS s \n" +
                "  GROUP BY dow,\n" +
                "           hr,\n" +
                "           device_name\n" +
                ")         \n" +
                "SELECT device_name,\n" +
                "       dow, \n" +
                "       hr,  \n" +
                "       in_use\n" +
                "FROM room_use AS r\n" +
                "WHERE in_use = (\n" +
                "  SELECT MIN(in_use)\n" +
                "  FROM room_use\n" +
                "  WHERE device_name = r.device_name\n" +
                ") \n" +
                "ORDER BY device_name;");
        put(4,"SELECT device_name,\n" +
                "       device_floor,\n" +
                "       COUNT(*) AS ct\n" +
                "FROM logs\n" +
                "WHERE event_type = 'door_open'\n" +
                "  AND log_time >= TIMESTAMP '2019-06-01 00:00:00'\n" +
                "GROUP BY device_name,\n" +
                "         device_floor\n" +
                "ORDER BY ct DESC;");
        put(5,"WITH temperature AS (\n" +
                "  SELECT dt,\n" +
                "         device_name,\n" +
                "         device_type,\n" +
                "         device_floor\n" +
                "  FROM (\n" +
                "    SELECT dt,\n" +
                "           hr,\n" +
                "           device_name,\n" +
                "           device_type,\n" +
                "           device_floor,\n" +
                "           AVG(event_value) AS temperature_hourly_avg\n" +
                "    FROM (\n" +
                "      SELECT CAST(log_time AS DATE) AS dt,\n" +
                "             EXTRACT(HOUR FROM log_time) AS hr,\n" +
                "             device_name,\n" +
                "             device_type,\n" +
                "             device_floor,\n" +
                "             event_value\n" +
                "      FROM logs\n" +
                "      WHERE event_type = 'temperature'\n" +
                "    ) AS r\n" +
                "    GROUP BY dt,\n" +
                "             hr,\n" +
                "             device_name,\n" +
                "             device_type,\n" +
                "             device_floor\n" +
                "  ) AS s\n" +
                "  GROUP BY dt,\n" +
                "           device_name,\n" +
                "           device_type,\n" +
                "           device_floor\n" +
                "  HAVING MAX(temperature_hourly_avg) - MIN(temperature_hourly_avg) >= 25.0\n" +
                ")\n" +
                "SELECT DISTINCT device_name,\n" +
                "       device_type,\n" +
                "       device_floor,\n" +
                "       'WINTER'\n" +
                "FROM temperature\n" +
                "WHERE dt >= DATE '2018-12-01'\n" +
                "  AND dt < DATE '2019-03-01'\n" +
                "UNION\n" +
                "SELECT DISTINCT device_name,\n" +
                "       device_type,\n" +
                "       device_floor,\n" +
                "       'SUMMER'\n" +
                "FROM temperature\n" +
                "WHERE dt >= DATE '2019-06-01'\n" +
                "  AND dt < DATE '2019-09-01';");
        put(6,"SELECT yr,\n" +
                "       mo,\n" +
                "       SUM(coffee_hourly_avg) AS coffee_monthly_sum,\n" +
                "       AVG(coffee_hourly_avg) AS coffee_monthly_avg,\n" +
                "       SUM(printer_hourly_avg) AS printer_monthly_sum,\n" +
                "       AVG(printer_hourly_avg) AS printer_monthly_avg,\n" +
                "       SUM(projector_hourly_avg) AS projector_monthly_sum,\n" +
                "       AVG(projector_hourly_avg) AS projector_monthly_avg,\n" +
                "       SUM(vending_hourly_avg) AS vending_monthly_sum,\n" +
                "       AVG(vending_hourly_avg) AS vending_monthly_avg\n" +
                "FROM (     \n" +
                "  SELECT dt,\n" +
                "         yr,\n" +
                "         mo, \n" +
                "         hr, \n" +
                "         AVG(coffee) AS coffee_hourly_avg,\n" +
                "         AVG(printer) AS printer_hourly_avg,\n" +
                "         AVG(projector) AS projector_hourly_avg,\n" +
                "         AVG(vending) AS vending_hourly_avg\n" +
                "  FROM (\n" +
                "    SELECT CAST(log_time AS DATE) AS dt,\n" +
                "           EXTRACT(YEAR FROM log_time) AS yr,\n" +
                "           EXTRACT(MONTH FROM log_time) AS mo,\n" +
                "           EXTRACT(HOUR FROM log_time) AS hr,\n" +
                "           CASE WHEN device_name LIKE 'coffee%' THEN event_value ELSE 0 END AS coffee,\n" +
                "           CASE WHEN device_name LIKE 'printer%' THEN event_value ELSE 0 END AS printer,\n" +
                "           CASE WHEN device_name LIKE 'projector%' THEN event_value ELSE 0 END AS projector,\n" +
                "           CASE WHEN device_name LIKE 'vending%' THEN event_value ELSE 0 END AS vending\n" +
                "    FROM logs\n" +
                "    WHERE device_type = 'meter'\n" +
                "  ) AS r   \n" +
                "  GROUP BY dt,\n" +
                "           yr,\n" +
                "           mo,\n" +
                "           hr\n" +
                ") AS s \n" +
                "GROUP BY yr,\n" +
                "         mo\n" +
                "ORDER BY yr,\n" +
                "         mo;");
    }};

    private static HashMap<Integer,Map<Integer,String>> queries = new HashMap<Integer,Map<Integer,String>>() {{
        put(1,bench1);
        put(2,bench2);
        put(3,bench3);
    }};

    private static void runQuery (Connection conn, int benchID, int query) throws SQLException {
        Map<Integer,String> q = queries.get(benchID);
        if (!q.containsKey(query))
            return;
        long start = System.currentTimeMillis();
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery(q.get(query));

        if (rs!=null) {
            System.out.println("Query " + benchID + "." + query + " in " + (System.currentTimeMillis() - start) + " ms");
        }
    }

    private static void loadData (Connection conn, String csvPath, int benchID) throws SQLException {
        long start = System.currentTimeMillis();
        long updateCount;
        String csvFile = csvPath + "bench" + benchID + ".csv";
        Statement s = conn.createStatement();
        s.executeUpdate("DROP TABLE IF EXISTS logs;");
        s.executeUpdate(create.get(benchID));
        updateCount = s.executeLargeUpdate(load.get(benchID).replace("%s",csvFile));
        System.out.println("Inserted " + updateCount + " tuples in " + (System.currentTimeMillis() - start) + "ms (bench " + benchID + ")");
    }

    public static void main(String[] args) {
        String csvPath = "/Users/bernardo/Monet/MonetDBe-Java/temp/";
        Connection conn;

        try {
            conn = DriverManager.getConnection("jdbc:monetdb:memory:", null);
            System.out.println("Start mgbench");
            for (int i = 1; i <= create.size(); i++) {
                loadData(conn,csvPath,i);
                for (int j = 1; j <= queries.get(i).size(); j++) {
                    runQuery(conn,i,j);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
