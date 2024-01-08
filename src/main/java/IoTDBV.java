import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class IoTDBV extends IoTDB {
    List<String> labelGarbage = new ArrayList<>();

    @Override
    public void update(List<Schema> main) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        for(Schema schema: main) {
            this.update(schema);
        }
    }

    @Override
    public void update(Schema main) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        String sql = String.format("select coalesce(main, b0) from %s", main.deviceId);
        session.executeQueryStatement(sql);
    }

    @Override
    public void align(Schema tableMain1, Schema tableMain2) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        String sql = String.format
                ("select coalesce(%s.main, %s.b0), coalesce(%s.main, %s.b0) from %s where time < %s",
                        tableMain1.key, tableMain1.key,
                        tableMain2.key, tableMain2.key, constants.IoTDB_DATABASE,
                        Double.min(constants.timeRange.get(tableMain1.main).right, constants.timeRange.get(tableMain2.main).right));
        //System.out.println("V");
        session.executeQueryStatement(sql);
    }

    @Override
    public void align(List<Schema> tableMain) throws Exception {
        for(int i=0;i<tableMain.size()-1;i++) {
            this.align(tableMain.get(i), tableMain.get(i+1));
        }
    }

    @Override
    public void rangeFilter(Schema tableMain, double selectivity) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        Range timeRange = Range.randomBySelectivity(constants.timeRange.get(tableMain.main), selectivity);
        String sql = String.format
                ("select coalesce(%s.main, %s.b0) from %s where time > %s and time < %s;",
                        tableMain.key, tableMain.key, constants.IoTDB_DATABASE,
                        timeRange.left, timeRange.right);
        // IoTDB pushes down filter.
        session.executeQueryStatement(sql);
    }

    @Override
    public void rangeFilter(List<Schema> tableMain, double selectivity) throws Exception {
        for(int i=0;i<tableMain.size();i++) {
            this.rangeFilter(tableMain.get(i), selectivity);
        }
    }

    @Override
    public void valueFilter(Schema tableMain, double selectivity) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        Range valueRange = Range.randomBySelectivity(constants.valueRange.get(tableMain.main), selectivity);
        String sql = String.format
                ("select coalesce(main, b0) from %s where b0 is not null or (A > %s and A < %s); ",
                        tableMain.deviceId,
                        valueRange.left, valueRange.right);
        session.executeQueryStatement(sql);
    }

    @Override
    public void valueFilter(List<Schema> tableMain, double selectivity) throws Exception {
        for(int i=0;i<tableMain.size();i++) {
            this.valueFilter(tableMain.get(i), selectivity);
        }
    }


    public void downSample(Schema tableMain, int window, String label) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        String sql = String.format
                ("select avg(b0-main) into %s.%s(%s) from %s group by session(%ss); ",
                        constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO, label,
                        tableMain.deviceId, window);
//        System.out.println(sql);
        String sql2 = String.format
                ("select avg(main) from %s group by session(%ss);",
                        tableMain.deviceId,window);
        session.executeNonQueryStatement(sql);
        session.executeQueryStatement(sql2);
        labelGarbage.add(label);
    }

    public void clean_temp() throws IoTDBConnectionException, StatementExecutionException {
        for(String label: this.labelGarbage) {
            try {
                session.deleteTimeseries(constants.IoTDB_DATABASE + "." + constants.IoTDB_AGG_INTO + "." + label);
            } catch (Exception e) {}
        }
    }

    @Override
    public void downSample(List<Schema> tableMain, int window) throws Exception {
        for(int i=0;i<tableMain.size();i++) {
            this.downSample(tableMain.get(i), window, tableMain.get(i).key);
        }
    }

    @Override
    public void downSampleAligned(Schema tableMain1, Schema tableMain2, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        downSample(tableMain1, window, tableMain1.key + "A");
        downSample(tableMain2, window, tableMain2.key + "B");
        String sql3 = String.format
                ("select avg(%sA), avg(%sB) from %s.%s;",
                        tableMain1.key, tableMain2.key,
                        constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO);
        session.executeQueryStatement(sql3);
        labelGarbage.add(tableMain1.key + "A"); labelGarbage.add(tableMain2.key + "B");
    }

    @Override
    public void downSampleAligned(List<Schema> tableMain, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.downSampleAligned(tableMain.get(i), tableMain.get(i+1), window);
        }
    }

    @Override
    public void downSampleUnion(Schema tableMain1, Schema tableMain2, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        downSample(tableMain1, window, tableMain1.key + "A");
        downSample(tableMain2, window, tableMain2.key + "B");
        String sql3 = String.format
                ("select coalesce(%sA, %sB) from %s.%s;",
                        tableMain1.key, tableMain2.key,
                        constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO);
        session.executeQueryStatement(sql3);
    }

    @Override
    public void downSampleUnion(List<Schema> tableMain, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.downSampleUnion(tableMain.get(i), tableMain.get(i+1), window);
        }
    }

    @Override
    public void seriesJoinValue(Schema tableMain1, Schema tableMain2) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        String sql3 = String.format
                ("select %s.main, %s.main from %s where (%s.main=%s.main and %s.b0 is null) or " +
                                "(%s.main=%s.main and %s.b0 is null) or (%s.b0=%s.main and %s.b0 is not null) or " +
                                "(%s.main=%s.main and %s.b0 is not null);",
                        tableMain1.key, tableMain2.key,
                        constants.IoTDB_DATABASE,
                        tableMain1.key, tableMain2.key, tableMain1.key,
                        tableMain1.key, tableMain2.key, tableMain2.key,
                        tableMain1.key, tableMain2.key, tableMain1.key,
                        tableMain1.key, tableMain2.key, tableMain2.key
                        );
        session.executeQueryStatement(sql3);
    }

    @Override
    public void seriesJoinValue(List<Schema> tableMain) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.seriesJoinValue(tableMain.get(i), tableMain.get(i+1));
        }
    }

    @Override
    public void branchAlign(Schema tableMain1, Schema tableMain2) throws SQLException {

    }

    @Override
    public void branchAlign(List<Schema> tableMain) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        String keys = ""; String filter = "";
        for(int i=0;i<tableMain.size();i++) {
            Schema schema = tableMain.get(i);
            if(i == 0) keys += String.format("coalesce(%s.main,%s.b0) ",schema.key, schema.key);
            else keys += String.format(", coalesce(%s.main,%s.b0) ",schema.key, schema.key);
            if(i == 0) filter += String.format(" %s.b0 is not null ", schema.key);
            else filter += String.format(" and %s.b0 is not null ", schema.key);
        }
        String sql = String.format
                ("select %s from %s where %s; ",
                        keys,
                        constants.IoTDB_DATABASE, filter);
        session.executeQueryStatement(sql);
    }

    @Override
    public void execute() throws Exception {
        long rat = System.currentTimeMillis()%10000;
        String fres = constants.RES_PREFIX + "IoTDBV-" + rat + constants.dataset + constants.RES_POSTFIX;
        String flog = constants.LOG_PREFIX + "IoTDBV-" + rat + constants.dataset + constants.LOG_POSTFIX;
        FileWriter wres = new FileWriter(new File(fres));
        wlog = new FileWriter(new File(flog));
        System.out.println("Saved as " + fres);
        for(BenchFunctions bf: constants.BENCHMARK_CODE) {
            wlog.write(bf.name() + " ");
            int ti = 5;
            if(bf == BenchFunctions.VALUE_JOIN) ti = 2;
            long cost = 0;
            for(int i=0;i<ti;i++) {
                try {
                    this.clean();
                    this.insertDataPrepare(this.constants.path_climate, 100000, 0.10, 0, 0, 1);
                } catch (Exception e) {
                    System.err.println(e);
                    //System.exit(-1);
                }
                long now = System.currentTimeMillis();
                switch (bf) {
                    case ALIGN: align(constants.descriptor.mainName); break;
                    case VALUE_FILTER: valueFilter(constants.descriptor.mainName, constants.SELECTIVITY); break;
                    case RANGE_FILTER: rangeFilter(constants.descriptor.mainName, constants.SELECTIVITY); break;
                    case DOWNSAMPLE: downSample(constants.descriptor.mainName, constants.WINDOW); break;
                    case DOWNSAMPLE_ALIGN: downSampleAligned(constants.descriptor.mainName, constants.WINDOW); break;
                    case DOWNSAMPLE_UNION: downSampleUnion(constants.descriptor.mainName, constants.WINDOW); break;
                    case VALUE_JOIN: seriesJoinValue(constants.descriptor.mainName); break;
                    case BRANCH_ALIGN: branchAlign(constants.descriptor.mainName); break;
                    case UPDATE: update(constants.descriptor.mainName); break;
                }
                cost += (System.currentTimeMillis() - now);
                //clean
                switch (bf) {
                    case ALIGN:
                    case VALUE_FILTER:
                    case RANGE_FILTER: break;
                    case DOWNSAMPLE:
                    case DOWNSAMPLE_ALIGN:
                    case DOWNSAMPLE_UNION: clean_temp(); break;
                    case VALUE_JOIN:
                    case BRANCH_ALIGN: break;
                    case UPDATE: break;
                    default:
                }
            }
            cost /= ti;
            System.out.println(cost);
            wres.write(String.valueOf(cost) + " ");
        }
        wlog.close();
        wres.close();
    }

    @Override
    public void execute(BenchFunctions benchFunctions) throws Exception {

    }

    public static void benchmarking() throws Exception {
        IoTDBV ioTDB = new IoTDBV();
        ioTDB.init(1, "Climate", "IoTDBV");
//        ioTDB.registerCoalesce(); // execute once.
        ioTDB.execute();
//        ioTDB.clean();
    }

    public static void main(String[] args) throws Exception {
        benchmarking();
    }

}
