import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class PostgresV extends Postgres {

    @Override
    public void init(int num_branch, String dataset, String ev) throws SQLException {
        String url = "jdbc:postgresql://localhost:8080/dbv?user=postgres&password=postgres";
        db = DriverManager.getConnection(url);
        constants = new Constants(num_branch, dataset, ev);
    }

    @Override
    public void align(Schema tableMain1, Schema tableMain2) throws SQLException {
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
        sb.append("select l.t, coalesce(r.%s, l.%s) from ")
                .append("((select m.time t, coalesce(s.%s, m.%s) A, m.%s B from (( %s natural join %s ) m left outer join %s s on m.time=s.time)) l")
                .append(" left outer join %s r on l.t=r.time);");
        String sql = String.format(sb.toString(),
                tableMain2.attrName.get(0), tableMain2.attrName.get(0),
                tableMain1.attrName.get(0), tableMain1.attrName.get(0), tableMain2.attrName.get(0),
                tableMain1.main, tableMain2.main,
                constants.descriptor.branches.get(tableMain1.main).get(0),
                constants.descriptor.branches.get(tableMain2.main).get(0));
        System.out.println(sql);
        st.executeQuery(sql);
    }

    @Override
    public void align(List<Schema> tableMain) throws Exception {
        for(int i=0;i<tableMain.size()-1;i++) {
            this.align(tableMain.get(i), tableMain.get(i+1));
        }
    }

    @Override
    public void rangeFilter(Schema tableMain, double selectivity) throws SQLException {
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
        Range timeRange = Range.randomBySelectivity(constants.timeRange.get(tableMain.main), selectivity);
        sb.append("select m.time time, coalesce(u.%s, m.%s) A")
                .append(" from (select * from %s where time > %s and time < %s) m ")
                .append(" left outer join %s u on m.time=u.time;");
        String sql = String.format(sb.toString(),
                tableMain.attrName.get(0), tableMain.attrName.get(0), tableMain.main,
                timeRange.left, timeRange.right,
                constants.descriptor.branches.get(tableMain.main).get(0));
        System.out.println(sql);
        ResultSet rs = st.executeQuery(sql);
    }

    @Override
    public void rangeFilter(List<Schema> tableMain, double selectivity) throws Exception {
        for(int i=0;i<tableMain.size();i++) {
            this.rangeFilter(tableMain.get(i), selectivity);
        }
    }

    @Override
    public void valueFilter(Schema tableMain, double selectivity) throws SQLException {
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
        for(String x: tableMain.attrName) {
            Range valueRange = Range.randomBySelectivity(constants.valueRange.get(x), selectivity);
            sb.append("select * from ( select x.time, coalesce(y.%s, x.%s) A from " +
                            "(%s x left join (select * from %s where %s > %s and %s < %s) y on x.time=y.time) ) ")
                    .append("where A > %s and A < %s;");
            String sql = String.format(sb.toString(),
                    tableMain.attrName.get(0), tableMain.attrName.get(0),
                    tableMain.main,
                    constants.descriptor.branches.get(tableMain.main).get(0),
                    tableMain.attrName.get(0), valueRange.left,
                    tableMain.attrName.get(0), valueRange.right,
                    valueRange.left, valueRange.right);
            System.out.println(sql);
            st.executeQuery(sql);
        }
    }

    @Override
    public void valueFilter(List<Schema> tableMain, double selectivity) throws Exception {
        for(int i=0;i<tableMain.size();i++) {
            this.valueFilter(tableMain.get(i), selectivity);
        }
    }

    @Override
    public void downSample(Schema tableMain, int window) throws SQLException {
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
        //st.executeQuery("select m.time time, ceil(m.time/5) grp, coalesce(u.A, m.A) A from climate1 m full outer join climate1b0 u on m.time=u.time;");
        sb.append("with R1(G, A) as (select ceil(time/%s), %s from %s),")
                .append("R2(G, A, B) as (select ceil(time/%s), m.%s, n.%s from %s m natural join %s n),")
                .append("AGG1(G, A) as (select G, avg(A) from R1 group by G),")
                .append("AGG2(G, A1) as (select G, avg(A)-avg(B) from R2 group by G)")
                .append("select G, A+A1 from AGG1 natural join AGG2;");
        String sql = String.format(sb.toString(),
                window, tableMain.attrName.get(0), tableMain.main,
                window, tableMain.attrName.get(0), tableMain.attrName.get(0),
                constants.descriptor.branches.get(tableMain.main).get(0), tableMain.main);
        System.out.println(sql);
        st.executeQuery(sql);
    }

    @Override
    public void downSample(List<Schema> tableMain, int window) throws Exception {
        for(Schema schema: tableMain) {
            this.downSample(schema, window);
        }
    }

    @Override
    public void downSampleAligned(Schema tableMain1, Schema tableMain2, int window) throws SQLException {
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
        sb.append("with R1(G, A) as (select ceil(time/%s), %s from %s),")
                .append("R11(G, A, B) as (select ceil(time/%s), m.%s, n.%s from %s m natural join %s n),")
                .append("R2(G, A) as (select ceil(time/%s), %s from %s),")
                .append("R22(G, A, B) as (select ceil(time/%s), m.%s, n.%s from %s m natural join %s n),")
                .append("AGG1(G, A) as (select G, avg(A) from R1 group by G),")
                .append("AGG11(G, A1) as (select G, avg(A)-avg(B) from R11 group by G),")
                .append("AGG2(G, X) as (select G, avg(A) from R2 group by G),")
                .append("AGG22(G, X1) as (select G, avg(A)-avg(B) from R22 group by G)")
                .append("select G, A+A1, X+X1 from AGG1 natural join AGG11 natural join AGG2 natural join AGG22;");
        String sql = String.format(sb.toString(),
                window, tableMain1.attrName.get(0), tableMain1.main,
                window, tableMain1.attrName.get(0), tableMain1.attrName.get(0),
                constants.descriptor.branches.get(tableMain1.main).get(0), tableMain1.main,
                window, tableMain2.attrName.get(0), tableMain2.main,
                window, tableMain2.attrName.get(0), tableMain2.attrName.get(0),
                constants.descriptor.branches.get(tableMain2.main).get(0), tableMain2.main);
        System.out.println(sql);
        st.executeQuery(sql);
    }

    @Override
    public void downSampleAligned(List<Schema> tableMain, int window) throws SQLException {
        for(int i=0;i<tableMain.size()-1;i++) {
            this.downSampleAligned(tableMain.get(i), tableMain.get(i+1), window);
        }
    }

    @Override
    public void downSampleUnion(Schema tableMain1, Schema tableMain2, int window) throws SQLException {
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
        sb.append("with R1(G, A) as (select ceil(time/%s), %s from %s),")
                .append("R11(G, A, B) as (select ceil(time/%s), m.%s, n.%s from %s m natural join %s n),")
                .append("R2(G, A) as (select ceil(time/%s), %s from %s),")
                .append("R22(G, A, B) as (select ceil(time/%s), m.%s, n.%s from %s m natural join %s n),")
                .append("AGG1(G, A) as (select G, avg(A) from R1 group by G),")
                .append("AGG11(G, A1) as (select G, avg(A)-avg(B) from R11 group by G),")
                .append("AGG2(G, X) as (select G, avg(A) from R2 group by G),")
                .append("AGG22(G, X1) as (select G, avg(A)-avg(B) from R22 group by G)")
                .append("select G, A+A1+X+X1 from AGG1 natural join AGG11 natural join AGG2 natural join AGG22;");
        String sql = String.format(sb.toString(),
                window, tableMain1.attrName.get(0), tableMain1.main,
                window, tableMain1.attrName.get(0), tableMain1.attrName.get(0),
                constants.descriptor.branches.get(tableMain1.main).get(0), tableMain1.main,
                window, tableMain2.attrName.get(0), tableMain2.main,
                window, tableMain2.attrName.get(0), tableMain2.attrName.get(0),
                constants.descriptor.branches.get(tableMain2.main).get(0), tableMain2.main);
        //System.out.println(sql);
        st.executeQuery(sql);
    }

    @Override
    public void downSampleUnion(List<Schema> tableMain, int window) throws SQLException {
        for(int i=0;i<tableMain.size()-1;i++) {
            this.downSampleUnion(tableMain.get(i), tableMain.get(i+1), window);
        }
    }

    @Override
    public void seriesJoinValue(Schema tableMain1, Schema tableMain2) throws SQLException {
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
//        sb.append("with UPD(time1, time2, ATTR1, ATTR2) as (select * from %s m, %s n where ceil(m.%s)=ceil(n.%s))")
//                .append("select * from (select l.time t1, coalesce(UPD.ATTR1, l.%s) A from %s l left join UPD on UPD.time1=l.time) s " +
//                        ", (select l.time t2, coalesce(UPD.ATTR2, l.%s) B from %s l left join UPD on UPD.time2=l.time) p")
//                .append(" where ceil(s.A) = ceil(p.B);");
//        String sql = String.format(sb.toString(),
//                constants.descriptor.branches.get(tableMain1.main).get(0),
//                constants.descriptor.branches.get(tableMain2.main).get(0),
//                tableMain1.attrName.get(0), tableMain2.attrName.get(0),
//                tableMain1.attrName.get(0), tableMain1.main,
//                tableMain2.attrName.get(0), tableMain2.main);
                //System.out.println(sql);
        sb.append("with R1 as ")
                .append("(select m.time time, coalesce(u.%s, m.%s) A " +
                        "from %s m full outer join %s u on m.time=u.time), R2 as ")
                .append("(select x.time time, coalesce(y.%s, x.%s) A " +
                        "from %s x full outer join %s y on x.time=y.time) ")
                .append("select R1.time, R2.time, R1.A, R2.A from R1, R2 where ceil(R1.A)=ceil(R2.A);");
        String sql = String.format(sb.toString(),
                tableMain1.attrName.get(0), tableMain1.attrName.get(0),
                tableMain1.main,
                constants.descriptor.branches.get(tableMain1.main).get(0),
                tableMain2.attrName.get(0), tableMain2.attrName.get(0),
                tableMain2.main,
                constants.descriptor.branches.get(tableMain2.main).get(0));
        //System.out.println(sql);
        st.executeQuery(sql);
    }

    @Override
    public void seriesJoinValue(List<Schema> tableMain) throws SQLException {
        for(int i=0;i<tableMain.size()-1;i++) {
            this.seriesJoinValue(tableMain.get(i), tableMain.get(i+1));
        }
    }

    @Override
    public void branchAlign(Schema tableMain1, Schema tableMain2) throws SQLException {
        // use batch
    }

    @Override
    public void branchAlign(List<Schema> tableMain) throws SQLException {
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
        sb.append("with Aligned (select * from " + tableMain.get(0).main);
        for(int i=1;i<tableMain.size();i++) {
            Schema schema = tableMain.get(i);
            sb.append(" natural join ").append(schema.main);
        }

        sb.deleteCharAt(sb.length()-1); // remove ,
        sb.append("select * from R0");
        for(int i=1;i<tableMain.size();i++) {
            //Schema schema = tableMain.get(i);
            sb.append(" natural join R" + i);
        }
        sb.append(";");
        System.out.println(sb.toString());
        st.executeQuery(sb.toString());
    }

    @Override
    public void execute() throws Exception {
        String fres = constants.RES_PREFIX + "PostgresV-" + System.currentTimeMillis()%10000 + constants.dataset + constants.RES_POSTFIX;
        String flog = constants.LOG_PREFIX + "PostgresV-" + System.currentTimeMillis()%10000 + constants.dataset + constants.LOG_POSTFIX;
        FileWriter wres = new FileWriter(new File(fres));
        wlog = new FileWriter(new File(flog));
        System.out.println("Saved as " + fres);
        for(BenchFunctions bf: constants.BENCHMARK_CODE) {
            wlog.write(bf.name() + " ");
            int ti = 5;
            if(bf == BenchFunctions.VALUE_JOIN) ti = 2;
            long now = System.currentTimeMillis();
            for(int i=0;i<ti;i++) {
                switch (bf) {
                    case ALIGN: align(constants.descriptor.mainName); break;
                    case VALUE_FILTER: valueFilter(constants.descriptor.mainName, constants.SELECTIVITY); break;
                    case RANGE_FILTER: rangeFilter(constants.descriptor.mainName, constants.SELECTIVITY); break;
                    case DOWNSAMPLE: downSample(constants.descriptor.mainName, constants.WINDOW); break;
                    case DOWNSAMPLE_ALIGN: downSampleAligned(constants.descriptor.mainName, constants.WINDOW); break;
                    case DOWNSAMPLE_UNION: downSampleUnion(constants.descriptor.mainName, constants.WINDOW); break;
                    case VALUE_JOIN: seriesJoinValue(constants.descriptor.mainName); break;
                    case BRANCH_ALIGN: branchAlign(constants.descriptor.mainName); break;
                    default:
                }
            }
            long cost = (System.currentTimeMillis() - now)/ti;
            System.out.println(cost);
            wres.write(String.valueOf(cost) + " ");
        }
        wlog.close();
        wres.close();
    }

    public static void benchmarking() throws Exception {
        Postgres pg = new Postgres();
        pg.init(1, "Climate", "PostgresV");
        try {
            pg.clean();
            pg.create();
            pg.insertDataPrepare(pg.constants.path_climate, 100000, 0.10, 0, 0, 1);
//            System.out.println("Generated insert data.");
//            pg.insertCSV();

        } catch (Exception e) {
//            pg.clean();
            System.err.println(e);
            System.exit(-1);
        }
        pg.execute();
    }


    public static void main(String[] args) throws Exception {
        benchmarking();
        //pg.clean();
    }
}
