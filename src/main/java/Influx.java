import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.List;

public class Influx extends benchmarks {
    @Override
    public void create() throws SQLException {

    }

    @Override
    public void clean() throws SQLException {

    }

    @Override
    public void init(int num_branch, String dataset, String ev) throws SQLException {

    }

    @Override
    public void insert() throws SQLException {

    }

    @Override
    public void insertCSV() throws SQLException {

    }

    @Override
    public void insertDataPrepare(String path, int length, double upd, double delay, double dup, int verbose) throws FileNotFoundException, SQLException {

    }

    @Override
    public void insertDataPrepare_gene(int length, double upd, double delay, double dup, int verbose) throws SQLException {

    }

    @Override
    public void update(List<Schema> main) throws SQLException {

    }

    @Override
    public void update(Schema main) throws SQLException {

    }

    @Override
    public void align(Schema tableMain1, Schema tableMain2) throws SQLException {

    }

    @Override
    public void align(List<Schema> tableMain) throws Exception {

    }

    @Override
    public void rangeFilter(Schema tableMain, double selectivity) throws SQLException {

    }

    @Override
    public void rangeFilter(List<Schema> tableMain, double selectivity) throws Exception {

    }

    @Override
    public void valueFilter(Schema tableMain, double selectivity) throws SQLException {

    }

    @Override
    public void valueFilter(List<Schema> tableMain, double selectivity) throws Exception {

    }

    @Override
    public void downSample(Schema tableMain, int window) throws SQLException {

    }

    @Override
    public void downSample(List<Schema> tableMain, int window) throws Exception {

    }

    @Override
    public void downSampleAligned(Schema tableMain1, Schema tableMain2, int window) throws SQLException {

    }

    @Override
    public void downSampleAligned(List<Schema> tableMain, int window) throws SQLException {

    }

    @Override
    public void downSampleUnion(Schema tableMain1, Schema tableMain2, int window) throws SQLException {

    }

    @Override
    public void downSampleUnion(List<Schema> tableMain, int window) throws SQLException {

    }

    @Override
    public void seriesJoinValue(Schema tableMain1, Schema tableMain2) throws SQLException {

    }

    @Override
    public void seriesJoinValue(List<Schema> tableMain) throws SQLException {

    }

    @Override
    public void branchAlign(Schema tableMain1, Schema tableMain2) throws SQLException {

    }

    @Override
    public void branchAlign(List<Schema> tableMain) throws SQLException {

    }

    @Override
    public void execute() throws Exception {

    }

    @Override
    public void execute(BenchFunctions benchFunctions) throws Exception {

    }
}
