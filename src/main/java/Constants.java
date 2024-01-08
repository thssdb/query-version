import java.util.HashMap;
import java.util.Map;

public class Constants {
    // colid -> max
    public Map<String, Range> valueRange; // colid -> range
    public Map<String, Range> timeRange; // schema.main.name -> range

    public String LOG_POSTFIX = "-log.txt";
    public String LOG_PREFIX = "./data/log/";
    public String RES_POSTFIX = "-res.txt";
    public String RES_PREFIX = "./data/result/";
    public String DATA_PREFIX = "./data/local/";
    public String DATA_SUFFIX = ".csv";
    public String REPAIR_DATA_SUFFIX = "-repair.csv";
    public BenchFunctions[] BENCHMARK_CODE =
            {
                    BenchFunctions.ALIGN,
                    BenchFunctions.VALUE_FILTER,
                    BenchFunctions.RANGE_FILTER,
                    BenchFunctions.DOWNSAMPLE,
                    BenchFunctions.DOWNSAMPLE_ALIGN,
                    BenchFunctions.DOWNSAMPLE_UNION,
                    BenchFunctions.VALUE_JOIN,
                    BenchFunctions.BRANCH_ALIGN,
//                    BenchFunctions.UPDATE
            };
    public double SELECTIVITY = 0.50;
    public int WINDOW = 100;
    public SchemaDescriptor descriptor;
    public String dataset;
    public String path_climate = "E://projects/iotdb/dataset/Climate/climate_sc/iot.climate.csv";
    public String path_ship = "E://projects/iotdb/dataset/Ship/iot.ship.csv";
    public String path_btc = "E://projects/iotdb/dataset/Open.Bitcoin/bitcoin.csv";

    public String IoTDB_DATABASE;
    public String IoTDB_AGG_INTO = "AGG";


    Constants (int branch_num, String dataset, String ev) {
        descriptor = new SchemaDescriptor(branch_num, dataset, ev);
        timeRange = new HashMap<>();
        valueRange = new HashMap<>();
        this.dataset = dataset;
        IoTDB_DATABASE = "root." + dataset;
    }
}
