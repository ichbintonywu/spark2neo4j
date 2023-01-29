import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimplSparkNeo4j {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark SQL Example")
                .config("spark.master", "local")
                .getOrCreate();
        // Reading nodes from Neo4j
        Dataset<Row> ds = spark.read().format("org.neo4j.spark.DataSource")
                .option("url", "bolt://localhost:11003")
                .option("authentication.basic.username", "neo4j")
                .option("authentication.basic.password", "Ne04j!")
                .option("database", "neo4j")
                .option("labels", "Person")
                .load();
        ds.show(20);
    }
}
