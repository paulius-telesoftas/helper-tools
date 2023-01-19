import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ConversionTest {

    public static final String JSON_DATA = "json_data";
    public static final String PARQUET_DATA = "parquet_data";

    @Test
    public void toJsonTest() throws IOException {
        Files.deleteIfExists(Paths.get(JSON_DATA));
        SparkSession session = SparkSession
                .builder()
                .config("spark.master", "local")
                .appName(ConversionTest.class.getSimpleName()).getOrCreate();
        Dataset<Row> df = session.read().parquet("data.parquet");
        Files.writeString(Paths.get("schema.json"), df.schema().prettyJson());
        df.write().json(JSON_DATA);
    }

    @Test
    public void toParquetTest() throws IOException {
//        Files.deleteIfExists(Paths.get(PARQUET_DATA));
        SparkSession session = SparkSession
                .builder()
                .config("spark.master", "local")
                .appName(ConversionTest.class.getSimpleName()).getOrCreate();

        StructType schemaFromJson = (StructType) DataType.fromJson(new String(Files.readAllBytes(Paths.get("schema.json"))));
        Dataset<Row> df = session.read().schema(schemaFromJson).option("multiline", true).json("data.json");
        df.printSchema();
        df.write().parquet(PARQUET_DATA);
    }
}
