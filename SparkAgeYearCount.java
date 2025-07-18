import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.*;

public class SparkAgeYearCount {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: SparkAgeYearCount <inputPath> <outputPath>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Create a Spark context
        JavaSparkContext sc = new JavaSparkContext("local", "SparkAgeYearCount");

        // Load the input data
        JavaRDD<String> lines = sc.textFile(inputPath);

        // Transformations to extract age and year and perform counting
        JavaPairRDD<String, Integer> ageYearCount = lines
                .filter(line -> line.split(",").length >= 20)
                .flatMapToPair((String line) -> {
                    String[] columns = line.split(",");
                    String age = columns[20].trim();
                    String year = columns[0].trim();
                    return Arrays.asList(new Tuple2<>(age + "\t" + year, 1)).iterator();
                })
                .reduceByKey((a, b) -> a + b);

        // Save the result to the output path
        ageYearCount.saveAsTextFile(outputPath);

        // Stop the Spark context
        sc.close();
    }
}
