import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class ReadAvroWriteToBigtableLocally {

  public static void main(String[] args) throws IOException {
    // Set schema and file
    File file = new File(
        "src/temp_BigQueryExtractTemp_0b4295d2c4cc448b99b47c56ff1768eb_000000000000.avro");

    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);

    Schema schema = dataFileReader.getSchema();
    datumReader.setSchema(schema);

    GenericRecord user = null;
    while (dataFileReader.hasNext()) {
      // Reuse user object by passing it to next(). This saves us from
      // allocating and garbage collecting many objects for files with
      // many items.
      user = dataFileReader.next(user);
      System.out.println(user);
    }
  }
}
