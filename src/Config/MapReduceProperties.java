package Config;

import Constants.MRConstant;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MapReduceProperties {

    public Properties getProperties() throws IOException {
        InputStream inputStream = new FileInputStream(MRConstant.PROPERTY_FILE);
        Properties prop = new Properties();
        prop.load(inputStream);
        return prop;
    }
}
