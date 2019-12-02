package org.yyff.esspark.wordcount;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    private Properties properties;

    public Config(String fileName) throws IOException {
        InputStream input = null;
        Properties prop = null;
        try {
            input = getClass().getClassLoader().getResourceAsStream(fileName);
            prop = new Properties();
            prop.load(input);
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            input.close();
        }
        this.properties = prop;
    }

    public Properties getProperties() {
        return properties;
    }
}
