import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.util.Properties;

public class Config
{
    Properties configFile;

    public Config()
    {
        configFile = new java.util.Properties();
        try {
            Reader reader = new BufferedReader(new FileReader("src/config.cfg"));
            configFile.load(reader);
        }catch(Exception eta){
            eta.printStackTrace();
        }
    }

    public String getProperty(String key)
    {
        return this.configFile.getProperty(key);
    }
}
