import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: abozorgk
 * Date: 3/4/13
 * Time: 5:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class Config2
{
    Properties configFile;

    public Config2()
    {
        configFile = new java.util.Properties();
        try {
            Reader reader = new BufferedReader(new FileReader("src/SubsetConfig.cfg"));
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
