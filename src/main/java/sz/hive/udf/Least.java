/**
 * My first udf
 *
 */

package sz.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class Least extends UDF
{
    public Text evaluate(Text[] arguments) throws Exception
    {
        int numArgs = arguments.length;
        if ( numArgs < 1 ) { return null; }

        Text minValue = new Text("0");
        for (Text arg: arguments) {
            if ( arg != null && minValue.compareTo(arg) > 0 ) {
                minValue = arg;
            }
        }
        return minValue;
    }
}
