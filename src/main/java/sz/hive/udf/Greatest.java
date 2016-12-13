/**
 * TODO
 *
 */

package sz.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class Greatest extends UDF
{
    public Text evaluate(Text[] arguments) throws Exception
    {
        int numArgs = arguments.length;
        if ( numArgs < 2 ) { return null; }

        Text maxValue = new Text("0");
        for (Text arg: arguments) {
            if ( arg != null && maxValue.compareTo(arg) < 0 ) {
                maxValue = arg;
            }
        }
        return maxValue;
    }
}
