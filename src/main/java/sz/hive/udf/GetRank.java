/**
 * 
 *
 */

package sz.hive.udf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


import java.util.List;
import java.util.Arrays;
import java.util.Collections;


/**
 * Hive UDF to extract the value of an element based on the value rank in the list
 *
 */
@Description(
    name = "Return the value of an array based on the rank requested"
    , value = "ExtractPrice(array, 1) returns value with rank = 1 etc ..."
)

public class GetRank extends GenericUDF {
    private ListObjectInspector listInspector;
    private PrimitiveObjectInspector intInspector;


    public double extract(List<Object> strArray, int index) {

        int lenArray = strArray.size();
        double[] numArray= new double[lenArray];
        int i= 0;

        for ( Object obj: strArray ) {

            Object dblObj = ( ( PrimitiveObjectInspector ) ( listInspector.getListElementObjectInspector() ) ).getPrimitiveJavaObject(obj);
            if ( dblObj == null ) { numArray[i] = 0.0; } else {
                if ( dblObj instanceof Number ) {
                    Number dblNum = (Number) dblObj;
                    numArray[i] = dblNum.doubleValue();
                } else {
                    String dblStr = ( dblObj.toString() );
                    try {
                        Double dblCoerce = Double.parseDouble( dblStr );
                        numArray[i] = dblCoerce;
                    } catch ( NumberFormatException formatExc ) {
                        // TODO: log this
                    }
                }
                i += 1;
            }
        }

        // Now sort numArray
        Arrays.sort(numArray);

        // Start from the last element because sort by default returns ascending order
        // TODO: improve this by sorting in descending order
        int returnIndex = numArray.length - index;
        if ( returnIndex >= 0 ) {
            return numArray[ returnIndex ];
        } else { return 0.0; }
    }

    @Override
    public Object evaluate( DeferredObject[] arg ) throws HiveException {
        List argList = listInspector.getList( arg[0].get() );
        Object iin = arg[1].get();
        int index = (Integer) intInspector.getPrimitiveJavaObject( iin );

        if ( argList != null ) {
            return extract( argList, index );
        } else { return null; }
    }

    @Override
    public String getDisplayString( String[] arg ) {
        return "evaluate(array, rank)";
    }

    @Override
    public ObjectInspector initialize( ObjectInspector[] arg ) throws UDFArgumentException {
        this.listInspector = (ListObjectInspector) arg[0];
        this.intInspector = (PrimitiveObjectInspector) arg[1];

        ObjectInspector returnType = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;

        return returnType;
    }

}
