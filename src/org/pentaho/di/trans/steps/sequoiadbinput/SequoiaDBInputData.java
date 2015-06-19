package org.pentaho.di.trans.steps.sequoiadbinput;

import java.util.List;

import org.bson.BSONObject;
import org.bson.types.Binary;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.sequoiadb.SequoiaDBField;
import org.pentaho.di.trans.steps.sequoiadb.SequoiaDBMeta;

public class SequoiaDBInputData extends BaseStepData implements StepDataInterface {

   public RowMetaInterface outputRowMeta;
   
   public Object[] BSONToKettle( BSONObject input, List<SequoiaDBField> fields ){
      Object[] result = null;
      if ( null == fields || fields.size() == 0 ){
         return result;
      }
      result = RowDataUtil.allocateRowData( outputRowMeta.size() );
      for ( SequoiaDBField f : fields ){
         Object fieldObj = input.get( f.m_fieldName );
         Object valObj = getKettleValue( fieldObj,
                               ValueMeta.getType( f.m_kettleType ));
      }
      return result;
   }
   
   public Object getKettleValue( Object input, int type ){
      Object result = null;
      switch ( type ){
         case ValueMetaInterface.TYPE_INTEGER:
            if ( input instanceof Number ){
               result = new Long( ((Number)input).intValue() );
            }
            else if( input instanceof Binary ){
               byte[] b = ((Binary)input).getData();
               String s = new String( b );
               result = new Integer( s );
            }
            else{
               result = new Integer( input.toString() );
            }
            // TODO:
            // TODO:
            // TODO:
            // TODO:
            // TODO:
      }
      return result;
   }
}