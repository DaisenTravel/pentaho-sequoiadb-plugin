package org.pentaho.di.trans.steps.sequoiadbinput;

import java.util.List;

import org.bson.BSONObject;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
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
      //TODO: get the value
      return null;
   }
}