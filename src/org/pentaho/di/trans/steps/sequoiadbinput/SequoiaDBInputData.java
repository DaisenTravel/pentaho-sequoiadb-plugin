package org.pentaho.di.trans.steps.sequoiadbinput;

import java.util.List;

import org.bson.BSONObject;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class SequoiaDBInputData extends BaseStepData implements StepDataInterface {

   public RowMetaInterface outputRowMeta;
   
   public Object[] BSONToKettle( BSONObject input, List<SequoiaDBInputField> fields ) throws KettleException{
      Object[] result = null;
      if ( null == fields || fields.size() == 0 ){
         return result;
      }
      result = RowDataUtil.allocateRowData( outputRowMeta.size() );
      for ( SequoiaDBInputField f : fields ){
         Object fieldObj = input.get( f.m_fieldName );
         Object valObj = f.getKettleValue( fieldObj,
                               ValueMeta.getType( f.m_kettleType ));
         result[f.m_outputIndex] = valObj;
      }
      return result;
   }
}