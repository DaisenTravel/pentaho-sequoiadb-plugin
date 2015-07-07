package org.pentaho.di.trans.steps.sequoiadboutput;

import java.util.List;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class SequoiaDBOutputData extends BaseStepData implements StepDataInterface {

   private static Class<?> PKG = SequoiaDBOutputMeta.class;

   protected RowMetaInterface m_outputRowMeta;

   public void setOutputRowMeta( RowMetaInterface outMeta ) {
      m_outputRowMeta = outMeta;
    }
   
   public BSONObject kettleToBson( Object[] row, List<SequoiaDBOutputField> fields ) throws KettleValueException{
      Object valTmp = null ;
      BSONObject result = new BasicBSONObject() ;
      for ( SequoiaDBOutputField f : fields ){
         int index = m_outputRowMeta.indexOfValue( f.m_fieldName ) ;
         ValueMetaInterface vmi = m_outputRowMeta.getValueMeta( index ) ;
         try{
            valTmp = f.getBsonValue( row[index], vmi ) ;
            result.put( f.m_fieldName, valTmp ) ;
         }
         catch( KettleValueException e ){
            throw new KettleValueException( BaseMessages.getString( PKG,
                  "SequoiaDBOutput.Msg.Err.FailedToGetTheFieldVal"
                        + "(" + f.m_fieldName + ":" + row[index].toString() + ")" ) );
         }
      }
      return result ;
   }
   
}