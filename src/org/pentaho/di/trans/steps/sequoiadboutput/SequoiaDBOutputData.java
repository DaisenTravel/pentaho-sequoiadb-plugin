package org.pentaho.di.trans.steps.sequoiadboutput;

import java.util.ArrayList;
import java.util.Iterator;
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
            Iterator<String> it = f.m_pathField.iterator();
            BSONObject subObj = null;
            // skip first path
            String fieldName = it.next();
            if ( it.hasNext() ) {
            	String str = it.next();
            	subObj = makeBSONObject(it, str, result.get(fieldName), valTmp);
            }

            if (null != subObj) {
            	result.put(fieldName, subObj);
            }
            else {
            	result.put( fieldName, valTmp ) ;
            }
         }
         catch( KettleValueException e ){
            throw new KettleValueException( BaseMessages.getString( PKG,
                  "SequoiaDBOutput.Msg.Err.FailedToGetTheFieldVal"
                  + "(" + f.m_fieldName + ":" + row[index].toString() + ")" ) );
         }
      }
      return result ;
   }
   
   public BSONObject makeBSONObject( Iterator<String> it, String key,
		                             Object o, Object value ) throws KettleValueException {
	   
	   BSONObject obj = null;
	   if (null != o) {
		   if (o instanceof BSONObject) {
			   obj = (BSONObject)o;
			   if ( it.hasNext() ) {
				   String str = it.next();
				   obj = makeBSONObject(it, str, obj.get(key), value);
			   }
			   else {
				   obj.put(key, value);
			   }
		   }
		   else {
			   throw new KettleValueException( BaseMessages.getString( PKG,
                       "SequoiaDBOutput.Msg.Err.FailedToWriteTheFieldVal"
                       + "(" + key + ":" + o.toString() + ")"
                       + ", the field and the value existed" ) );
		   }
	   }
	   else {
		   if ( it.hasNext() ) {
			   String str = it.next();
			   obj = makeBSONObject( it, str, null, value );
		   }
		   else {
			   obj = new BasicBSONObject();
			   obj.put(key, value);
		   }
	   }

	   return obj;
   }
}