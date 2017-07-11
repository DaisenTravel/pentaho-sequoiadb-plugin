package org.pentaho.di.trans.steps.sequoiadboutput;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.bson.types.BSONTimestamp;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;

public class SequoiaDBOutputFieldInfo {
   
   private static Class<?> PKG = SequoiaDBOutputMeta.class;
   
   private List<String> m_fieldPath = new ArrayList<String>();
   
   private Object m_value ;
   
   public List<String> getFieldPath() {
      return m_fieldPath ;
   }
   
   public void addFieldPath( String fieldPath ) {
      m_fieldPath.add( fieldPath ) ;
   }
   
   public void setVal( Object input, ValueMetaInterface vmi ) throws KettleValueException {
      m_value = getBsonValue( input, vmi ) ;
   }
   
   public Object getVal(){
      return m_value ;
   }

   public Object getBsonValue( Object input, ValueMetaInterface vmi ) throws KettleValueException {
      if ( vmi.isNull( input ) ) {
         return null ;
      }
      Object retObj ;
      if ( vmi.isString() ) {
         retObj = vmi.getString( input ) ;
         return retObj ;
      }
      if ( vmi.isBoolean() ) {
         retObj = vmi.getBoolean( input );
         return retObj ;
      }
      if ( vmi.isInteger() ) {
         retObj = vmi.getInteger( input ) ;
         return retObj ;
      }
      if ( vmi.isDate() ) {
         if( input instanceof Timestamp ) {
            retObj = new BSONTimestamp((int) (((Timestamp)input).getTime()/1000),
                                       ((Timestamp)input).getNanos()/1000);
         }else {
            retObj = vmi.getDate( input ) ;
         }
         return retObj ;
      }
      if ( vmi.isNumber() ) {
         retObj = vmi.getNumber( input ) ;
         return retObj ;
      }
      if ( vmi.isBigNumber() ) {
         retObj = vmi.getBigNumber( input ) ;
         return retObj ;
      }
      if ( vmi.isBinary() ) {
         retObj = vmi.getBinary( input ) ;
         return retObj ;
      }

      throw new KettleValueException( BaseMessages.getString( PKG,
            "SequoiaDBOutput.Msg.Err.FailedToGetTheFieldVal" ) );
   }
}
