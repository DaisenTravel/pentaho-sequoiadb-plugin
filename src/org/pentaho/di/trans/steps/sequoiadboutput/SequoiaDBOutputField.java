package org.pentaho.di.trans.steps.sequoiadboutput;

import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;

public class SequoiaDBOutputField{

   private static Class<?> PKG = SequoiaDBOutputMeta.class;

   public String m_fieldName = "" ;

   public String m_path = "" ;

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
         retObj = vmi.getDate( input ) ;
         return retObj ;
      }
      if ( vmi.isNumber() ) {
         retObj = vmi.getNumber( input ) ;
         return retObj ;
      }
      if ( vmi.isBigNumber() ) {
         retObj = vmi.getString( input ) ;
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