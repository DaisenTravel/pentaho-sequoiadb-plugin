package org.pentaho.di.trans.steps.sequoiadboutput;

import java.sql.Timestamp;

import org.bson.types.BSONTimestamp;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;

public class SequoiaDBOutputFieldInfo{

   private static Class<?> PKG = SequoiaDBOutputMeta.class;
   
   private String m_name = "" ;
   
   private String m_path = "" ;
   
   private String m_updateOp = "$set" ;
   
   private boolean m_cond = false ;
   
   public SequoiaDBOutputFieldInfo( String name, String path) {
      m_name = name ;
      if( null == path || path.isEmpty() ) {
         m_path = m_name ;
      }
      else {
         m_path = path ;
      }
   }
   
   public String getName() {
      return m_name ;
   }
   
   public String getPath() {
      return m_path ;
   }
   
   public void setUpdateOp(String updateOp) {
      m_updateOp = updateOp ;
   }
   
   public String getUpdateOp() {
      return m_updateOp ;
   }
   
   public void setCond(boolean cond) {
      m_cond = cond ;
   }
   
   public boolean getCond() {
      return m_cond ;
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