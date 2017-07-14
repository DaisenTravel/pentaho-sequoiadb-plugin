package org.pentaho.di.trans.steps.sequoiadboutput;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.bson.types.BSONTimestamp;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;

public class SequoiaDBInsertFieldInfo extends SequoiaDBOutputFieldInfo{
   
   public SequoiaDBInsertFieldInfo(String name, String path) {
      super(name, path);
   }

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
}
