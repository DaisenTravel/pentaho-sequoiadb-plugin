package org.pentaho.di.trans.steps.sequoiadboutput;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;

public class SequoiaDBOutputRecordInfo {
   
   private static Class<?> PKG = SequoiaDBOutputMeta.class;

   private List<SequoiaDBOutputField> m_fields = new ArrayList<SequoiaDBOutputField>() ;
   
   private Map<String, SequoiaDBOutputFieldInfo> m_fieldsInfo
                                       = new TreeMap<String, SequoiaDBOutputFieldInfo>() ;
   
   public void addField( String srcFieldName, String fieldPath ) throws KettleValueException {
      if ( null == fieldPath || fieldPath.isEmpty() ){
         fieldPath = srcFieldName ;
      }
      int numFields = m_fields.size() ;
      boolean isMerged = false ;
      for ( int i = 0 ; i < numFields ; i++ ){
         if ( m_fields.get(i).isNeedMerge( fieldPath ) ){
            m_fields.get(i).addField( srcFieldName, fieldPath ) ;
            isMerged = true ;
            break ;
         }
      }
      if ( !isMerged ){
         SequoiaDBOutputField newField = new SequoiaDBOutputField() ;
         newField.init( srcFieldName, fieldPath ) ;
         m_fields.add( newField ) ;
      }
      
      SequoiaDBOutputFieldInfo fieldInfo = m_fieldsInfo.get( srcFieldName ) ;
      if( null == fieldInfo ){
         fieldInfo = new  SequoiaDBOutputFieldInfo() ;
         fieldInfo.addFieldPath( fieldPath ) ;
         m_fieldsInfo.put( srcFieldName, fieldInfo ) ;
      }
      else{
         fieldInfo.addFieldPath( fieldPath );
      }
   }
   
   public void done() throws KettleValueException {

      int numFields = m_fields.size() ;
      for ( int i = 0 ; i < numFields ; i++ ){
         m_fields.get(i).done() ;
      }
   }
   
   public BSONObject toBson( Object[] row, RowMetaInterface rowMeta ) throws KettleValueException {
      if ( null == m_fieldsInfo || m_fieldsInfo.size() == 0 ){
         return null ;
      }
      for( Map.Entry<String, SequoiaDBOutputFieldInfo> entry : m_fieldsInfo.entrySet() ){
         int index = rowMeta.indexOfValue( entry.getKey() ) ;
         ValueMetaInterface vmi = rowMeta.getValueMeta( index ) ;
         try{
            entry.getValue().setVal( row[index], vmi ) ;
         }
         catch( KettleValueException e ){
            throw new KettleValueException( BaseMessages.getString( PKG,
                  "SequoiaDBOutput.Msg.Err.FailedToGetTheFieldVal"
                  + "(" + entry.getKey() + ":" + row[index].toString() + ")" ) );
         }
      }
      
      BSONObject result = new BasicBSONObject() ;
      boolean hasField = false ;
      int fieldsNum = m_fields.size() ;
      for( int i = 0 ; i < fieldsNum ; i++ ){
         Object tmpObj = m_fields.get(i).toObj( m_fieldsInfo ) ;
         if ( tmpObj != null ){
            result.put( m_fields.get(i).getFieldName(), tmpObj ) ;
            hasField = true ;
         }
      }
      if ( hasField ){
         return result ;
      }
      return null ;
   }
   
   public Map<String, SequoiaDBOutputFieldInfo> getFieldsInfo() {
      return m_fieldsInfo ;
   }
}
