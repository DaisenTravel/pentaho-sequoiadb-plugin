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

public class SequoiaDBInsertRecordInfo {
   
   private static Class<?> PKG = SequoiaDBOutputMeta.class;

   private List<SequoiaDBInsertField> m_fields = new ArrayList<SequoiaDBInsertField>() ;
   
   private Map<String, SequoiaDBInsertFieldInfo> m_fieldsInfo
                                       = new TreeMap<String, SequoiaDBInsertFieldInfo>() ;
   
   public void addField( SequoiaDBOutputFieldInfo fieldInfo ) throws KettleValueException {
      int numFields = m_fields.size() ;
      boolean isMerged = false ;
      for ( int i = 0 ; i < numFields ; i++ ){
         if ( m_fields.get(i).isNeedMerge( fieldInfo.getPath() ) ){
            m_fields.get(i).addField( fieldInfo.getName(), fieldInfo.getPath() ) ;
            isMerged = true ;
            break ;
         }
      }
      if ( !isMerged ){
         SequoiaDBInsertField newField = new SequoiaDBInsertField() ;
         newField.init( fieldInfo.getName(), fieldInfo.getPath() ) ;
         m_fields.add( newField ) ;
      }
      
      SequoiaDBInsertFieldInfo insertFieldInfo = m_fieldsInfo.get( fieldInfo.getName() ) ;
      if( null == insertFieldInfo ){
         insertFieldInfo = new  SequoiaDBInsertFieldInfo(fieldInfo.getName(), null) ;
         insertFieldInfo.addFieldPath( fieldInfo.getPath() ) ;
         m_fieldsInfo.put( fieldInfo.getName(), insertFieldInfo ) ;
      }
      else{
         insertFieldInfo.addFieldPath( fieldInfo.getPath() );
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
      for( Map.Entry<String, SequoiaDBInsertFieldInfo> entry : m_fieldsInfo.entrySet() ){
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
   
   public Map<String, SequoiaDBInsertFieldInfo> getFieldsInfo() {
      return m_fieldsInfo ;
   }
}
