package org.pentaho.di.trans.steps.sequoiadboutput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;

public class SequoiaDBUpdateRecordInfo {

   private static Class<?> PKG = SequoiaDBOutputMeta.class;

   private Map<String, List<SequoiaDBUpdateFieldInfo>> m_updateFields
                           = new HashMap<String, List<SequoiaDBUpdateFieldInfo>>();
   
   private List<SequoiaDBUpdateFieldInfo> m_condFields = new ArrayList<SequoiaDBUpdateFieldInfo>();
   
   public void addField(SequoiaDBOutputFieldInfo fieldInfo) {
      SequoiaDBUpdateFieldInfo fieldTmp = new SequoiaDBUpdateFieldInfo( fieldInfo.getName(),
                                                                fieldInfo.getPath());
      fieldTmp.setCond(fieldInfo.getCond());
      fieldTmp.setUpdateOp(fieldInfo.getUpdateOp());
      List<SequoiaDBUpdateFieldInfo> opFieldsTmp
                              = m_updateFields.get(fieldTmp.getUpdateOp()) ;
      if ( null == opFieldsTmp ) {
         opFieldsTmp = new ArrayList<SequoiaDBUpdateFieldInfo>();
         m_updateFields.put(fieldTmp.getUpdateOp(), opFieldsTmp ) ;
      }
      opFieldsTmp.add( fieldTmp ) ;
      if(fieldTmp.getCond()) {
         m_condFields.add( fieldTmp ) ;
      }
   }
   
   public void done() {
      
   }
   
   public BSONObject getUpdater( Object[] row, RowMetaInterface rowMeta ) throws KettleValueException {
      BSONObject updater = new BasicBSONObject() ;
      for(Map.Entry<String, List<SequoiaDBUpdateFieldInfo>> entry:m_updateFields.entrySet()) {
         BSONObject fieldsObj = new BasicBSONObject() ;
         int fieldNum = entry.getValue().size() ;
         for( int i = 0 ; i < fieldNum ; i++ ) {
            SequoiaDBUpdateFieldInfo fieldTmp = entry.getValue().get(i) ;
            int index = rowMeta.indexOfValue( fieldTmp.getName() ) ;
            ValueMetaInterface vmi = rowMeta.getValueMeta( index ) ;
            try{
               fieldsObj.put(fieldTmp.getPath(), fieldTmp.getBsonValue(row[index], vmi)) ;
            }
            catch( KettleValueException e ){
               throw new KettleValueException( BaseMessages.getString( PKG,
                     "SequoiaDBOutput.Msg.Err.FailedToGetTheFieldVal"
                     + "(" + entry.getKey() + ":" + row[index].toString() + ")" ) );
            }
         }
         updater.put( entry.getKey(), fieldsObj ) ;
      }
      if ( updater.isEmpty()) {
         return null ;
      }
      return updater ;
   }
   
   public BSONObject getUpdateCond( Object[] row, RowMetaInterface rowMeta ) throws KettleValueException {
      BSONObject condition = new BasicBSONObject() ;
      int fieldNum = m_condFields.size() ;
      for( int i = 0 ; i < fieldNum ; i++ ) {
         SequoiaDBUpdateFieldInfo fieldTmp = m_condFields.get(i) ;
         int index = rowMeta.indexOfValue( fieldTmp.getName() ) ;
         ValueMetaInterface vmi = rowMeta.getValueMeta( index ) ;
         try{
            condition.put(fieldTmp.getPath(), fieldTmp.getBsonValue(row[index], vmi)) ;
         }
         catch( KettleValueException e ){
            throw new KettleValueException( BaseMessages.getString( PKG,
                  "SequoiaDBOutput.Msg.Err.FailedToGetTheFieldVal"
                  + "(" + fieldTmp.getName() + ":" + row[index].toString() + ")" ) );
         }
      }
      if ( condition.isEmpty() ) {
         return null ;
      }
      return condition ;
   }
}