/**
 *      Copyright (C) 2012 SequoiaDB Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.pentaho.di.trans.steps.sequoiadboutput;

import java.util.Map;
import java.util.TreeMap;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.i18n.BaseMessages;

public class SequoiaDBOutputField{

   private static Class<?> PKG = SequoiaDBOutputMeta.class;

   private String m_fieldName = "" ;
   
   private String m_srcFieldName = "" ;
   
   //private List<SequoiaDBOutputField> m_children = null ;
   
   private Map<String, SequoiaDBOutputField> m_children = null ;
   
   private boolean m_isArray = false ;
      
   private void parseFieldPath( String fieldPath, String[] fieldPathInfo ){
      if( null == fieldPath || fieldPath.isEmpty() ){
         return ;
      }
      int index = fieldPath.indexOf( '.' ) ;
      if ( -1 == index ){
         fieldPathInfo[0] = fieldPath ;
         return ;
      }
      fieldPathInfo[0] = fieldPath.substring( 0, index ) ;
      fieldPathInfo[1] = fieldPath.substring( index + 1 ) ;
   }
   
   private int getArrayPos(){
      if ( null == m_fieldName || m_fieldName.isEmpty() ){
         return -1 ;
      }
      if ( m_fieldName.startsWith( "$" ) ){
         String index = m_fieldName.substring(1) ;
         try{
            int i = Integer.parseInt( index ) ;
            return i ;
         }
         catch( NumberFormatException e ){
            return -1 ;
         }
      }
      return -1 ;
   }
   
   public void init( String srcFieldName,  String fieldPath ) throws KettleValueException{
      if ( null == srcFieldName || srcFieldName.isEmpty()
            || null == fieldPath || fieldPath.isEmpty() ){
         throw new KettleValueException( BaseMessages.getString( PKG,
                                         "SequoiaDBOutput.Msg.Err.FailedToAddField",
                                         srcFieldName, fieldPath ));
      }
      String[] fieldPathInfo = new String[2] ;
      parseFieldPath( fieldPath, fieldPathInfo ) ;
      m_fieldName = fieldPathInfo[0] ;
      if ( null == fieldPathInfo[1] || fieldPathInfo[1].isEmpty() ){
         m_srcFieldName = srcFieldName ;
         return ;
      }
      
      SequoiaDBOutputField childField = new SequoiaDBOutputField() ;
      childField.init( srcFieldName, fieldPathInfo[1] ) ;
      if ( null == m_children ){
         m_children = new TreeMap<String, SequoiaDBOutputField>() ;
      }
      m_children.put( childField.getFieldName(), childField ) ;
   }
   
   public String getFieldName(){
      return m_fieldName ;
   }
   
   public boolean isNeedMerge( String fieldPath ){
      if ( null == fieldPath || fieldPath.isEmpty() ){
         return false ;
      }
      String[] fieldPathInfo = new String[2] ;
      parseFieldPath( fieldPath, fieldPathInfo ) ;
      
      if ( 0 == m_fieldName.compareTo( fieldPathInfo[0]) ){
         return true ;
      }
      return false ;
   }
   
   public void addField( String srcFieldName,  String fieldPath ) throws KettleValueException{
      if ( null == srcFieldName || srcFieldName.isEmpty()
            || null == fieldPath || fieldPath.isEmpty()
            || !m_srcFieldName.isEmpty() ){
         throw new KettleValueException( BaseMessages.getString( PKG,
                                         "SequoiaDBOutput.Msg.Err.FailedToAddSubField",
                                         srcFieldName, fieldPath ));
      }
      String[] fieldPathInfo = new String[2] ;
      parseFieldPath( fieldPath, fieldPathInfo ) ;
      if ( 0 != m_fieldName.compareTo( fieldPathInfo[0])
            || null == fieldPathInfo[1] || fieldPathInfo[1].isEmpty() ){
         throw new KettleValueException( BaseMessages.getString( PKG,
               "SequoiaDBOutput.Msg.Err.FailedToAddSubField",
               srcFieldName, fieldPath ));
      }
      
      boolean isMerged = false ;
      for( Map.Entry<String, SequoiaDBOutputField> entry : m_children.entrySet() ){
         if ( entry.getValue().isNeedMerge( fieldPathInfo[1] ) ){
            entry.getValue().addField( srcFieldName, fieldPathInfo[1] ) ;
            isMerged = true ;
            break ;
         }
      }
      if ( !isMerged ){
         SequoiaDBOutputField newField = new SequoiaDBOutputField() ;
         newField.init( srcFieldName, fieldPathInfo[1] ) ;
         if ( null == m_children ){
            m_children = new TreeMap<String, SequoiaDBOutputField>() ;
         }
         m_children.put( newField.getFieldName(), newField ) ;
      }
   }
   
   public Object toObj(  Map<String, SequoiaDBOutputFieldInfo> fieldsInfo ) throws KettleValueException{
      if ( null == m_children || m_children.isEmpty() ){
         return fieldsInfo.get( m_srcFieldName ).getVal() ;
      }
      else{
         BSONObject childrenField = null ;
         if ( m_isArray ){
            childrenField = new BasicBSONList() ;
            Integer i = 0;
            for( Map.Entry<String, SequoiaDBOutputField> entry : m_children.entrySet() ){
               childrenField.put( i.toString(), entry.getValue().toObj( fieldsInfo ) ) ;
               ++i ;
               }
         }
         else {
            childrenField = new BasicBSONObject() ;
            for( Map.Entry<String, SequoiaDBOutputField> entry : m_children.entrySet() ){
               childrenField.put( entry.getKey(), entry.getValue().toObj( fieldsInfo ) ) ;
               }
         }
         return childrenField ;
      }
   }
   
   public void done() throws KettleValueException{
      boolean hasNotArrayField = false ;
      if ( null == m_children || m_children.size() == 0 ){
         hasNotArrayField = true ;
      }
      else{
         for( Map.Entry<String, SequoiaDBOutputField> entry : m_children.entrySet() ){
            entry.getValue().done();
            if ( !hasNotArrayField && -1 == entry.getValue().getArrayPos() ){
               hasNotArrayField = true ;
            }
         }
      }
      m_isArray = !hasNotArrayField ;
   }
}
