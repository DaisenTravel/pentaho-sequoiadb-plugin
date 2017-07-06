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
package org.pentaho.di.trans.steps.sequoiadbinput;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;



public class SequoiaDBInputField implements Comparable<SequoiaDBInputField> {

   protected static Class<?> PKG = SequoiaDBInputField.class;
   public String m_fieldName = "";
   public String m_kettleType = "";
   public String m_path = "";
   public int m_outputIndex = 0;
   private ValueMetaInterface m_tmpValueMeta;
   
   public void init( int index ) throws KettlePluginException{
      m_tmpValueMeta = ValueMetaFactory.createValueMeta( ValueMeta.getType( m_kettleType ) );
      m_outputIndex = index;
   }

   @Override
   public int compareTo( SequoiaDBInputField comp ) {
     return m_fieldName.compareTo( comp.m_fieldName );
   }
   
   public Object getKettleValue( Object input, int type ) throws KettleException{
      Object result = null;
      Object valTmp = null;
      switch ( type ){
         case ValueMetaInterface.TYPE_INTEGER:
            if ( null == input )
            {
               // TODO: get default value from dialog-input
               valTmp = getDefaultValue( type );
            }
            else if ( input instanceof Number ){
               valTmp = new Long( ((Number)input).intValue() );
            }
            else if( input instanceof Binary ){
               byte[] b = ((Binary)input).getData();
               String s = new String( b );
               valTmp = new Long( s );
            }
            else{
               valTmp = new Long( input.toString() );
            }
            result = m_tmpValueMeta.getInteger( valTmp );
            break;
         case ValueMetaInterface.TYPE_NUMBER:
            if ( null == input ){
               valTmp = getDefaultValue( type );
            }
            else if ( input instanceof Number ){
               valTmp = new Double(((Number)input).doubleValue());
            }
            else if ( valTmp instanceof Binary ){
               byte[] b = ((Binary)input).getData();
               String s = new String( b );
               valTmp = new Double( s );
            }
            else{
               valTmp = new Double( input.toString() );
            }
            result = m_tmpValueMeta.getNumber( valTmp );
            break;
         case ValueMetaInterface.TYPE_STRING:
            if ( null == input ){
               valTmp = getDefaultValue( type );
               result = m_tmpValueMeta.getString( valTmp );
            }
            else{
               result = m_tmpValueMeta.getString( input );
            }
            break;
         case ValueMetaInterface.TYPE_BINARY:
            if ( null == input ){
               valTmp = getDefaultValue( type );
            }
            else if ( input instanceof Binary ){
               valTmp = ((Binary)input).getData();
            }
            else{
               valTmp = input.toString().getBytes();
            }
            result = m_tmpValueMeta.getBinary( valTmp );
            break;
         case ValueMetaInterface.TYPE_BOOLEAN:
            if ( null == input ){
               valTmp = getDefaultValue( type );
            }
            else if ( input instanceof Number ){
               valTmp = new Boolean(((Number)input).intValue() != 0 );
            }
            else if ( input instanceof Date ){
               valTmp = new Boolean(((Date)input).getTime() != 0 );
            }
            else{
               valTmp = new Boolean(   input.toString().equalsIgnoreCase( "false" )
                                    || input.toString().equalsIgnoreCase( "0" ) );
               
            }
            result = !m_tmpValueMeta.getBoolean( valTmp );
            break;
         case ValueMetaInterface.TYPE_DATE:
            if ( null == input ){
               valTmp = getDefaultValue( type );
            }
            else if( input instanceof Number ){
               valTmp = new Date(((Number)input).longValue());
            }
            else if( input instanceof BSONTimestamp ) {
               valTmp = ((BSONTimestamp)input).getDate();
            }
            else if ( input instanceof Date ){
               valTmp = input;
            }
            else{
               throw new KettleException( BaseMessages.getString( PKG, "SequoiaDB.ErrorMessage.DateConversion",
                     input.toString() ));
            }
            result = m_tmpValueMeta.getDate( valTmp );
            break;
         case ValueMetaInterface.TYPE_TIMESTAMP:
            if ( null == input ){
               valTmp = getDefaultValue( type );
            }
            else if( input instanceof Number ){
               valTmp = new Timestamp(((Number)input).longValue());
            }
            else if( input instanceof Date ) {
               valTmp = new Timestamp(((Date)input).getTime());
            }
            else if ( input instanceof BSONTimestamp ){
               long tmp = ((BSONTimestamp)input).getTime();
               valTmp = new Timestamp(tmp*1000);
            }
            else{
               throw new KettleException( BaseMessages.getString( PKG, "SequoiaDB.ErrorMessage.DateConversion",
                     input.toString() ));
            }
            result = m_tmpValueMeta.getDate( valTmp );
            break;
         default:
      }
      return result;
   }
   
   private Object getDefaultValue( int type )
   {
   // TODO: get default value from dialog-input
      Object result = null;
      switch ( type ){
         case ValueMetaInterface.TYPE_INTEGER:
            result = new Long(0);
            break;
         case ValueMetaInterface.TYPE_NUMBER:
            result = new Double( 0 );
            break;
         case ValueMetaInterface.TYPE_STRING:
            result = new String( "" );
            break;
         case ValueMetaInterface.TYPE_BIGNUMBER:
            result = new BigDecimal( 0 );
            break;
         case ValueMetaInterface.TYPE_BINARY:
            result = new Binary( null );
            break;
         case ValueMetaInterface.TYPE_BOOLEAN:
            result = new Boolean( true );
            break;
         case ValueMetaInterface.TYPE_DATE:
            result = new Date();
            break;
         case ValueMetaInterface.TYPE_TIMESTAMP:
            result = new Timestamp(0);
            break;
         default:
      }
      return result;
   }
}
