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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
   
   public BSONObject kettleToBson( Object[] row, SequoiaDBOutputRecordInfo recordInfo ) throws KettleValueException{
      return recordInfo.toBson( row, m_outputRowMeta ) ;
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
