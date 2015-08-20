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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;

public class SequoiaDBInput extends BaseStep implements StepInterface {
   
   private SequoiaDBInputMeta m_meta;
   private SequoiaDBInputData m_data;
   private DBCursor           m_cursor;
   private boolean            m_isOutputJson = false;

	public SequoiaDBInput(StepMeta stepMeta,
			StepDataInterface stepDataInterface, int copyNr,
			TransMeta transMeta, Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	@Override
	public boolean init(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface) {
	   if(super.init(stepMetaInterface, stepDataInterface)){
	      m_meta = (SequoiaDBInputMeta)stepMetaInterface;
	      m_data = (SequoiaDBInputData)stepDataInterface;
	      
	      String connString = environmentSubstitute(m_meta.getHostname())
	            + ":" + environmentSubstitute(m_meta.getPort());
	      Sequoiadb sdb = null;
	      sdb = new Sequoiadb(connString, "", "");
	      if(!sdb.isCollectionSpaceExist(m_meta.getCSName())){
	         return false;
	      }
	      CollectionSpace cs = sdb.getCollectionSpace(m_meta.getCSName());
	      if(!cs.isCollectionExist(m_meta.getCLName())){
	         return false;
	      }
	      DBCollection cl = cs.getCollection(m_meta.getCLName());
	      
	      List<SequoiaDBInputField> selectedFields = m_meta.getSelectedFields();
	      if ( null != selectedFields && selectedFields.size() != 0 ){
	         BSONObject fieldsObj = new BasicBSONObject();
	         for ( SequoiaDBInputField f : selectedFields ){
	            String pathTmp = "$" + f.m_path;
	            fieldsObj.put(f.m_fieldName, pathTmp);
	         }
	         BSONObject projectObj = new BasicBSONObject();
	         projectObj.put("$project", fieldsObj);
	         List<BSONObject> aggrObjs = new ArrayList<BSONObject>();
	         aggrObjs.add(projectObj);
	         m_cursor = cl.aggregate( aggrObjs );
	      }
	      else
	      {
	         m_cursor = cl.query();
	      }
	      return true;
	   }
	   return false;
	}

	@Override
	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
	   // TODO close connection
	   super.dispose(smi, sdi);
	   m_cursor.close();
	}

	@Override
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException{
      // safely cast the step settings (meta) and runtime info (data) to specific implementations 
      SequoiaDBInputMeta meta = (SequoiaDBInputMeta) smi;
      SequoiaDBInputData data = (SequoiaDBInputData) sdi;

      // get incoming row, getRow() potentially blocks waiting for more rows, returns null if no more rows expected
      //Object[] r = getRow(); 
      
      // if no more rows are expected, indicate step is finished and processRow() should not be called again
      //if (r == null){
      //   setOutputDone();
      //   return false;
      //}

      // the "first" flag is inherited from the base step implementation
      // it is used to guard some processing tasks, like figuring out field indexes
      // in the row structure that only need to be done once
      if (first) {
         first = false;
         // clone the input row structure and place it in our data object
         data.outputRowMeta = new RowMeta();
         // use meta.getFields() to change it, so it reflects the output row structure 
         meta.getFields(data.outputRowMeta, getStepname(), null, null, SequoiaDBInput.this);
         meta.init( data.outputRowMeta );
         List<SequoiaDBInputField> selectedFields = meta.getSelectedFields();
         if ( null == selectedFields || selectedFields.size() == 0 ){
            m_isOutputJson = true;
         }
         else
         {
            m_isOutputJson = false;
         }
      }

      // read the record from SDB
      if(m_cursor.hasNext())
      {
         BSONObject obj = m_cursor.getNext();

         if( m_isOutputJson ){
             String json = obj.toString();
             Object row[] = RowDataUtil.allocateRowData(m_data.outputRowMeta.size());
             row[0]=json;
             putRow(data.outputRowMeta,row);
         }
         else{
	     List<BSONObject> objList = expandBSONArray( obj ) ;
             for ( BSONObject o : objList ) {
                 Object[] row = data.BSONToKettle( o, meta.getSelectedFields() ) ;
                 putRow(data.outputRowMeta,row);
             }
         }
      }
      else{
         setOutputDone();
         return false;
      }
      
      // safely add the string "Hello World!" at the end of the output row
      // the row array will be resized if necessary 
      //Object[] outputRow = RowDataUtil.addValueData(r, data.outputRowMeta.size() - 1, "Hello World!");

      // put the row to the output row stream
      //putRow(data.outputRowMeta, outputRow); 

      // log progress if it is time to to so
      if (checkFeedback(getLinesRead())) {
         logBasic("Linenr " + getLinesRead()); // Some basic logging
      }

      // indicate that processRow() should be called again
      return true;
	}
	
	private List<BSONObject> expandBSONArray( BSONObject obj ) {
		
		List<BSONObject> objList = null;
		List<BSONObject> total = new ArrayList<BSONObject>();
		Set<String> sets = obj.keySet() ;
		for ( String str : sets ) {
			if ( null == objList ) {
				Object v = obj.get(str);
				BasicBSONObject o = new BasicBSONObject() ;
				objList = genBSONObject(o, str, v);
			}
			else {
				total.clear();
				for ( BSONObject bo : objList ) {
					
					Object v = obj.get(str);
					List<BSONObject> loop = genBSONObject(bo, str, v);
					total.addAll(loop);
				}
				objList.clear();
				objList.addAll(total);
			}
		}
		
		if ( !objList.isEmpty() ) {
			total = objList;
		}
		
		return total;
	}

	private List<BSONObject> genBSONObject( BSONObject obj, String keyName, Object v ) {
		
		List<BSONObject> objList = new ArrayList<BSONObject>();
		if( v instanceof BasicBSONList ) {
			BasicBSONList l = (BasicBSONList) v ;
			Set<String> set = l.keySet();
			for( String str : set ) {
				BasicBSONObject o = new BasicBSONObject() ;
				if ( !obj.isEmpty() ) {
					o.putAll( obj );
				}
				o.append(keyName, l.get(str));
				objList.add(o);
			}
		}
		else
		{
			BasicBSONObject o = new BasicBSONObject() ;
			if ( !obj.isEmpty() ) {
				o.putAll( obj );
			}
			o.append(keyName, v);
			objList.add(o);
		}
		return objList;
	}
}
