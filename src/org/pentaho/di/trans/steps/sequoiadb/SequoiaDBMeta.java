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
package org.pentaho.di.trans.steps.sequoiadb;

import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public abstract class SequoiaDBMeta extends BaseStepMeta implements StepMetaInterface {

   private static Class<?> PKG = SequoiaDBMeta.class;// for i18n purposes

   private String m_hostname = "localhost";
   private String m_port = "11810";
   private String m_CSName;
   private String m_CLName;

   @Override
   public Object clone() {
      SequoiaDBMeta retval =(SequoiaDBMeta) super.clone();
      retval.m_hostname = m_hostname;
      retval.m_port = m_port;
      retval.m_CSName = m_CSName;
      retval.m_CLName = m_CLName;
      return retval;
   }
   
   public String getHostname() {
      return m_hostname;
   }

   public void setHostname(String hostname) {
     this.m_hostname = hostname;
   }
   
   public String getPort() {
      return m_port;
    }

    public void setPort(String port) {
      this.m_port = port;
    }
    
    public String getCSName() {
       return m_CSName;
     }

     public void setCSName(String CSName) {
       this.m_CSName = CSName;
     }
     
     public String getCLName() {
        return m_CLName;
      }

      public void setCLName(String CLName) {
        this.m_CLName = CLName;
      }
}
