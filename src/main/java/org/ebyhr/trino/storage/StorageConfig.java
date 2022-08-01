/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ebyhr.trino.storage;

import io.airlift.configuration.Config;
import javax.validation.constraints.NotNull;


public class StorageConfig {
  private String boonduser = "";
  private  String boondpassword = "boondpassword";


  @NotNull
  public String getboondusername()
  {
    return boonduser;
  }

  @Config("storage.boondusername")
  public StorageConfig setboondusername(String username)
  {
    this.boonduser = username;
    return this;
  }

  @NotNull
  public String getboondpassword()
  {
    return boondpassword;
  }

  @Config("storage.boondpassword")
  public StorageConfig setboondpassword(String password)
  {
    this.boondpassword = password;
    return this;
  }


}
