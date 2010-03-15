/*
 * Copyright 2008-2009 LinkedIn, Inc
 * Copyright 2010 Marcus Eriksson
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.drizzle;

import org.apache.commons.dbcp.BasicDataSource;
import voldemort.VoldemortException;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

import java.sql.SQLException;

public class DrizzleStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "drizzle";

    private final BasicDataSource dataSource;

    public DrizzleStorageConfiguration(VoldemortConfig config) {
        BasicDataSource ds = new BasicDataSource();
        ds.setUrl("jdbc:drizzle://" + config.getDrizzleHost() + ":" + config.getDrizzlePort() + "/"
                  + config.getDrizzleDatabaseName());
        ds.setUsername(config.getDrizzleUsername());
        ds.setPassword(config.getDrizzlePassword());
        ds.setDriverClassName("org.drizzle.jdbc.DrizzleDriver");
        this.dataSource = ds;
    }

    public StorageEngine<ByteArray, byte[]> getStore(String name) {
        return new DrizzleStorageEngine(name, dataSource);
    }

    public String getType() {
        return TYPE_NAME;
    }

    public void close() {
        try {
            this.dataSource.close();
        } catch(SQLException e) {
            throw new VoldemortException("Exception while closing connection pool.", e);
        }
    }

}