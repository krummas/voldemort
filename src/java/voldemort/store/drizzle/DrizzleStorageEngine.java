package voldemort.store.drizzle;

import voldemort.store.mysql.MysqlStorageEngine;
import javax.sql.DataSource;


/**
 * A StorageEngine that uses Drizzle for persistence. Extends the mysql storage engine. Currently no
 * diff in usage, only in configuration.
 *
 * @author Marcus Eriksson (krummas@gmail.com)
 *
 */
public class DrizzleStorageEngine extends MysqlStorageEngine {
    public DrizzleStorageEngine(String name, DataSource datasource) {
        super(name, datasource);
    }
}