package org.nla.banjo.migration.repository;

import amberdb.AmberDb;
import com.jolbox.bonecp.BoneCPDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.nio.file.Paths;

@Repository
public class AmberRepository {

    private AmberDb amberDb;

    private DataSource dataSource;

    private Path amberPath;

    @Value("${amber.url}")
    private String jdbcUrl;

    @Value("${amber.username}")
    private String userName;

    @Value("${amber.password}")
    private String password;

    @Value("${amber.path:/xyz}")
    private String amberdbPath;

    public synchronized AmberDb getAmberDb() {
        if (amberDb == null) {
            amberDb = new AmberDb(getDataSource(), getPath());
        }
        return amberDb;
    }

    private synchronized DataSource getDataSource() {
        if (dataSource == null) {
            BoneCPDataSource boneCPDataSource = new BoneCPDataSource();
            boneCPDataSource.setMaxConnectionsPerPartition(10);
            boneCPDataSource.setPartitionCount(5);
            boneCPDataSource.setDisableJMX(true);
            boneCPDataSource.setIdleConnectionTestPeriodInMinutes(5);
            boneCPDataSource.setJdbcUrl(jdbcUrl);
            boneCPDataSource.setUsername(userName);
            boneCPDataSource.setPassword(password);
            dataSource = boneCPDataSource;
        }
        return dataSource;
    }

    private Path getPath() {
        if (amberPath == null) {
            amberPath = Paths.get(amberdbPath);
        }
        return amberPath;
    }

    public Handle getAmberDBI() {
        DBI amberDbi = new DBI(dataSource);
        return amberDbi.open();
    }
}
