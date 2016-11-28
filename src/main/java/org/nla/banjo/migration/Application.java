package org.nla.banjo.migration;

import lombok.extern.slf4j.Slf4j;
import org.nla.banjo.migration.service.BanjoMigrationService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
@Slf4j
public class Application {

    public static final String AMBERDB_USERNAME = "amber.username";
    public static final String JDBC_URL = "amber.url";
    public static final String AMBERDB_PASSWORD = "amber.password";

    public static void main(String[] args) {
        ApplicationContext ctx = SpringApplication.run(Application.class, args);
        log.info(JDBC_URL + " provided - {}", System.getProperty(JDBC_URL));
        log.info(AMBERDB_USERNAME + " provided - {}", System.getProperty(AMBERDB_USERNAME));
        log.info(AMBERDB_PASSWORD + " provided - {}", System.getProperty(AMBERDB_PASSWORD));
        BanjoMigrationService migrationService = ctx.getBean(BanjoMigrationService.class);
        migrationService.migrate();
    }
}
