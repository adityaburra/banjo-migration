package org.nla.application.migration;

import lombok.extern.slf4j.Slf4j;
import org.nla.application.migration.service.MigrationService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
@Slf4j
public class Application {

    private static final String AMBERDB_USERNAME = "amber.username";
    private static final String JDBC_URL = "amber.url";

    public static void main(String[] args) {
        ApplicationContext ctx = SpringApplication.run(Application.class, args);
        log.info(JDBC_URL + " provided - {}", System.getProperty(JDBC_URL));
        log.info(AMBERDB_USERNAME + " provided - {}", System.getProperty(AMBERDB_USERNAME));
        MigrationService migrationService = ctx.getBean(MigrationService.class);
        migrationService.migrate();
    }
}
