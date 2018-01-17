package org.nla.application.migration.service;

import amberdb.AmberSession;
import lombok.extern.slf4j.Slf4j;
import org.nla.banjo.migration.repository.AmberRepository;
import org.skife.jdbi.v2.Handle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DataCleanUp {

    @Autowired
    AmberRepository repository;

    public void findWorksForCleanUp() {
        log.info("Clean up :: started");
        findWorks();
        log.info("Clean up :: Completed");
    }


    private void findWorks() {
        log.info("findWorks :: started");
        long[] works = new long[]{}; // provide ids here
        try (AmberSession amberSession = repository.getAmberDb().begin()) {
            try (Handle amberHandle = repository.getAmberDBI()) {
               amberHandle.select("SELECT DISTINCT e.v_in, e.v_out, e.edge_order\n" +
                       "FROM {{TABLE_NAMES}}" +
                       "WHERE e.v_in = {{ID}}\n" +
                       "  AND e.v_out = v.id\n" +
                       "  AND e.label = 'isPartOf'\n" +
                       "  AND p.id = v.id\n" +
                       "  AND p.type IN ({{TYPES}})\n" +
                       "ORDER BY e.edge_order;");

            }
        } finally {
            log.info("findWorks :: completed");
        }
    }

}
