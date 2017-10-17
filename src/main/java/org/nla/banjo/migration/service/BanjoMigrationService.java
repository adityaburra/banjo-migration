package org.nla.banjo.migration.service;

import amberdb.AmberSession;
import amberdb.model.Work;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.nla.banjo.migration.repository.AmberRepository;
import org.skife.jdbi.v2.Handle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class BanjoMigrationService {

    private static final String RESTRICTED = "Restricted";
    private static final String ONSITE_ONLY = "Onsite Only";
    private static final String UNRESTRICTED = "Unrestricted";
    private static final String OPEN = "Open";
    private static final String VIEW_ONLY = "View Only";
    private static final String BANJO = "banjo";
    private static final String BANJO_MIGRATION = "banjo migration";

    private static final String SELECT_ID_FROM_WORK_WHERE = "select id from ${table.name} where";

    @Autowired
    AmberRepository repository;

    public void migrate() {
        log.info("Banjo Migration :: started");
        updateAccessAgreement();
        updateAccessConditionAndInternalAccessCondition();
        log.info("Banjo Migration :: Completed");
    }


    private void updateAccessAgreement() {
        log.info("Access agreement update :: started");
        try (AmberSession amberSession = repository.getAmberDb().begin()) {
            try (Handle amberHandle = repository.getAmberDBI()) {
                queryAndUpdate(amberSession, amberHandle, ACCESSAGREEMENT_BASICACCESS, "BasicAccess");
                queryAndUpdate(amberSession, amberHandle, ACCESSAGREEMENT_OPENACCESSIMMEDIATELY, "OpenAccessImmediately");
                queryAndUpdate(amberSession, amberHandle, ACCESSAGREEMENT_OPENACCESSEMBARGOED, "OpenAccessEmbargoed");
                log.info("Amber session commit in progress");
                amberSession.commit(BANJO, BANJO_MIGRATION);
                log.info("Amber session commit successful");
            }
        } finally {
            log.info("Access agreement update :: completed");
        }
    }


    private void updateAccessConditionAndInternalAccessCondition() {
        log.info("Access Condition and Internal Access Condition update :: started");
        try (AmberSession amberSession = repository.getAmberDb().begin()) {
            try (Handle amberHandle = repository.getAmberDBI()) {
                queryAndUpdateAccessCondition(amberSession, amberHandle, ACCESSCONDITION_BASED_ON_SUBUNITTYPE, UNRESTRICTED, OPEN);
                queryAndUpdateAccessCondition(amberSession, amberHandle, ACCESSCONDITION_BASED_ON_ACCESSAGREEMENT_AND_COMMERCIALSTATUS, ONSITE_ONLY, RESTRICTED);
                queryAndUpdateAccessCondition(amberSession, amberHandle, ACCESSCONDITION_BASED_ON_ACCESSAGREEMENT_AND_COMMERCIALSTATUS_AND_EXPIRY, ONSITE_ONLY, RESTRICTED);
                queryAndUpdateAccessCondition(amberSession, amberHandle, ACCESSCONDITION_BASED_ON_ACCESSAGREEMENT_AND_NONCOMMERCIALSTATUS, VIEW_ONLY, RESTRICTED);
                queryAndUpdateAccessCondition(amberSession, amberHandle, ACCESSCONDITION_BASED_ON_ACCESSAGREEMENT_AND_NONCOMMERCIALSTATUS_AND_EXPIRY, VIEW_ONLY, RESTRICTED);
                queryAndUpdateAccessCondition(amberSession, amberHandle, ACCESSCONDITION_BASED_ON_ACCESSAGREEMENT, UNRESTRICTED, OPEN);
                queryAndUpdateAccessCondition(amberSession, amberHandle, ACCESSCONDITION_BASED_ON_ACCESSAGREEMENT_AND_EXPIRY, UNRESTRICTED, OPEN);
                log.info("Amber session commit in progress");
                amberSession.commit(BANJO, BANJO_MIGRATION);
                log.info("Amber session commit successful");
            }
        } finally {
            log.info("Access Condition and Internal Access Condition update :: completed");
        }
    }

    private void queryAndUpdateAccessCondition(AmberSession amberSession, Handle amberHandle, String sql, String accessCondition, String internalAccessCondition) {
        log.info("Querying works with sql -> {}", sql);
        List<Long> objIds = queryForWorks(amberHandle, sql);
        log.info("Number of works found: {} and work ids: {}", objIds.size(), objIds);
        log.info("Updating accessCondition to '{}' and internalAccessCondition to '{}' for the works above", accessCondition, internalAccessCondition);
        objIds.forEach(objId -> updateAccessCondition(amberSession, accessCondition, internalAccessCondition, objId));
    }

    private void updateAccessCondition(AmberSession amberSession, String accessCondition, String internalAccessCondition, Long objId) {
        Work work = amberSession.findWork(objId);
        work.setAccessConditions(accessCondition);
        work.setInternalAccessConditions(internalAccessCondition);
    }

    private void queryAndUpdate(AmberSession amberSession, Handle amberHandle, String sql, String accessAgreement) {
        log.info("Querying works with sql -> {}", sql);
        List<Long> objIds = queryForWorks(amberHandle, sql);
        log.info("Number of works found: {} and work ids: {}", objIds.size(), objIds);
        log.info("Updating accessAgreement to '{}' for the works above", accessAgreement);
        objIds.forEach(objId -> updateAccessAgreement(amberSession, accessAgreement, objId));
    }

    private void updateAccessAgreement(AmberSession amberSession, String accessAgreement, Long objId) {
        Work work = amberSession.findWork(objId);
        work.setAccessAgreement(accessAgreement);
    }

    private List<Long> queryForWorks(Handle amberHandle, String sql) {
        List<Map<String, Object>> select = amberHandle.select(sql);
        return Lists.transform(select, stringObjectMap -> (Long) stringObjectMap.get("id"));
    }
}
