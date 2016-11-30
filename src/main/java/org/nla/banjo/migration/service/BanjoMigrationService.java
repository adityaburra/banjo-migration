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

    private static final String SELECT_ID_FROM_WORK_WHERE = "select id from work where";

    private static final String DEPOSIT_TYPE_IN = " depositType IN ('OnlineLegal', 'OnlineGovernment', 'OnlineVoluntary', 'OfflineVoluntary')";

    private static final String ACCESSAGREEMENT_BASICACCESS = SELECT_ID_FROM_WORK_WHERE + DEPOSIT_TYPE_IN + " AND availabilityConstraint LIKE '%edeposit library access coming soon%'";

    private static final String ACCESSAGREEMENT_OPENACCESSIMMEDIATELY = SELECT_ID_FROM_WORK_WHERE + DEPOSIT_TYPE_IN + " AND availabilityConstraint LIKE '%edeposit online access coming soon%'";

    private static final String ACCESSAGREEMENT_OPENACCESSEMBARGOED = SELECT_ID_FROM_WORK_WHERE + DEPOSIT_TYPE_IN + " AND availabilityConstraint LIKE '%edeposit online access after embargo%'";

    private static final String ACCESSCONDITION_BASED_ON_SUBUNITTYPE = SELECT_ID_FROM_WORK_WHERE + " subUnitType = 'Cover' AND depositType IS NOT NULL AND accessAgreement IS NOT NULL;";

    private static final String ACCESSCONDITION_BASED_ON_ACCESSAGREEMENT_AND_COMMERCIALSTATUS = SELECT_ID_FROM_WORK_WHERE + " accessAgreement = 'BasicAccess' AND (NULLIF(commercialStatus, '') IS NULL OR commercialStatus = 'Commercial');";

    private static final String ACCESSCONDITION_BASED_ON_ACCESSAGREEMENT_AND_COMMERCIALSTATUS_AND_EXPIRY = SELECT_ID_FROM_WORK_WHERE + " accessAgreement = 'OpenAccessEmbargoed'" +
            " AND (NULLIF(commercialStatus, '') IS NULL OR commercialStatus = 'Commercial') AND DATE(expiryDate) >= curdate();";

    private static final String ACCESSCONDITION_BASED_ON_ACCESSAGREEMENT_AND_NONCOMMERCIALSTATUS = SELECT_ID_FROM_WORK_WHERE + " accessAgreement = 'BasicAccess' AND commercialStatus = 'Noncommerc';";

    private static final String ACCESSCONDITION_BASED_ON_ACCESSAGREEMENT_AND_NONCOMMERCIALSTATUS_AND_EXPIRY = SELECT_ID_FROM_WORK_WHERE + " accessAgreement = 'OpenAccessEmbargoed'" +
            " AND commercialStatus = 'Noncommerc' AND DATE(expiryDate) >= curdate();";

    private static final String ACCESSCONDITION_BASED_ON_ACCESSAGREEMENT = SELECT_ID_FROM_WORK_WHERE + " accessAgreement = 'OpenAccessImmediately';";

    private static final String ACCESSCONDITION_BASED_ON_ACCESSAGREEMENT_AND_EXPIRY = SELECT_ID_FROM_WORK_WHERE + " accessAgreement = 'OpenAccessEmbargoed' AND DATE(expiryDate) <= DATE_SUB(curdate(), INTERVAL 1 DAY);";

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