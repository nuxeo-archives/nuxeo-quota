package org.nuxeo.ecm.quota.automation;

import java.security.Principal;

import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.NuxeoPrincipal;
import org.nuxeo.ecm.core.api.PathRef;
import org.nuxeo.ecm.quota.QuotaStatsService;

/**
 * Compute Quota size without resetting quota max size on documents. This operation aims to fix wrong quota computed in
 * the past in prod
 */
@Operation(id = ReComputeQuotaForTenant.ID, category = Constants.CAT_DOCUMENT, label = "Compute quota on users documents under Tenant ")
public class ReComputeQuotaForTenant {

    public static final String ID = "ReComputeQuota.Tenant";

    @Context
    protected CoreSession session;
    
    @Param(name = "tenantId", required = true)
    protected String tenantId;

    @Context
    QuotaStatsService quotaStatsService;

    @OperationMethod
    public String run() throws Exception {
        String updaterName = "documentsSizeUpdater";
        String quotaStatus = "";
        Principal principal = session.getPrincipal();
        // Allow only administrator to perform this action
        if (principal instanceof NuxeoPrincipal) {
            if (!((NuxeoPrincipal) principal).isAdministrator()) {
                return quotaStatus = "Not authorized to perform this operation";
            }
        }
        // Get the Tenant path
        String tenantPath = session.getDocument(new PathRef("/" + tenantId)).getPathAsString();
        quotaStatsService.launchInitialStatisticsComputation(updaterName, session.getRepositoryName(), tenantPath);
        quotaStatus = quotaStatsService.getProgressStatus(updaterName, session.getRepositoryName());
        return quotaStatus;
    }
}
