package org.nuxeo.ecm.quota.automation;

import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.platform.userworkspace.api.UserWorkspaceService;
import org.nuxeo.ecm.quota.QuotaStatsService;
import org.nuxeo.runtime.api.Framework;

/**
 * Compute Quota size without resetting quota max size on documents. This operation aims to fix wrong quota computed in
 * the past in prod
 */
public class ReComputeQuotaForUser {
	public static final String ID = "ReComputeQuota.User";

    @Context
    protected CoreSession session;

    @Context
    QuotaStatsService quotaStatsService;

    @Param(name = "userName", required = true)
    protected String userName;

    @OperationMethod
    public String run() throws Exception {
        String updaterName = "documentsSizeUpdater";
        String userWSPath = getUserPersonalWorkspace(userName, session);
        quotaStatsService.launchInitialStatisticsComputation(updaterName, session.getRepositoryName(), userWSPath);
        return quotaStatsService.getProgressStatus(updaterName, session.getRepositoryName());
    }
    
    /**
     * Get User's Personal workspace
     * @param username
     * @param session the CoreSessiom Object
     * @return User's Personal workspace DocumentModel
     * @throws ClientException
     */
    private String getUserPersonalWorkspace(String userName, CoreSession session){
        UserWorkspaceService uws = Framework.getLocalService(UserWorkspaceService.class);
        DocumentModel userWorkspace = uws.getUserPersonalWorkspace(userName, session.getRootDocument());
        return userWorkspace.getPathAsString();
    }
}
