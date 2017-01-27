/*
 * (C) Copyright 2011 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */
package org.nuxeo.ecm.quota.count;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.nuxeo.ecm.quota.size.QuotaAwareDocument.DOCUMENTS_SIZE_STATISTICS_FACET;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentRef;
import org.nuxeo.ecm.core.api.PathRef;
import org.nuxeo.ecm.core.api.impl.blob.StringBlob;
import org.nuxeo.ecm.core.event.EventService;
import org.nuxeo.ecm.core.event.EventServiceAdmin;
import org.nuxeo.ecm.core.test.annotations.TransactionalConfig;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.ecm.platform.userworkspace.api.UserWorkspaceService;
import org.nuxeo.ecm.quota.QuotaStatsService;
import org.nuxeo.ecm.quota.size.QuotaAware;
import org.nuxeo.ecm.quota.size.QuotaSizeService;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import com.google.inject.Inject;

/**
 * NXP-21017 : Test that quota re-computation by path
 * Contributors: lnirupama@ssdi.sharp.co.in
 */

@RunWith(FeaturesRunner.class)
@Features(QuotaFeature.class)
@TransactionalConfig(autoStart = false)
public class TestRecomputeQuota {

    @Inject
    protected QuotaStatsService qs;

    @Inject
    QuotaSizeService sus;

    @Inject
    protected UserWorkspaceService uwm;

    @Inject
    CoreSession session;

    @Inject
    EventService eventService;

    protected DocumentRef fileRef;

    private IsolatedSessionRunner isr;

    @Before
    public void cleanupSessionAssociationBeforeTest() throws Exception {
        isr = new IsolatedSessionRunner(session, eventService);
        assertThat(sus, is(notNullValue()));
    }

    @Test
    public void itReComputeQuota() throws Exception {
        EventServiceAdmin eventAdmin = Framework.getLocalService(EventServiceAdmin.class);
        eventAdmin.setListenerEnabledFlag("quotaStatsListener", true);

        // Initiate and enable quota computation on user workspace
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                qs.activateQuotaOnUserWorkspaces(300L, session);
                qs.launchSetMaxQuotaOnUserWorkspaces(300L, session.getRootDocument(), session);

            }
        });

        WorkManager workManager = Framework.getLocalService(WorkManager.class);
        workManager.awaitCompletion("quota", 3, TimeUnit.SECONDS);
        assertEquals(0, workManager.getQueueSize("quota", null));

        // Add a file
        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                DocumentModel uw = uwm.getUserPersonalWorkspace("toto",
                        session.getDocument(new PathRef("/default-domain/")));
                assertQuota(uw, 0, 0, 300L);
                DocumentModel firstFile = session.createDocumentModel(uw.getPathAsString(), "fileQuota", "File");
                firstFile.setPropertyValue("file:content", (Serializable) getFakeBlob(100));
                firstFile = session.createDocument(firstFile);
                session.saveDocument(firstFile);
                session.save();

            }
        });

        // Check quota has been updated
        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                DocumentModel uw = uwm.getUserPersonalWorkspace("toto",
                        session.getDocument(new PathRef("/default-domain/")));
                assertQuota(uw, 0, 100, 300L);

            }
        });

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                DocumentModel uw = uwm.getUserPersonalWorkspace("toto",
                        session.getDocument(new PathRef("/default-domain/")));
                // Corrupt statistics by setting wrong size manually
                uw.setPropertyValue("dss:totalSize", 80L);
                session.saveDocument(uw);

                uw = uwm.getUserPersonalWorkspace("toto", session.getDocument(new PathRef("/default-domain/")));
                assertQuota(uw, 0, 80, 300L);
                // Run recompute quota API
                qs.launchInitialStatisticsComputation("documentsSizeUpdater", session.getRepositoryName(),
                        "/default-domain");
                eventService.waitForAsyncCompletion();

            }
        });

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                DocumentModel uw = uwm.getUserPersonalWorkspace("toto",
                        session.getDocument(new PathRef("/default-domain/")));
                // Assert that quota has been recomputed successfully
                assertQuota(uw, 0, 100, -1L);
            }
        });

    }

    protected Blob getFakeBlob(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append('a');
        }
        Blob blob = new StringBlob(sb.toString());
        blob.setMimeType("text/plain");
        blob.setFilename("FakeBlob_" + size + ".txt");
        return blob;
    }

    protected void assertQuota(DocumentModel doc, long innerSize, long totalSize) {
        assertTrue(doc.hasFacet(DOCUMENTS_SIZE_STATISTICS_FACET));
        QuotaAware qa = doc.getAdapter(QuotaAware.class);
        assertNotNull(qa);
        assertEquals("inner:" + innerSize + " total:" + totalSize,
                "inner:" + qa.getInnerSize() + " total:" + qa.getTotalSize());
    }

    protected void assertQuota(DocumentModel doc, long innerSize, long totalSize, long maxSize) {
        assertTrue(doc.hasFacet(DOCUMENTS_SIZE_STATISTICS_FACET));
        QuotaAware qa = doc.getAdapter(QuotaAware.class);
        assertNotNull(qa);
        assertEquals("inner:", innerSize, qa.getInnerSize());
        assertEquals("total:", totalSize, qa.getTotalSize());
        assertEquals("max:", maxSize, qa.getMaxQuota());
    }

}
