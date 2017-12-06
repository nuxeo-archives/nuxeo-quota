/*
 * (C) Copyright 2006-2011 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Thomas Roger <troger@nuxeo.com>
 */

package org.nuxeo.ecm.quota.count;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.nuxeo.ecm.quota.size.QuotaAwareDocument.DOCUMENTS_SIZE_STATISTICS_FACET;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.Blobs;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.api.DocumentRef;
import org.nuxeo.ecm.core.api.PathRef;
import org.nuxeo.ecm.core.api.VersioningOption;
import org.nuxeo.ecm.core.event.EventService;
import org.nuxeo.ecm.core.event.EventServiceAdmin;
import org.nuxeo.ecm.core.query.sql.NXQL;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.trash.TrashService;
import org.nuxeo.ecm.core.versioning.VersioningService;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.ecm.platform.userworkspace.api.UserWorkspaceService;
import org.nuxeo.ecm.quota.QuotaStatsInitialWork;
import org.nuxeo.ecm.quota.QuotaStatsService;
import org.nuxeo.ecm.quota.size.QuotaAware;
import org.nuxeo.ecm.quota.size.QuotaAwareDocument;
import org.nuxeo.ecm.quota.size.QuotaExceededException;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.transaction.TransactionHelper;

/**
 * Various test that verify quota Sizes are updated accordingly to various operations. Due to the asynchronous nature of
 * size update, these test rely on {@link Thread#sleep(long)} and can fail because of that.
 *
 * @since 5.6
 */
@RunWith(FeaturesRunner.class)
@Features({ QuotaFeature.class })
public class TestDocumentsSizeUpdater {

    @Inject
    protected QuotaStatsService quotaStatsService;

    @Inject
    protected CoreSession session;

    @Inject
    protected CoreFeature coreFeature;

    @Inject
    protected EventService eventService;

    @Inject
    protected UserWorkspaceService uwm;

    @Inject
    protected WorkManager workManager;

    protected DocumentRef wsRef;

    protected DocumentRef firstFolderRef;

    protected DocumentRef secondFolderRef;

    protected DocumentRef firstSubFolderRef;

    protected DocumentRef secondSubFolderRef;

    protected DocumentRef firstFileRef;

    protected DocumentRef secondFileRef;

    private IsolatedSessionRunner isr;

    @Before
    public void cleanupSessionAssociationBeforeTest() throws Exception {
        isr = new IsolatedSessionRunner(session, eventService);
    }

    @Test
    public void testQuotaOnAddContent() throws Exception {

        addContent();
        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                dump();

                assertQuota(getFirstFile(), 100L, 100L);
                assertQuota(getSecondFile(), 200L, 200L);
                assertQuota(getFirstSubFolder(), 0L, 300L);
                assertQuota(getFirstFolder(), 0L, 300L);
                assertQuota(getWorkspace(), 0L, 300L);

            }
        });

    }

    @Test
    public void testQuotaOnAddAndModifyContent() throws Exception {

        addContent();
        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                dump();

                assertQuota(getFirstFile(), 100L, 100L);
                assertQuota(getSecondFile(), 200L, 200L);
                assertQuota(getFirstSubFolder(), 0L, 300L);
                assertQuota(getFirstFolder(), 0L, 300L);
                assertQuota(getWorkspace(), 0L, 300L);

            }
        });

        doUpdateContent();

        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                dump();

                assertQuota(getFirstFile(), 380L, 380L);
                assertQuota(getSecondFile(), 200L, 200L);
                assertQuota(getFirstSubFolder(), 0L, 580L);
                assertQuota(getFirstFolder(), 0L, 580L);
                assertQuota(getWorkspace(), 50L, 630L);
            }
        });

    }

    @Test
    public void testQuotaInitialCheckIn() throws Exception {

        addContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                dump();

                assertQuota(getFirstFile(), 100, 100, 0, 0);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 300, 0, 0);
                assertQuota(getFirstFolder(), 0, 300, 0, 0);
                assertQuota(getWorkspace(), 0, 300, 0, 0);
            }
        });

        doCheckIn();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                dump();

                DocumentModel firstFile = getFirstFile();
                assertFalse(firstFile.isCheckedOut());
                assertEquals("0.1", firstFile.getVersionLabel());

                // checked in: We count immediatly because at user (UI) level
                // checkout is not a visible action
                assertQuota(firstFile, 100, 200, 0, 100);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 400, 0, 100);
                assertQuota(getFirstFolder(), 0, 400, 0, 100);
                assertQuota(getWorkspace(), 0, 400, 0, 100);
            }
        });

        // checkout the doc
        doCheckOut();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                dump();

                DocumentModel firstFile = getFirstFile();
                assertTrue(firstFile.isCheckedOut());
                assertEquals("0.1+", firstFile.getVersionLabel());

                assertQuota(firstFile, 100, 200, 0, 100);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 400, 0, 100);
                assertQuota(getFirstFolder(), 0, 400, 0, 100);
                assertQuota(getWorkspace(), 0, 400, 0, 100);
            }
        });
    }

    @Test
    public void testQuotaOnCheckInCheckOut() throws Exception {

        addContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();

            }
        });

        doCheckIn();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                DocumentModel firstFile = getFirstFile();
                assertFalse(firstFile.isCheckedOut());
                assertEquals("0.1", firstFile.getVersionLabel());

                assertQuota(firstFile, 100, 200, 0, 100);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 400, 0, 100);
                assertQuota(getFirstFolder(), 0, 400, 0, 100);
                assertQuota(getWorkspace(), 0, 400, 0, 100);
            }
        });

        // checkout the doc
        doCheckOut();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                dump();

                DocumentModel firstFile = getFirstFile();
                assertTrue(firstFile.isCheckedOut());
                assertEquals("0.1+", firstFile.getVersionLabel());

                assertQuota(firstFile, 100, 200, 0, 100);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 400, 0, 100);
                assertQuota(getFirstFolder(), 0, 400, 0, 100);
                assertQuota(getWorkspace(), 0, 400, 0, 100);

            }
        });
    }

    @Test
    public void testQuotaOnVersions() throws Exception {

        addContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                dump();

                assertQuota(getFirstFile(), 100, 100, 0, 0);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 300, 0, 0);
                assertQuota(getFirstFolder(), 0, 300, 0, 0);
                assertQuota(getWorkspace(), 0, 300, 0, 0);

            }
        });

        // update and create a version
        doUpdateAndVersionContent();
        // ws + 50, file + 280 + version

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                dump();

                DocumentModel firstFile = getFirstFile();
                assertFalse(firstFile.isCheckedOut());
                assertEquals("0.1", firstFile.getVersionLabel());

                assertQuota(firstFile, 380, 760, 0, 380);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 960, 0, 380);
                assertQuota(getFirstFolder(), 0, 960, 0, 380);
                assertQuota(getWorkspace(), 50, 1010, 0, 380);

            }
        });

        // create another version
        doSimpleVersion();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();

                DocumentModel firstFile = getFirstFile();

                assertFalse(firstFile.isCheckedOut());
                assertEquals("0.2", firstFile.getVersionLabel());

                assertQuota(firstFile, 380, 1140, 0, 760);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 1340, 0, 760);
                assertQuota(getFirstFolder(), 0, 1340, 0, 760);
                assertQuota(getWorkspace(), 50, 1390, 0, 760);

            }
        });

        // remove a version
        doRemoveFirstVersion();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();

                DocumentModel firstFile = getFirstFile();

                assertFalse(firstFile.isCheckedOut());
                assertEquals("0.2", firstFile.getVersionLabel());

                assertQuota(firstFile, 380, 760, 0, 380);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 960, 0, 380);
                assertQuota(getFirstFolder(), 0, 960, 0, 380);
                assertQuota(getWorkspace(), 50, 1010, 0, 380);

            }
        });
        // remove doc and associated version

        doRemoveContent();
        eventService.waitForAsyncCompletion();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();

                assertFalse(session.exists(firstFileRef));

                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 200, 0, 0);
                assertQuota(getFirstFolder(), 0, 200, 0, 0);
                assertQuota(getWorkspace(), 50, 250, 0, 0);

            }
        });

    }

    @Test
    public void testQuotaOnMoveContent() throws Exception {
        // Given some content
        addContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                dump();
            }
        });

        // When i Move the content
        doMoveContent();

        // Then the quota ore computed accordingly
        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();

                assertQuota(getFirstFile(), 100L, 100L);
                assertQuota(getSecondFile(), 200L, 200L);
                assertQuota(getFirstSubFolder(), 0L, 200L);
                assertQuota(getSecondSubFolder(), 0L, 100L);
                assertQuota(getFirstFolder(), 0L, 300L);
                assertQuota(getWorkspace(), 0L, 300L);

            }
        });

    }

    @Test
    public void testQuotaOnRemoveContent() throws Exception {

        addContent();
        doRemoveContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();

                assertFalse(session.exists(firstFileRef));

                assertQuota(getSecondFile(), 200L, 200L);
                assertQuota(getFirstSubFolder(), 0L, 200L);
                assertQuota(getFirstFolder(), 0L, 200L);
                assertQuota(getWorkspace(), 0L, 200L);

            }
        });

    }

    @Test
    public void testQuotaOnCopyContent() throws Exception {

        addContent();

        dump();

        doCopyContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();

                DocumentModel copiedFile = session.getChildren(secondSubFolderRef).get(0);

                assertQuota(getFirstFile(), 100L, 100L);
                assertQuota(getSecondFile(), 200L, 200L);
                assertQuota(copiedFile, 100L, 100L);
                assertQuota(getFirstSubFolder(), 0L, 300L);
                assertQuota(getSecondSubFolder(), 0L, 100L);
                assertQuota(getFirstFolder(), 0L, 400L);
                assertQuota(getWorkspace(), 0L, 400L);

            }
        });

    }

    @Test
    public void testQuotaOnCopyFolderishContent() throws Exception {

        addContent();

        dump();

        doCopyFolderishContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();
                DocumentModel copiedFolder = session.getChildren(secondFolderRef).get(0);

                DocumentModel copiedFirstFile = session.getChild(copiedFolder.getRef(), "file1");
                DocumentModel copiedSecondFile = session.getChild(copiedFolder.getRef(), "file2");

                assertQuota(getFirstFile(), 100L, 100L);
                assertQuota(getSecondFile(), 200L, 200L);
                assertQuota(getFirstSubFolder(), 0L, 300L);
                assertQuota(getFirstFolder(), 0L, 300L);
                assertQuota(getSecondFolder(), 0L, 300L);
                assertQuota(copiedFolder, 0L, 300L);
                assertQuota(copiedFirstFile, 100L, 100L);
                assertQuota(copiedSecondFile, 200L, 200L);
                assertQuota(getWorkspace(), 0L, 600L);

            }
        });

    }

    @Test
    public void testQuotaOnRemoveFoldishContent() throws Exception {
        addContent();

        dump();

        doRemoveFolderishContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();
                assertFalse(session.exists(firstSubFolderRef));

                assertQuota(getFirstFolder(), 0L, 0L);
                assertQuota(getWorkspace(), 0L, 0L);

            }
        });

    }

    @Test
    public void testQuotaOnMoveFoldishContent() throws Exception {

        addContent();

        dump();

        doMoveFolderishContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();

                assertEquals(1, session.getChildren(firstFolderRef).size());

                assertQuota(getWorkspace(), 0L, 300L);
                assertQuota(getFirstFolder(), 0L, 0L);
                assertQuota(getSecondFolder(), 0L, 300L);
                assertQuota(getFirstSubFolder(), 0L, 300L);
                assertQuota(getFirstFile(), 100L, 100L);
                assertQuota(getSecondFile(), 200L, 200L);

            }
        });

    }

    @Test
    public void testQuotaExceeded() throws Exception {

        addContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();
                assertQuota(getFirstFile(), 100L, 100L);

                // now add quota limit
                QuotaAware qa = getWorkspace().getAdapter(QuotaAware.class);
                assertNotNull(qa);

                assertEquals(300L, qa.getTotalSize());
                assertEquals(-1L, qa.getMaxQuota());

                // set the quota to 400
                qa.setMaxQuota(400);
                qa.save();
            }
        });

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();

                boolean canNotExceedQuota = false;
                try {
                    // now try to update one
                    DocumentModel firstFile = session.getDocument(firstFileRef);
                    firstFile.setPropertyValue("file:content", (Serializable) getFakeBlob(250));
                    firstFile = session.saveDocument(firstFile);
                } catch (Exception e) {
                    if (QuotaExceededException.isQuotaExceededException(e)) {
                        canNotExceedQuota = true;
                    }
                    TransactionHelper.setTransactionRollbackOnly();
                }
                assertTrue(canNotExceedQuota);
            }
        });

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                assertQuota(getFirstFile(), 100L, 100L);

                // now remove the quota limit
                QuotaAware qa = getWorkspace().getAdapter(QuotaAware.class);
                assertNotNull(qa);

                assertEquals(300L, qa.getTotalSize());
                assertEquals(400L, qa.getMaxQuota());

                // set the quota to -1 / unlimited
                qa.setMaxQuota(-1);
                qa.save();

            }
        });

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                dump();

                boolean canNotExceedQuota = false;
                try {
                    // now try to update one
                    DocumentModel firstFile = session.getDocument(firstFileRef);
                    firstFile.setPropertyValue("file:content", (Serializable) getFakeBlob(250));
                    firstFile = session.saveDocument(firstFile);
                } catch (Exception e) {
                    if (QuotaExceededException.isQuotaExceededException(e)) {
                        canNotExceedQuota = true;
                    }
                    TransactionHelper.setTransactionRollbackOnly();
                }
                assertFalse(canNotExceedQuota);
            }
        });

    }

    @Test
    public void testQuotaExceededAfterDelete() throws Exception {
        addContent();
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                dump();
                assertQuota(getFirstFile(), 100L, 100L);
                // now add quota limit
                QuotaAware qa = getWorkspace().getAdapter(QuotaAware.class);
                assertNotNull(qa);
                assertEquals(300L, qa.getTotalSize());
                assertEquals(-1L, qa.getMaxQuota());
                // set the quota to 300
                qa.setMaxQuota(500);
                qa.save();
            }
        });
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                dump();
                boolean quotaExceeded = false;
                try {

                    DocumentModel doc = session.copy(firstFileRef, firstSubFolderRef, "newCopy");
                    doc.setPropertyValue("file:content", (Serializable) getFakeBlob(100));
                    doc = session.createDocument(doc);
                    session.save();
                } catch (Exception e) {
                    if (QuotaExceededException.isQuotaExceededException(e)) {
                        quotaExceeded = true;
                    }
                    TransactionHelper.setTransactionRollbackOnly();
                }
                assertFalse(quotaExceeded);
            }
        });
        // TODO
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                session.removeChildren(firstFolderRef);
                dump();
            }
        });
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                dump();
                boolean quotaExceeded = false;
                try {
                    DocumentModel doc = session.createDocumentModel("File");
                    doc.setPropertyValue("file:content", (Serializable) getFakeBlob(299));
                    doc.setPropertyValue("dc:title", "Other file");
                    doc.setPathInfo(getWorkspace().getPathAsString(), "otherfile");
                    doc = session.createDocument(doc);
                } catch (Exception e) {
                    if (QuotaExceededException.isQuotaExceededException(e)) {
                        quotaExceeded = true;
                    }
                    TransactionHelper.setTransactionRollbackOnly();
                }
                assertFalse(quotaExceeded);
            }
        });
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                dump();
            }
        });
    }

    @Test
    public void testQuotaExceededMassCopy() throws Exception {
        addContent();
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                dump();
                assertQuota(getFirstFile(), 100L, 100L);
                assertQuota(getSecondFile(), 200L, 200L);
                assertQuota(getFirstSubFolder(), 0L, 300L);
                assertQuota(getFirstFolder(), 0L, 300L);
                assertQuota(getWorkspace(), 0L, 300L);

            }
        });
        final String title = "MassCopyDoc";
        final int nbrDocs = 20;
        final int fileSize = 50;
        final int firstSubFolderExistingFilesNbr = session.getChildren(firstSubFolderRef, "File").size();
        assertEquals(2, firstSubFolderExistingFilesNbr);
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                Blob blob = getFakeBlob(fileSize);
                for (int i = 0; i < nbrDocs; i++) {
                    DocumentModel doc = session.createDocumentModel("File");
                    doc.setPropertyValue("file:content", (Serializable) blob);
                    doc.setPropertyValue("dc:title", title);
                    doc.setPathInfo(getSecondFolder().getPathAsString(), "myfile" + i);
                    doc = session.createDocument(doc);
                }
                eventService.waitForAsyncCompletion();
            }
        });
        final long maxSize = 455L;
        QuotaAware qa = getFirstSubFolder().getAdapter(QuotaAware.class);
        assertNotNull(qa);
        final long firstSubFolderTotalSize = qa.getTotalSize();
        final long expectedNbrDocsCopied = (maxSize-firstSubFolderTotalSize)/fileSize;
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                assertEquals(nbrDocs, session.getChildren(secondFolderRef, "File").size());
                dump();
                QuotaAware qa = getSecondFolder().getAdapter(QuotaAware.class);
                assertEquals(nbrDocs*fileSize, qa.getTotalSize());
                // now add quota limit
                assertEquals(300L, firstSubFolderTotalSize);
                assertEquals(-1L, qa.getMaxQuota());
                // set the quota
                qa = getFirstSubFolder().getAdapter(QuotaAware.class);
                assertNotNull(qa);
                qa.setMaxQuota(maxSize);
                qa.save();
            }
        });
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                QuotaAware qa = getFirstSubFolder().getAdapter(QuotaAware.class);
                assertNotNull(qa);
                assertEquals(maxSize, qa.getMaxQuota());
                DocumentModelList docsToCopy = session.query("SELECT * FROM Document WHERE " + NXQL.ECM_PARENTID + " = '" + getSecondFolder().getId() + "' AND dc:title = '" + title + "'");
                List<DocumentRef> refsToCopy = new ArrayList<DocumentRef>(docsToCopy.size());
                for (DocumentModel doc : docsToCopy) {
                    refsToCopy.add(doc.getRef());
                }
                boolean quotaExceeded = false;
                try {
                    session.move(refsToCopy, firstSubFolderRef);
                } catch (Exception e) {
                    if (QuotaExceededException.isQuotaExceededException(e)) {
                        quotaExceeded = true;
                    }
                    // Rollback all copy operations
                    TransactionHelper.setTransactionRollbackOnly();
                }
                assertTrue(quotaExceeded);
                eventService.waitForAsyncCompletion();
            }
        });
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                QuotaAware qa = getFirstSubFolder().getAdapter(QuotaAware.class);
                assertNotNull(qa);
                assertTrue(qa.getTotalSize() < maxSize);
//                assertEquals(firstSubFolderTotalSize+(expectedNbrDocsCopied*fileSize), qa.getTotalSize());
                assertEquals(firstSubFolderTotalSize, qa.getTotalSize());
                DocumentModelList children = session.getChildren(firstSubFolderRef, "File");
//                assertEquals(firstSubFolderExistingFilesNbr + expectedNbrDocsCopied, children.size());
                assertEquals(firstSubFolderExistingFilesNbr, children.size());
            }
        });
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                DocumentModel doc = session.createDocumentModel("File");
                doc.setPropertyValue("file:content", (Serializable) getFakeBlob(50));
                doc.setPropertyValue("dc:title", "Other file");
                doc.setPathInfo(getFirstSubFolder().getPathAsString(), "otherfile");
                boolean quotaExceeded = false;
                try {
                    doc = session.createDocument(doc);
                } catch (Exception e) {
                    if (QuotaExceededException.isQuotaExceededException(e)) {
                        quotaExceeded = true;
                    }
                    TransactionHelper.setTransactionRollbackOnly();
                }
                assertFalse(quotaExceeded);
                eventService.waitForAsyncCompletion();
            }
        });
    }

    @Test
    public void testQuotaExceededOnVersion() throws Exception {

        addContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                // now add quota limit
                DocumentModel ws = session.getDocument(wsRef);
                QuotaAware qa = ws.getAdapter(QuotaAware.class);
                assertNotNull(qa);

                assertEquals(300L, qa.getTotalSize());

                assertEquals(-1L, qa.getMaxQuota());

                // set the quota to 350
                qa.setMaxQuota(350);
                qa.save();

            }
        });

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                dump();

                // create a version
                DocumentModel firstFile = session.getDocument(firstFileRef);
                firstFile.checkIn(VersioningOption.MINOR, null);

                boolean canNotExceedQuota = false;
                try {
                    // now try to checkout
                    firstFile.checkOut();
                } catch (Exception e) {
                    if (QuotaExceededException.isQuotaExceededException(e)) {
                        canNotExceedQuota = true;
                    }
                    TransactionHelper.setTransactionRollbackOnly();
                }

                dump();

                assertTrue(canNotExceedQuota);
            }
        });

    }

    @Test
    public void testComputeInitialStatistics() throws Exception {

        EventServiceAdmin eventAdmin = Framework.getService(EventServiceAdmin.class);
        eventAdmin.setListenerEnabledFlag("quotaStatsListener", false);

        addContent();
        doCheckIn();
        doDeleteFileContent(secondFileRef);

        dump();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                DocumentModel ws = session.getDocument(wsRef);
                assertFalse(ws.hasFacet(QuotaAwareDocument.DOCUMENTS_SIZE_STATISTICS_FACET));

                String updaterName = "documentsSizeUpdater";
                quotaStatsService.launchInitialStatisticsComputation(updaterName, session.getRepositoryName());
                String queueId = workManager.getCategoryQueueId(QuotaStatsInitialWork.CATEGORY_QUOTA_INITIAL);

                workManager.awaitCompletion(queueId, 10, TimeUnit.SECONDS);

            }
        });

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();

                assertQuota(getFirstFile(), 100L, 200L, 0L, 100L);
                assertQuota(getSecondFile(), 200L, 200L, 200L, 0L);
                assertQuota(getFirstSubFolder(), 0L, 400L, 200L, 100L);
                assertQuota(getFirstFolder(), 0L, 400L, 200L, 100L);
                assertQuota(getWorkspace(), 0L, 400L, 200L, 100L);

            }
        });

        eventAdmin.setListenerEnabledFlag("quotaStatsListener", true);

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                DocumentModel firstFile = getFirstFile();
                // modify file to checkout
                firstFile.setPropertyValue("dc:title", "File1");
                firstFile = session.saveDocument(firstFile);
                session.save(); // process invalidations
                assertTrue(firstFile.isCheckedOut());
            }
        });

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                assertQuota(getFirstFile(), 100L, 200L, 0L, 100L);
                assertQuota(getSecondFile(), 200L, 200L, 200L, 0L);
                assertQuota(getFirstSubFolder(), 0L, 400L, 200L, 100L);
                assertQuota(getFirstFolder(), 0L, 400L, 200L, 100L);
                assertQuota(getWorkspace(), 0L, 400L, 200L, 100L);

            }
        });
    }

    @Test
    public void testComputeInitialStatisticsAfterFileMovedToTrash() throws Exception {

        EventServiceAdmin eventAdmin = Framework.getService(EventServiceAdmin.class);
        eventAdmin.setListenerEnabledFlag("quotaStatsListener", true);

        addContent(true);
        doDeleteFileContent(firstFileRef);

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                dump();
                assertQuota(getFirstFile(), 100L, 200L, 100L, 100L);
                assertQuota(getSecondFile(), 200L, 200L, 0L, 0L);
                assertQuota(getFirstSubFolder(), 0L, 400L, 100L, 100L);
                assertQuota(getFirstFolder(), 0L, 400L, 100L, 100L);
                assertQuota(getWorkspace(), 0L, 400L, 100L, 100L);
            }
        });
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                eventService.waitForAsyncCompletion();

                String updaterName = "documentsSizeUpdater";
                quotaStatsService.launchInitialStatisticsComputation(updaterName, session.getRepositoryName());
                String queueId = workManager.getCategoryQueueId(QuotaStatsInitialWork.CATEGORY_QUOTA_INITIAL);

                workManager.awaitCompletion(queueId, 10, TimeUnit.SECONDS);

            }
        });
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                dump();
                assertQuota(getFirstFile(), 100L, 200L, 100L, 100L);
                assertQuota(getSecondFile(), 200L, 200L, 0L, 0L);
                assertQuota(getFirstSubFolder(), 0L, 400L, 100L, 100L);
                assertQuota(getFirstFolder(), 0L, 400L, 100L, 100L);
                assertQuota(getWorkspace(), 0L, 400L, 100L, 100L);
            }
        });
    }

    @Test
    public void testQuotaOnDeleteContent() throws Exception {
        addContent();
        doDeleteFileContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                DocumentModel ws = session.getDocument(wsRef);
                DocumentModel firstFolder = session.getDocument(firstFolderRef);
                DocumentModel firstFile = session.getDocument(firstFileRef);

                assertQuota(firstFile, 100L, 100L, 100L);
                assertQuota(firstFolder, 0L, 300L, 100L);
                assertQuota(ws, 0L, 300L, 100L);

            }
        });

        doUndeleteFileContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                assertQuota(getFirstFile(), 100L, 100L, 0L);
                assertQuota(getFirstFolder(), 0L, 300L, 0L);
                assertQuota(getWorkspace(), 0L, 300L, 0L);

            }
        });

        // sent file to trash
        doDeleteFileContent();
        // then permanently delete file when file is in trash
        doRemoveContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                assertFalse(session.exists(firstFileRef));
                assertQuota(getFirstFolder(), 0L, 200L, 0L);
                assertQuota(getWorkspace(), 0L, 200L, 0L);

            }
        });
    }

    @Test
    public void testQuotaOnDeleteVersion() throws Exception {
        addContent();
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {

                dump();

                assertQuota(getFirstFile(), 100, 100, 0, 0);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 300, 0, 0);
                assertQuota(getFirstFolder(), 0, 300, 0, 0);
                assertQuota(getWorkspace(), 0, 300, 0, 0);

            }
        });
        // update and create a version
        doUpdateAndVersionContent();
        // ws + 50, file + 280 + version
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {

                dump();

                DocumentModel firstFile = getFirstFile();
                assertFalse(firstFile.isCheckedOut());
                assertEquals("0.1", firstFile.getVersionLabel());

                assertQuota(firstFile, 380, 760, 0, 380);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 960, 0, 380);
                assertQuota(getFirstFolder(), 0, 960, 0, 380);
                assertQuota(getWorkspace(), 50, 1010, 0, 380);
            }
        });
        doDeleteFileContent(firstFileRef);
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                dump();
                assertQuota(getFirstFile(), 380, 760, 380, 380);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 960, 380, 380);
                assertQuota(getFirstFolder(), 0, 960, 380, 380);
                assertQuota(getWorkspace(), 50, 1010, 380, 380);
            }
        });
        doRemoveContent();
        eventService.waitForAsyncCompletion();
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                dump();
                assertFalse(session.exists(firstFileRef));
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 200, 0, 0);
                assertQuota(getFirstFolder(), 0, 200, 0, 0);
                assertQuota(getWorkspace(), 50, 250, 0, 0);
            }
        });
    }

    /**
     * NXP-17350
     * @throws Exception
     * @since TODO
     */
    @Test
    public void testQuotaOnDeleteFolder() throws Exception {
        addContent();
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                dump();
                assertQuota(getFirstFile(), 100, 100, 0, 0);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 300, 0, 0);
                assertQuota(getFirstFolder(), 0, 300, 0, 0);
                assertQuota(getWorkspace(), 0, 300, 0, 0);
            }
        });
        // update and create a version
        doUpdateAndVersionContent();
        // ws + 50, file + 280 + version
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                dump();
                DocumentModel firstFile = getFirstFile();
                assertFalse(firstFile.isCheckedOut());
                assertEquals("0.1", firstFile.getVersionLabel());

                assertQuota(firstFile, 380, 760, 0, 380);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 960, 0, 380);
                assertQuota(getFirstFolder(), 0, 960, 0, 380);
                assertQuota(getWorkspace(), 50, 1010, 0, 380);
            }
        });
        doDeleteFileContent(firstSubFolderRef);
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                dump();
                // inner, total, trash, versions
                assertQuota(getFirstFile(), 380, 760, 380, 380);
                assertQuota(getSecondFile(), 200, 200, 200, 0);
                assertQuota(getFirstSubFolder(), 0, 960, 580, 380);
                assertQuota(getFirstFolder(), 0, 960, 580, 380);
                assertQuota(getWorkspace(), 50, 1010, 580, 380);
            }
        });
    }

    @Test
    public void testQuotaOnMoveContentWithVersions() throws Exception {

        addContent();
        // update and create a version
        doUpdateAndVersionContent();
        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                assertQuota(getFirstFile(), 380, 760, 0, 380);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 960, 0, 380);
                assertQuota(getFirstFolder(), 0, 960, 0, 380);
                assertQuota(getWorkspace(), 50, 1010, 0, 380);

            }
        });

        doMoveFileContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                assertQuota(getFirstFile(), 380, 760, 0, 380);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 200, 0, 0);
                assertQuota(getFirstFolder(), 0, 200, 0, 0);
                assertQuota(getSecondFolder(), 0, 760, 0, 380);
                assertQuota(getWorkspace(), 50, 1010, 0, 380);

            }
        });

    }

    @Test
    public void testQuotaOnCopyContentWithVersions() throws Exception {

        addContent();
        // update and create a version
        doUpdateAndVersionContent();
        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();

                assertQuota(getFirstFile(), 380, 760, 0, 380);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 960, 0, 380);
                assertQuota(getFirstFolder(), 0, 960, 0, 380);
                assertQuota(getWorkspace(), 50, 1010, 0, 380);

            }
        });

        doCopyContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();
                DocumentModel copiedFile = session.getChildren(secondSubFolderRef).get(0);

                assertQuota(getFirstFile(), 380, 760, 0, 380);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 960, 0, 380);
                assertQuota(getFirstFolder(), 0, 1340, 0, 380);
                assertQuota(copiedFile, 380, 380, 0, 0);
                assertQuota(getSecondSubFolder(), 0, 380, 0, 0);
                assertQuota(getWorkspace(), 50, 1390, 0, 380);

            }
        });
    }

    @Test
    public void testAllowSettingMaxQuota() throws Exception {
        addContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                // now add quota limit
                DocumentModel ws = session.getDocument(wsRef);
                QuotaAware qa = ws.getAdapter(QuotaAware.class);
                assertNotNull(qa);
                // set the quota to 400
                qa.setMaxQuota(400);
                qa.save();

                DocumentModel firstSubFolder = session.getDocument(firstSubFolderRef);
                QuotaAware qaFSF = firstSubFolder.getAdapter(QuotaAware.class);
                qaFSF.setMaxQuota(200);
                qaFSF.save();

            }
        });

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                DocumentModel secondFolder = session.getDocument(secondFolderRef);
                QuotaAware qaSecFolder = secondFolder.getAdapter(QuotaAware.class);
                try {
                    qaSecFolder.setMaxQuota(300);
                    fail();
                } catch (QuotaExceededException e) {
                    // ok
                }
                qaSecFolder.setMaxQuota(200);
                qaSecFolder.save();
            }
        });

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                QuotaAware qaSecFolder = getSecondFolder().getAdapter(QuotaAware.class);
                assertEquals(200L, qaSecFolder.getMaxQuota());

                QuotaAware qaFirstFolder = getFirstFolder().getAdapter(QuotaAware.class);

                try {
                    qaFirstFolder.setMaxQuota(50);
                    fail();
                } catch (QuotaExceededException e) {
                    // ok
                }

                QuotaAware qaSecSubFolder = getSecondSubFolder().getAdapter(QuotaAware.class);
                try {
                    qaSecSubFolder.setMaxQuota(50);
                    fail();
                } catch (QuotaExceededException e) {
                    // ok
                }
            }
        });
    }

    @Test
    public void testQuotaOnRevertVersion() throws Exception {

        addContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();

                assertQuota(getFirstFile(), 100, 100, 0, 0);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 300, 0, 0);
                assertQuota(getFirstFolder(), 0, 300, 0, 0);
                assertQuota(getWorkspace(), 0, 300, 0, 0);

            }
        });

        // update and create a version
        doUpdateAndVersionContent();
        // ws + 50, file + 280 + version

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();

                DocumentModel firstFile = getFirstFile();
                assertFalse(firstFile.isCheckedOut());
                assertEquals("0.1", firstFile.getVersionLabel());

                assertQuota(firstFile, 380, 760, 0, 380);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 960, 0, 380);
                assertQuota(getFirstFolder(), 0, 960, 0, 380);
                assertQuota(getWorkspace(), 50, 1010, 0, 380);

            }
        });
        // create another version
        doSimpleVersion();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                DocumentModel firstFile = getFirstFile();
                assertFalse(firstFile.isCheckedOut());
                assertEquals("0.2", firstFile.getVersionLabel());

                assertQuota(firstFile, 380, 1140, 0, 760);
                assertQuota(getSecondFile(), 200, 200, 0, 0);
                assertQuota(getFirstSubFolder(), 0, 1340, 0, 760);
                assertQuota(getFirstFolder(), 0, 1340, 0, 760);
                assertQuota(getWorkspace(), 50, 1390, 0, 760);

                List<DocumentModel> versions = session.getVersions(firstFileRef);
                for (DocumentModel documentModel : versions) {
                    if ("0.1".equals(documentModel.getVersionLabel())) {
                        firstFile = session.restoreToVersion(firstFileRef, documentModel.getRef(), true, true);
                    }
                }
                firstFileRef = firstFile.getRef();
            }
        });

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                dump();

                DocumentModel firstFile = getFirstFile();
                assertFalse(firstFile.isCheckedOut());
                assertEquals("0.1", firstFile.getVersionLabel());

                assertQuota(firstFile, 380, 1140, 0, 760);
                assertQuota(getFirstSubFolder(), 0, 1340, 0, 760);
                assertQuota(getFirstFolder(), 0, 1340, 0, 760);
                assertQuota(getWorkspace(), 50, 1390, 0, 760);

            }
        });
    }

    @Test
    public void testAllowSettingMaxQuotaOnUserWorkspace() throws Exception {
        addContent();

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                try (CoreSession userSession = coreFeature.openCoreSession("toto")) {
                    DocumentModel uw = uwm.getCurrentUserPersonalWorkspace(userSession, null);
                    assertNotNull(uw);
                    userSession.save();
                }
                try (CoreSession userSession = coreFeature.openCoreSession("titi")) {
                    DocumentModel uw = uwm.getCurrentUserPersonalWorkspace(userSession, null);
                    assertNotNull(uw);
                    userSession.save();
                }
                quotaStatsService.activateQuotaOnUserWorkspaces(300L, session);
                quotaStatsService.launchSetMaxQuotaOnUserWorkspaces(300L, session.getRootDocument(), session);

            }
        });

        workManager.awaitCompletion("quota", 3, TimeUnit.SECONDS);
        assertEquals(0, workManager.getQueueSize("quota", null));

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {
                DocumentModel totoUW = uwm.getUserPersonalWorkspace("toto",
                        session.getDocument(new PathRef("/default-domain/")));
                QuotaAware totoUWQuota = totoUW.getAdapter(QuotaAware.class);
                assertEquals(300L, totoUWQuota.getMaxQuota());
                DocumentModel titiUW = uwm.getUserPersonalWorkspace("titi",
                        session.getDocument(new PathRef("/default-domain/")));
                QuotaAware titiUWQuota = titiUW.getAdapter(QuotaAware.class);
                assertEquals(300L, titiUWQuota.getMaxQuota());

                // create content in the 2 user workspaces
                DocumentModel firstFile = session.createDocumentModel(totoUW.getPathAsString(), "file1", "File");
                firstFile.setPropertyValue("file:content", (Serializable) getFakeBlob(200));
                firstFile = session.createDocument(firstFile);
                firstFile = session.saveDocument(firstFile);

                DocumentModel secondFile = session.createDocumentModel(titiUW.getPathAsString(), "file2", "File");
                secondFile.setPropertyValue("file:content", (Serializable) getFakeBlob(200));
                secondFile = session.createDocument(secondFile);
                secondFile = session.saveDocument(secondFile);

            }
        });

        isr.run(new RunnableWithException() {

            @Override
            public void run() throws Exception {

                DocumentModel totoUW = uwm.getUserPersonalWorkspace("toto",
                        session.getDocument(new PathRef("/default-domain/")));
                QuotaAware totoUWQuota = totoUW.getAdapter(QuotaAware.class);
                assertEquals(200L, totoUWQuota.getTotalSize());
                DocumentModel titiUW = uwm.getUserPersonalWorkspace("titi",
                        session.getDocument(new PathRef("/default-domain/")));
                QuotaAware titiUWQuota = titiUW.getAdapter(QuotaAware.class);
                assertEquals(200L, titiUWQuota.getTotalSize());

                boolean canAddContent = true;
                try {
                    DocumentModel secondFile = session.createDocumentModel(titiUW.getPathAsString(), "file2", "File");
                    secondFile.setPropertyValue("file:content", (Serializable) getFakeBlob(200));
                    secondFile = session.createDocument(secondFile);
                    secondFile = session.saveDocument(secondFile);
                } catch (Exception e) {
                    if (e.getCause() instanceof QuotaExceededException) {
                        canAddContent = false;
                    }
                }
                assertFalse(canAddContent);

                canAddContent = true;
                try {
                    DocumentModel firstFile = session.createDocumentModel(totoUW.getPathAsString(), "file1", "File");
                    firstFile.setPropertyValue("file:content", (Serializable) getFakeBlob(200));
                    firstFile = session.createDocument(firstFile);
                    firstFile = session.saveDocument(firstFile);
                } catch (Exception e) {
                    if (e.getCause() instanceof QuotaExceededException) {
                        canAddContent = false;
                    }
                }
                assertFalse(canAddContent);
            }
        });
    }

    protected Blob getFakeBlob(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append('a');
        }
        Blob blob = Blobs.createBlob(sb.toString());
        blob.setFilename("FakeBlob_" + size + ".txt");
        return blob;
    }

    protected void addContent() throws Exception {
        addContent(false);
    }

    protected void addContent(final boolean checkInFirstFile) throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                DocumentModel ws = session.createDocumentModel("/", "ws", "Workspace");
                ws = session.createDocument(ws);
                ws = session.saveDocument(ws);
                wsRef = ws.getRef();

                DocumentModel firstFolder = session.createDocumentModel(ws.getPathAsString(), "folder1", "Folder");
                firstFolder = session.createDocument(firstFolder);
                firstFolderRef = firstFolder.getRef();

                DocumentModel firstSubFolder = session.createDocumentModel(firstFolder.getPathAsString(), "subfolder1",
                        "Folder");
                firstSubFolder = session.createDocument(firstSubFolder);

                firstSubFolderRef = firstSubFolder.getRef();

                DocumentModel firstFile = session.createDocumentModel(firstSubFolder.getPathAsString(), "file1", "File");
                firstFile.setPropertyValue("file:content", (Serializable) getFakeBlob(100));
                firstFile = session.createDocument(firstFile);
                if (checkInFirstFile) {
                    firstFile.checkIn(VersioningOption.MINOR, null);
                }

                firstFileRef = firstFile.getRef();

                DocumentModel secondFile = session.createDocumentModel(firstSubFolder.getPathAsString(), "file2",
                        "File");
                secondFile.setPropertyValue("file:content", (Serializable) getFakeBlob(200));

                secondFile = session.createDocument(secondFile);
                secondFileRef = secondFile.getRef();

                DocumentModel secondSubFolder = session.createDocumentModel(firstFolder.getPathAsString(),
                        "subfolder2", "Folder");
                secondSubFolder = session.createDocument(secondSubFolder);
                secondSubFolderRef = secondSubFolder.getRef();

                DocumentModel secondFolder = session.createDocumentModel(ws.getPathAsString(), "folder2", "Folder");
                secondFolder = session.createDocument(secondFolder);
                secondFolderRef = secondFolder.getRef();
            }
        });
    }

    protected void doMoveContent() throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                session.move(firstFileRef, secondSubFolderRef, null);
            }
        });
    }

    protected void doMoveFolderishContent() throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                session.move(firstSubFolderRef, secondFolderRef, null);
            }
        });
    }

    protected void doMoveFileContent() throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                session.move(firstFileRef, secondFolderRef, null);
            }
        });
    }

    protected void doUpdateContent() throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                DocumentModel ws = session.getDocument(wsRef);
                DocumentModel firstFile = session.getDocument(firstFileRef);

                ws.setPropertyValue("file:content", (Serializable) getFakeBlob(50));
                ws = session.saveDocument(ws);

                List<Map<String, Serializable>> files = new ArrayList<Map<String, Serializable>>();

                for (int i = 1; i < 5; i++) {
                    Map<String, Serializable> files_entry = new HashMap<String, Serializable>();
                    files_entry.put("file", (Serializable) getFakeBlob(70));
                    files.add(files_entry);
                }

                firstFile.setPropertyValue("files:files", (Serializable) files);
                firstFile = session.saveDocument(firstFile);
            }
        });

    }

    protected void doCheckIn() throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                DocumentModel firstFile = session.getDocument(firstFileRef);
                firstFile.checkIn(VersioningOption.MINOR, null);
            }
        });
    }

    protected void doCheckOut() throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {

                DocumentModel firstFile = session.getDocument(firstFileRef);
                firstFile.checkOut();
            }
        });
    }

    protected void doUpdateAndVersionContent() throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {

                DocumentModel ws = session.getDocument(wsRef);
                DocumentModel firstFile = session.getDocument(firstFileRef);

                ws.setPropertyValue("file:content", (Serializable) getFakeBlob(50));
                ws = session.saveDocument(ws);

                List<Map<String, Serializable>> files = new ArrayList<Map<String, Serializable>>();

                for (int i = 1; i < 5; i++) {
                    Map<String, Serializable> files_entry = new HashMap<String, Serializable>();
                    files_entry.put("file", (Serializable) getFakeBlob(70));
                    files.add(files_entry);
                }

                firstFile.setPropertyValue("files:files", (Serializable) files);
                // create minor version
                firstFile.putContextData(VersioningService.VERSIONING_OPTION, VersioningOption.MINOR);
                firstFile = session.saveDocument(firstFile);
            }
        });
    }

    protected void doSimpleVersion() throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {

                DocumentModel firstFile = session.getDocument(firstFileRef);

                firstFile.setPropertyValue("dc:title", "a version");
                firstFile.putContextData(VersioningService.VERSIONING_OPTION, VersioningOption.MINOR);
                firstFile = session.saveDocument(firstFile);
            }
        });
    }

    protected void doRemoveFirstVersion() throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {

                List<DocumentModel> versions = session.getVersions(firstFileRef);

                session.removeDocument(versions.get(0).getRef());
            }
        });
    }

    protected void doRemoveContent() throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                session.removeDocument(firstFileRef);
            }
        });
    }

    protected void doRemoveFolderishContent() throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                session.removeDocument(firstSubFolderRef);
            }
        });
    }

    protected void doDeleteFileContent() throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {

                List<DocumentModel> docs = new ArrayList<DocumentModel>();
                docs.add(session.getDocument(firstFileRef));
                Framework.getService(TrashService.class).trashDocuments(docs);
            }
        });
    }

    protected void doDeleteFileContent(final DocumentRef fileRef) throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {

                List<DocumentModel> docs = new ArrayList<DocumentModel>();
                docs.add(session.getDocument(fileRef));
                Framework.getService(TrashService.class).trashDocuments(docs);
            }
        });
    }

    protected void doUndeleteFileContent() throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {

                List<DocumentModel> docs = new ArrayList<DocumentModel>();
                docs.add(session.getDocument(firstFileRef));
                Framework.getService(TrashService.class).undeleteDocuments(docs);
            }
        });
    }

    protected void doCopyContent() throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {
                session.copy(firstFileRef, secondSubFolderRef, null);
            }
        });
    }

    protected void doCopyFolderishContent() throws Exception {
        isr.run(new RunnableWithException() {
            @Override
            public void run() throws Exception {

                session.copy(firstSubFolderRef, secondFolderRef, null);
            }
        });
    }

    protected void dump() throws Exception {
        if (Boolean.TRUE.booleanValue()) {
            return;
        }
        System.out.println("\n####################################\n");
        DocumentModelList docs = session.query("select * from Document order by ecm:path");
        for (DocumentModel doc : docs) {
            if (doc.isVersion()) {
                System.out.print(" --version ");
            }
            System.out.print(doc.getId() + " " + doc.getPathAsString());
            if (doc.hasSchema("uid")) {
                System.out.print(" (" + doc.getPropertyValue("uid:major_version") + "."
                        + doc.getPropertyValue("uid:minor_version") + ")");
            }

            if (doc.hasFacet(DOCUMENTS_SIZE_STATISTICS_FACET)) {
                QuotaAware qa = doc.getAdapter(QuotaAware.class);
                System.out.println( " " + qa.getQuotaInfo());
                // System.out.println(" with Quota facet");
            } else {
                System.out.println(" no Quota facet !!!");
            }
        }

    }

    protected void assertQuota(DocumentModel doc, long innerSize, long totalSize) {
        assertTrue(doc.hasFacet(DOCUMENTS_SIZE_STATISTICS_FACET));
        QuotaAware qa = doc.getAdapter(QuotaAware.class);
        assertNotNull(qa);
        assertEquals("inner:", innerSize, qa.getInnerSize());
        assertEquals("total:", totalSize, qa.getTotalSize());
    }

    protected void assertQuota(DocumentModel doc, long innerSize, long totalSize, long trashSize) {
        QuotaAware qa = doc.getAdapter(QuotaAware.class);
        assertNotNull(qa);
        assertEquals("inner:", innerSize, qa.getInnerSize());
        assertEquals("total:", totalSize, qa.getTotalSize());
        assertEquals("trash:", trashSize, qa.getTrashSize());
    }

    protected void assertQuota(DocumentModel doc, long innerSize, long totalSize, long trashSize, long versionsSize) {
        QuotaAware qa = doc.getAdapter(QuotaAware.class);
        assertNotNull(qa);
        assertEquals("inner:", innerSize, qa.getInnerSize());
        assertEquals("total:", totalSize, qa.getTotalSize());
        assertEquals("trash:", trashSize, qa.getTrashSize());
        assertEquals("versions: ", versionsSize, qa.getVersionsSize());
    }

    /**
     * @return
     */
    protected DocumentModel getWorkspace() {
        return session.getDocument(wsRef);
    }

    /**
     * @return
     */
    protected DocumentModel getFirstSubFolder() {
        return session.getDocument(firstSubFolderRef);
    }

    /**
     * @return
     */
    protected DocumentModel getSecondSubFolder() {
        return session.getDocument(secondSubFolderRef);
    }

    /**
     * @return
     */
    protected DocumentModel getFirstFolder() {
        return session.getDocument(firstFolderRef);
    }

    /**
     * @return
     */
    protected DocumentModel getSecondFolder() {
        return session.getDocument(secondFolderRef);
    }

    /**
     * @return
     */
    protected DocumentModel getSecondFile() {
        return session.getDocument(secondFileRef);
    }

    /**
     * @return
     */
    protected DocumentModel getFirstFile() {
        return session.getDocument(firstFileRef);
    }

}
