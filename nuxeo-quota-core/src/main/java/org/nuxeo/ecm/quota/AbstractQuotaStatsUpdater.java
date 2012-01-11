/*
 * (C) Copyright 2006-2012 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     Thomas Roger <troger@nuxeo.com>
 */

package org.nuxeo.ecm.quota;

import static org.nuxeo.ecm.core.api.event.DocumentEventTypes.ABOUT_TO_REMOVE;
import static org.nuxeo.ecm.core.api.event.DocumentEventTypes.DOCUMENT_CREATED;
import static org.nuxeo.ecm.core.api.event.DocumentEventTypes.DOCUMENT_CREATED_BY_COPY;
import static org.nuxeo.ecm.core.api.event.DocumentEventTypes.DOCUMENT_MOVED;
import static org.nuxeo.ecm.core.api.event.DocumentEventTypes.DOCUMENT_UPDATED;

import java.util.ArrayList;
import java.util.List;

import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentRef;
import org.nuxeo.ecm.core.api.event.CoreEventConstants;
import org.nuxeo.ecm.core.event.impl.DocumentEventContext;

/**
 * Abstract class implementing {@code QuotaStatsUpdater} to handle common cases.
 * <p>
 * Provides abstract methods to override for common events.
 *
 * @author <a href="mailto:troger@nuxeo.com">Thomas Roger</a>
 * @since 5.5
 */
public abstract class AbstractQuotaStatsUpdater implements QuotaStatsUpdater {

    protected String name;

    protected String label;

    protected String descriptionLabel;

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public void setDescriptionLabel(String descriptionLabel) {
        this.descriptionLabel = descriptionLabel;
    }

    @Override
    public String getDescriptionLabel() {
        return descriptionLabel;
    }

    @Override
    public void updateStatistics(CoreSession session,
            DocumentEventContext docCtx, String eventName)
            throws ClientException {
        DocumentModel doc = docCtx.getSourceDocument();
        if (DOCUMENT_CREATED.equals(eventName)) {
            processDocumentCreated(session, doc);
        } else if (ABOUT_TO_REMOVE.equals(eventName)) {
            processDocumentAboutToBeRemoved(session, doc);
        } else if (DOCUMENT_CREATED_BY_COPY.equals(eventName)) {
            processDocumentCopied(session, doc);
        } else if (DOCUMENT_MOVED.equals(eventName)) {
            DocumentRef sourceParentRef = (DocumentRef) docCtx.getProperty(CoreEventConstants.PARENT_PATH);
            DocumentModel sourceParent = session.getDocument(sourceParentRef);
            processDocumentMoved(session, doc, sourceParent);
        } else if (DOCUMENT_UPDATED.equals(eventName)) {
            processDocumentUpdated(session, doc);
        }
    }

    protected List<DocumentModel> getAncestors(CoreSession session,
            DocumentModel doc) throws ClientException {
        List<DocumentModel> ancestors = new ArrayList<DocumentModel>();
        if (doc != null) {
            doc = session.getDocument(doc.getParentRef());
            while (doc != null && !doc.getPath().isRoot()) {
                ancestors.add(doc);
                doc = session.getDocument(doc.getParentRef());
            }
        }
        return ancestors;

    }

    protected abstract void processDocumentCreated(CoreSession session,
            DocumentModel doc) throws ClientException;

    protected abstract void processDocumentCopied(CoreSession session,
            DocumentModel doc) throws ClientException;

    protected abstract void processDocumentUpdated(CoreSession session,
            DocumentModel doc);

    protected abstract void processDocumentMoved(CoreSession session,
            DocumentModel doc, DocumentModel sourceParent)
            throws ClientException;

    protected abstract void processDocumentAboutToBeRemoved(
            CoreSession session, DocumentModel doc) throws ClientException;
}
