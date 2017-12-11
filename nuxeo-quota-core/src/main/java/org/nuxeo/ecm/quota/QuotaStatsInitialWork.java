/*
 * (C) Copyright 2006-2012 Nuxeo SA (http://nuxeo.com/) and others.
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
 *     Florent Guillaume
 */

package org.nuxeo.ecm.quota;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.nuxeo.ecm.core.api.UnrestrictedSessionRunner;
import org.nuxeo.ecm.core.work.AbstractWork;
import org.nuxeo.runtime.api.Framework;

/**
 * Work doing an initial statistics computation for a defined {@link QuotaStatsUpdater}.
 *
 * @since 5.6
 */
public class QuotaStatsInitialWork extends AbstractWork {

    private static final long serialVersionUID = 1L;

    public static final String CATEGORY_QUOTA_INITIAL = "quotaInitialStatistics";

    private final String updaterName;

    private final String docPath;

    public QuotaStatsInitialWork(String updaterName, String repositoryName, String docPath) {
        super(repositoryName + ":quotaInitialStatistics:" + updaterName);
        setDocument(repositoryName, null);
        this.updaterName = updaterName;
        this.docPath = docPath;
    }

    @Override
    public String getCategory() {
        return CATEGORY_QUOTA_INITIAL;
    }

    @Override
    public String getTitle() {
        return "Quota Statistics " + updaterName;
    }

    public void notifyProgress(float percent) {
        setProgress(new Progress(percent));
    }

    public void notifyProgress(long current, long total) {
        setProgress(new Progress(current, total));
    }

    @Override
    public void work() {
        final QuotaStatsInitialWork currentWorker = this;
        new UnrestrictedSessionRunner(repositoryName) {
            @Override
            public void run() {
            	QuotaStatsService service = Framework.getService(QuotaStatsService.class);
                if(docPath == null) {
                	service.computeInitialStatistics(updaterName, session, currentWorker);
                }
                else {
                	service.computeInitialStatistics(updaterName, session, currentWorker, docPath);
                }
            }
        }.runUnrestricted();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("updaterName", updaterName).append("repositoryName", repositoryName).toString();
    }

}
