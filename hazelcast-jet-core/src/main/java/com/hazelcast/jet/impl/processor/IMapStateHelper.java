/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.config.JetConfig;

import java.text.MessageFormat;
import java.util.UUID;

public final class IMapStateHelper {
    // Name of IMap containing all live state Imap names, where key = vertex name and value = imap name
    public static final String VERTEX_TO_LIVE_STATE_IMAP_NAME = "statemapnames";
    // Name of IMap containing all snapshot state Imap names, where key = vertex name and value = imap name
    public static final String VERTEX_TO_SS_STATE_IMAP_NAME = "snapshotmapnames";
    // Boolean which controls if IMap state is used or not, use as global toggle for IMap state
    private static boolean enableImapState;
    // Used to keep track if imap state is cached
    private static boolean enableImapStateCached;

    // Private constructor to prevent instantiation
    private IMapStateHelper() {

    }

    /**
     * Helper method that gets whether the IMap state is enabled.
     *
     * @param config The JetConfig where the "enable.imap.state" property should be "true" of "false"
     * @return True if the config says true, false if it says false.
     */
    public static boolean getEnableImapState(JetConfig config) {
        if (!enableImapStateCached) {
            enableImapState = Boolean.parseBoolean(config.getProperties().getProperty("enable.imap.state"));
            enableImapStateCached = true;
        }
        return enableImapState;
    }

    public static String getBenchmarkIMapTimesListName(String jobName) {
        return String.format("benchmark-imap-%s", jobName);
    }

    public static String getBenchmarkPhase1TimesListName(String jobName) {
        return String.format("benchmark-phase1-%s", jobName);
    }

    public static String getBenchmarkPhase2TimesListName(String jobName) {
        return String.format("benchmark-phase2-%s", jobName);
    }

    public static String getSnapshotMapName(String liveMapName) {
        return MessageFormat.format("snapshot-{0}", liveMapName);
    }

    public static String getSnapshotIdName(String jobName) {
        return MessageFormat.format("ssid-{0}", jobName);
    }

    public static String clusterCountdownLatchHelper(String jobName) {
        return "cdl-" + jobName;
    }

    public static String memberCountdownLatchHelper(UUID memberName, String jobName) {
        return String.format("cdl-%s-%s", memberName.toString(), jobName);
    }
}
