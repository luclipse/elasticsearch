/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.settings;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A response for a update settings action.
 */
public class UpdateSettingsResponse extends ActionResponse {

    private Map<String, Settings> updatedSettings;

    UpdateSettingsResponse() {
    }

    void setUpdateSettings(Map<String, Settings> updatedSettings) {
        this.updatedSettings = updatedSettings;
    }

    public Map<String, Settings> getUpdatedSettings() {
        return updatedSettings;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        updatedSettings = new HashMap<String, Settings>(size);
        for (int i = 0; i < size; i++) {
            String index = in.readString();
            Settings settings = ImmutableSettings.readSettingsFromStream(in);
            updatedSettings.put(index, settings);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(updatedSettings.size());
        for (Map.Entry<String, Settings> entry : updatedSettings.entrySet()) {
            out.writeString(entry.getKey());
            ImmutableSettings.writeSettingsToStream(entry.getValue(), out);
        }
    }
}
