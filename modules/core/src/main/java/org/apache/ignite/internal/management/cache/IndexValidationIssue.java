/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.management.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class IndexValidationIssue extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Key. */
    private String key;

    /** Cache name. */
    private String cacheName;

    /** Index name. */
    private String idxName;

    /** T. */
    @GridToStringExclude
    private Throwable t;

    /**
     *
     */
    public IndexValidationIssue() {
        // Default constructor required for Externalizable.
    }

    /**
     * @param key Key.
     * @param cacheName Cache name.
     * @param idxName Index name.
     * @param t T.
     */
    public IndexValidationIssue(String key, String cacheName, String idxName, Throwable t) {
        this.key = key;
        this.cacheName = cacheName;
        this.idxName = idxName;
        this.t = t;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, key);
        U.writeString(out, cacheName);
        U.writeString(out, idxName);
        out.writeObject(t);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        key = U.readString(in);
        cacheName = U.readString(in);
        idxName = U.readString(in);
        t = (Throwable)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexValidationIssue.class, this) + ", " + t.getClass() + ": " + t.getMessage();
    }
}
