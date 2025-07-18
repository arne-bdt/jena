/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jena.geosparql.kryo;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/** Geometry de-/serialization via the WKB facilities of JTS. */
public class GeometrySerializerJtsWkb
    extends Serializer<Geometry>
{
    private transient WKBReader reader;
    private transient WKBWriter writer;

    public GeometrySerializerJtsWkb() {
        this(new GeometryFactory());
    }

    public GeometrySerializerJtsWkb(GeometryFactory geometryFactory) {
        this(new WKBReader(geometryFactory), new WKBWriter());
    }

    public GeometrySerializerJtsWkb(WKBReader reader, WKBWriter writer) {
        super();
        this.reader = reader;
        this.writer = writer;
    }

    @Override
    public void write(Kryo kryo, Output output, Geometry geometry) {
        byte[] data = writer.write(geometry);
        output.write(data, 0, data.length);
        kryo.writeClassAndObject(output, geometry.getUserData());
    }

    @Override
    public Geometry read(Kryo kryo, Input input, Class<Geometry> type) {
        byte[] bytes = kryo.readObject(input, byte[].class);
        Geometry geometry;
        try {
            geometry = reader.read(bytes);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        Object userData = kryo.readClassAndObject(input);
        geometry.setUserData(userData);
        return geometry;
    }
}
