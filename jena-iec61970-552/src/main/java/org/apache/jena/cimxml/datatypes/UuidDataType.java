package org.apache.jena.cimxml.datatypes;

import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.datatypes.DatatypeFormatException;

import java.util.UUID;

public class UuidDataType  extends BaseDatatype {

    public static final UuidDataType INSTANCE = new UuidDataType();

    UuidDataType() {
        super("java:java.util.UUID");
    }

    @Override
    public Class<?> getJavaClass() {
        return UUID.class;
    }

    @Override
    public Object parse(String lexicalForm) throws DatatypeFormatException {
        try {
            return UUID.fromString(lexicalForm);
        } catch (Throwable th) {
            throw new DatatypeFormatException();
        }
    }
}
