/**
 * Autogenerated by Thrift Compiler (0.19.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.jena.riot.thrift2.wire;

@SuppressWarnings("all")
public class RDF_DataTuple implements org.apache.thrift.TBase<RDF_DataTuple, RDF_DataTuple._Fields>, java.io.Serializable, Cloneable, Comparable<RDF_DataTuple> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("RDF_DataTuple");

  private static final org.apache.thrift.protocol.TField ROW_FIELD_DESC = new org.apache.thrift.protocol.TField("row", org.apache.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.thrift.protocol.TField STRINGS_FIELD_DESC = new org.apache.thrift.protocol.TField("strings", org.apache.thrift.protocol.TType.LIST, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new RDF_DataTupleStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new RDF_DataTupleTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.util.List<RDF_Term> row; // required
  public @org.apache.thrift.annotation.Nullable java.util.List<java.lang.String> strings; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ROW((short)1, "row"),
    STRINGS((short)2, "strings");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // ROW
          return ROW;
        case 2: // STRINGS
          return STRINGS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    @Override
    public short getThriftFieldId() {
      return _thriftId;
    }

    @Override
    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ROW, new org.apache.thrift.meta_data.FieldMetaData("row", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, RDF_Term.class))));
    tmpMap.put(_Fields.STRINGS, new org.apache.thrift.meta_data.FieldMetaData("strings", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(RDF_DataTuple.class, metaDataMap);
  }

  public RDF_DataTuple() {
  }

  public RDF_DataTuple(
    java.util.List<RDF_Term> row,
    java.util.List<java.lang.String> strings)
  {
    this();
    this.row = row;
    this.strings = strings;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public RDF_DataTuple(RDF_DataTuple other) {
    if (other.isSetRow()) {
      java.util.List<RDF_Term> __this__row = new java.util.ArrayList<RDF_Term>(other.row.size());
      for (RDF_Term other_element : other.row) {
        __this__row.add(new RDF_Term(other_element));
      }
      this.row = __this__row;
    }
    if (other.isSetStrings()) {
      java.util.List<java.lang.String> __this__strings = new java.util.ArrayList<java.lang.String>(other.strings);
      this.strings = __this__strings;
    }
  }

  @Override
  public RDF_DataTuple deepCopy() {
    return new RDF_DataTuple(this);
  }

  @Override
  public void clear() {
    this.row = null;
    this.strings = null;
  }

  public int getRowSize() {
    return (this.row == null) ? 0 : this.row.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<RDF_Term> getRowIterator() {
    return (this.row == null) ? null : this.row.iterator();
  }

  public void addToRow(RDF_Term elem) {
    if (this.row == null) {
      this.row = new java.util.ArrayList<RDF_Term>();
    }
    this.row.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<RDF_Term> getRow() {
    return this.row;
  }

  public RDF_DataTuple setRow(@org.apache.thrift.annotation.Nullable java.util.List<RDF_Term> row) {
    this.row = row;
    return this;
  }

  public void unsetRow() {
    this.row = null;
  }

  /** Returns true if field row is set (has been assigned a value) and false otherwise */
  public boolean isSetRow() {
    return this.row != null;
  }

  public void setRowIsSet(boolean value) {
    if (!value) {
      this.row = null;
    }
  }

  public int getStringsSize() {
    return (this.strings == null) ? 0 : this.strings.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<java.lang.String> getStringsIterator() {
    return (this.strings == null) ? null : this.strings.iterator();
  }

  public void addToStrings(java.lang.String elem) {
    if (this.strings == null) {
      this.strings = new java.util.ArrayList<java.lang.String>();
    }
    this.strings.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<java.lang.String> getStrings() {
    return this.strings;
  }

  public RDF_DataTuple setStrings(@org.apache.thrift.annotation.Nullable java.util.List<java.lang.String> strings) {
    this.strings = strings;
    return this;
  }

  public void unsetStrings() {
    this.strings = null;
  }

  /** Returns true if field strings is set (has been assigned a value) and false otherwise */
  public boolean isSetStrings() {
    return this.strings != null;
  }

  public void setStringsIsSet(boolean value) {
    if (!value) {
      this.strings = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case ROW:
      if (value == null) {
        unsetRow();
      } else {
        setRow((java.util.List<RDF_Term>)value);
      }
      break;

    case STRINGS:
      if (value == null) {
        unsetStrings();
      } else {
        setStrings((java.util.List<java.lang.String>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case ROW:
      return getRow();

    case STRINGS:
      return getStrings();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  @Override
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case ROW:
      return isSetRow();
    case STRINGS:
      return isSetStrings();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof RDF_DataTuple)
      return this.equals((RDF_DataTuple)that);
    return false;
  }

  public boolean equals(RDF_DataTuple that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_row = true && this.isSetRow();
    boolean that_present_row = true && that.isSetRow();
    if (this_present_row || that_present_row) {
      if (!(this_present_row && that_present_row))
        return false;
      if (!this.row.equals(that.row))
        return false;
    }

    boolean this_present_strings = true && this.isSetStrings();
    boolean that_present_strings = true && that.isSetStrings();
    if (this_present_strings || that_present_strings) {
      if (!(this_present_strings && that_present_strings))
        return false;
      if (!this.strings.equals(that.strings))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetRow()) ? 131071 : 524287);
    if (isSetRow())
      hashCode = hashCode * 8191 + row.hashCode();

    hashCode = hashCode * 8191 + ((isSetStrings()) ? 131071 : 524287);
    if (isSetStrings())
      hashCode = hashCode * 8191 + strings.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(RDF_DataTuple other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetRow(), other.isSetRow());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRow()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.row, other.row);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetStrings(), other.isSetStrings());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStrings()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.strings, other.strings);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  @Override
  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  @Override
  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("RDF_DataTuple(");
    boolean first = true;

    sb.append("row:");
    if (this.row == null) {
      sb.append("null");
    } else {
      sb.append(this.row);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("strings:");
    if (this.strings == null) {
      sb.append("null");
    } else {
      sb.append(this.strings);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class RDF_DataTupleStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public RDF_DataTupleStandardScheme getScheme() {
      return new RDF_DataTupleStandardScheme();
    }
  }

  private static class RDF_DataTupleStandardScheme extends org.apache.thrift.scheme.StandardScheme<RDF_DataTuple> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, RDF_DataTuple struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // ROW
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list24 = iprot.readListBegin();
                struct.row = new java.util.ArrayList<RDF_Term>(_list24.size);
                @org.apache.thrift.annotation.Nullable RDF_Term _elem25;
                for (int _i26 = 0; _i26 < _list24.size; ++_i26)
                {
                  _elem25 = new RDF_Term();
                  _elem25.read(iprot);
                  struct.row.add(_elem25);
                }
                iprot.readListEnd();
              }
              struct.setRowIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // STRINGS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list27 = iprot.readListBegin();
                struct.strings = new java.util.ArrayList<java.lang.String>(_list27.size);
                @org.apache.thrift.annotation.Nullable java.lang.String _elem28;
                for (int _i29 = 0; _i29 < _list27.size; ++_i29)
                {
                  _elem28 = iprot.readString();
                  struct.strings.add(_elem28);
                }
                iprot.readListEnd();
              }
              struct.setStringsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    @Override
    public void write(org.apache.thrift.protocol.TProtocol oprot, RDF_DataTuple struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.row.size()));
          for (RDF_Term _iter30 : struct.row)
          {
            _iter30.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.strings != null) {
        oprot.writeFieldBegin(STRINGS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.strings.size()));
          for (java.lang.String _iter31 : struct.strings)
          {
            oprot.writeString(_iter31);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class RDF_DataTupleTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public RDF_DataTupleTupleScheme getScheme() {
      return new RDF_DataTupleTupleScheme();
    }
  }

  private static class RDF_DataTupleTupleScheme extends org.apache.thrift.scheme.TupleScheme<RDF_DataTuple> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, RDF_DataTuple struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetRow()) {
        optionals.set(0);
      }
      if (struct.isSetStrings()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetRow()) {
        {
          oprot.writeI32(struct.row.size());
          for (RDF_Term _iter32 : struct.row)
          {
            _iter32.write(oprot);
          }
        }
      }
      if (struct.isSetStrings()) {
        {
          oprot.writeI32(struct.strings.size());
          for (java.lang.String _iter33 : struct.strings)
          {
            oprot.writeString(_iter33);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, RDF_DataTuple struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list34 = iprot.readListBegin(org.apache.thrift.protocol.TType.STRUCT);
          struct.row = new java.util.ArrayList<RDF_Term>(_list34.size);
          @org.apache.thrift.annotation.Nullable RDF_Term _elem35;
          for (int _i36 = 0; _i36 < _list34.size; ++_i36)
          {
            _elem35 = new RDF_Term();
            _elem35.read(iprot);
            struct.row.add(_elem35);
          }
        }
        struct.setRowIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list37 = iprot.readListBegin(org.apache.thrift.protocol.TType.STRING);
          struct.strings = new java.util.ArrayList<java.lang.String>(_list37.size);
          @org.apache.thrift.annotation.Nullable java.lang.String _elem38;
          for (int _i39 = 0; _i39 < _list37.size; ++_i39)
          {
            _elem38 = iprot.readString();
            struct.strings.add(_elem38);
          }
        }
        struct.setStringsIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}
