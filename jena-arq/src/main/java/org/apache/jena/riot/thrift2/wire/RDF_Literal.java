/**
 * Autogenerated by Thrift Compiler (0.19.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.jena.riot.thrift2.wire;

@SuppressWarnings("all")
public class RDF_Literal implements org.apache.thrift.TBase<RDF_Literal, RDF_Literal._Fields>, java.io.Serializable, Cloneable, Comparable<RDF_Literal> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("RDF_Literal");

  private static final org.apache.thrift.protocol.TField LEX_FIELD_DESC = new org.apache.thrift.protocol.TField("lex", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField LANGTAG_FIELD_DESC = new org.apache.thrift.protocol.TField("langtag", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField DATATYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("datatype", org.apache.thrift.protocol.TType.I32, (short)3);
  private static final org.apache.thrift.protocol.TField DT_PREFIX_FIELD_DESC = new org.apache.thrift.protocol.TField("dtPrefix", org.apache.thrift.protocol.TType.STRUCT, (short)4);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new RDF_LiteralStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new RDF_LiteralTupleSchemeFactory();

  public int lex; // required
  public int langtag; // optional
  public int datatype; // optional
  public @org.apache.thrift.annotation.Nullable RDF_PrefixName dtPrefix; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    LEX((short)1, "lex"),
    LANGTAG((short)2, "langtag"),
    DATATYPE((short)3, "datatype"),
    DT_PREFIX((short)4, "dtPrefix");

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
        case 1: // LEX
          return LEX;
        case 2: // LANGTAG
          return LANGTAG;
        case 3: // DATATYPE
          return DATATYPE;
        case 4: // DT_PREFIX
          return DT_PREFIX;
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
  private static final int __LEX_ISSET_ID = 0;
  private static final int __LANGTAG_ISSET_ID = 1;
  private static final int __DATATYPE_ISSET_ID = 2;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.LANGTAG,_Fields.DATATYPE,_Fields.DT_PREFIX};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.LEX, new org.apache.thrift.meta_data.FieldMetaData("lex", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.LANGTAG, new org.apache.thrift.meta_data.FieldMetaData("langtag", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.DATATYPE, new org.apache.thrift.meta_data.FieldMetaData("datatype", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.DT_PREFIX, new org.apache.thrift.meta_data.FieldMetaData("dtPrefix", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, RDF_PrefixName.class)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(RDF_Literal.class, metaDataMap);
  }

  public RDF_Literal() {
    this.lex = -1;

    this.langtag = -1;

    this.datatype = -1;

  }

  public RDF_Literal(
    int lex)
  {
    this();
    this.lex = lex;
    setLexIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public RDF_Literal(RDF_Literal other) {
    __isset_bitfield = other.__isset_bitfield;
    this.lex = other.lex;
    this.langtag = other.langtag;
    this.datatype = other.datatype;
    if (other.isSetDtPrefix()) {
      this.dtPrefix = new RDF_PrefixName(other.dtPrefix);
    }
  }

  @Override
  public RDF_Literal deepCopy() {
    return new RDF_Literal(this);
  }

  @Override
  public void clear() {
    this.lex = -1;

    this.langtag = -1;

    this.datatype = -1;

    this.dtPrefix = null;
  }

  public int getLex() {
    return this.lex;
  }

  public RDF_Literal setLex(int lex) {
    this.lex = lex;
    setLexIsSet(true);
    return this;
  }

  public void unsetLex() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __LEX_ISSET_ID);
  }

  /** Returns true if field lex is set (has been assigned a value) and false otherwise */
  public boolean isSetLex() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __LEX_ISSET_ID);
  }

  public void setLexIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __LEX_ISSET_ID, value);
  }

  public int getLangtag() {
    return this.langtag;
  }

  public RDF_Literal setLangtag(int langtag) {
    this.langtag = langtag;
    setLangtagIsSet(true);
    return this;
  }

  public void unsetLangtag() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __LANGTAG_ISSET_ID);
  }

  /** Returns true if field langtag is set (has been assigned a value) and false otherwise */
  public boolean isSetLangtag() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __LANGTAG_ISSET_ID);
  }

  public void setLangtagIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __LANGTAG_ISSET_ID, value);
  }

  public int getDatatype() {
    return this.datatype;
  }

  public RDF_Literal setDatatype(int datatype) {
    this.datatype = datatype;
    setDatatypeIsSet(true);
    return this;
  }

  public void unsetDatatype() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __DATATYPE_ISSET_ID);
  }

  /** Returns true if field datatype is set (has been assigned a value) and false otherwise */
  public boolean isSetDatatype() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __DATATYPE_ISSET_ID);
  }

  public void setDatatypeIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __DATATYPE_ISSET_ID, value);
  }

  @org.apache.thrift.annotation.Nullable
  public RDF_PrefixName getDtPrefix() {
    return this.dtPrefix;
  }

  public RDF_Literal setDtPrefix(@org.apache.thrift.annotation.Nullable RDF_PrefixName dtPrefix) {
    this.dtPrefix = dtPrefix;
    return this;
  }

  public void unsetDtPrefix() {
    this.dtPrefix = null;
  }

  /** Returns true if field dtPrefix is set (has been assigned a value) and false otherwise */
  public boolean isSetDtPrefix() {
    return this.dtPrefix != null;
  }

  public void setDtPrefixIsSet(boolean value) {
    if (!value) {
      this.dtPrefix = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case LEX:
      if (value == null) {
        unsetLex();
      } else {
        setLex((java.lang.Integer)value);
      }
      break;

    case LANGTAG:
      if (value == null) {
        unsetLangtag();
      } else {
        setLangtag((java.lang.Integer)value);
      }
      break;

    case DATATYPE:
      if (value == null) {
        unsetDatatype();
      } else {
        setDatatype((java.lang.Integer)value);
      }
      break;

    case DT_PREFIX:
      if (value == null) {
        unsetDtPrefix();
      } else {
        setDtPrefix((RDF_PrefixName)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case LEX:
      return getLex();

    case LANGTAG:
      return getLangtag();

    case DATATYPE:
      return getDatatype();

    case DT_PREFIX:
      return getDtPrefix();

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
    case LEX:
      return isSetLex();
    case LANGTAG:
      return isSetLangtag();
    case DATATYPE:
      return isSetDatatype();
    case DT_PREFIX:
      return isSetDtPrefix();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof RDF_Literal)
      return this.equals((RDF_Literal)that);
    return false;
  }

  public boolean equals(RDF_Literal that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_lex = true;
    boolean that_present_lex = true;
    if (this_present_lex || that_present_lex) {
      if (!(this_present_lex && that_present_lex))
        return false;
      if (this.lex != that.lex)
        return false;
    }

    boolean this_present_langtag = true && this.isSetLangtag();
    boolean that_present_langtag = true && that.isSetLangtag();
    if (this_present_langtag || that_present_langtag) {
      if (!(this_present_langtag && that_present_langtag))
        return false;
      if (this.langtag != that.langtag)
        return false;
    }

    boolean this_present_datatype = true && this.isSetDatatype();
    boolean that_present_datatype = true && that.isSetDatatype();
    if (this_present_datatype || that_present_datatype) {
      if (!(this_present_datatype && that_present_datatype))
        return false;
      if (this.datatype != that.datatype)
        return false;
    }

    boolean this_present_dtPrefix = true && this.isSetDtPrefix();
    boolean that_present_dtPrefix = true && that.isSetDtPrefix();
    if (this_present_dtPrefix || that_present_dtPrefix) {
      if (!(this_present_dtPrefix && that_present_dtPrefix))
        return false;
      if (!this.dtPrefix.equals(that.dtPrefix))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + lex;

    hashCode = hashCode * 8191 + ((isSetLangtag()) ? 131071 : 524287);
    if (isSetLangtag())
      hashCode = hashCode * 8191 + langtag;

    hashCode = hashCode * 8191 + ((isSetDatatype()) ? 131071 : 524287);
    if (isSetDatatype())
      hashCode = hashCode * 8191 + datatype;

    hashCode = hashCode * 8191 + ((isSetDtPrefix()) ? 131071 : 524287);
    if (isSetDtPrefix())
      hashCode = hashCode * 8191 + dtPrefix.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(RDF_Literal other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetLex(), other.isSetLex());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLex()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.lex, other.lex);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetLangtag(), other.isSetLangtag());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLangtag()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.langtag, other.langtag);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetDatatype(), other.isSetDatatype());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDatatype()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.datatype, other.datatype);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetDtPrefix(), other.isSetDtPrefix());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDtPrefix()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dtPrefix, other.dtPrefix);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("RDF_Literal(");
    boolean first = true;

    sb.append("lex:");
    sb.append(this.lex);
    first = false;
    if (isSetLangtag()) {
      if (!first) sb.append(", ");
      sb.append("langtag:");
      sb.append(this.langtag);
      first = false;
    }
    if (isSetDatatype()) {
      if (!first) sb.append(", ");
      sb.append("datatype:");
      sb.append(this.datatype);
      first = false;
    }
    if (isSetDtPrefix()) {
      if (!first) sb.append(", ");
      sb.append("dtPrefix:");
      if (this.dtPrefix == null) {
        sb.append("null");
      } else {
        sb.append(this.dtPrefix);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // alas, we cannot check 'lex' because it's a primitive and you chose the non-beans generator.
    // check for sub-struct validity
    if (dtPrefix != null) {
      dtPrefix.validate();
    }
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class RDF_LiteralStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public RDF_LiteralStandardScheme getScheme() {
      return new RDF_LiteralStandardScheme();
    }
  }

  private static class RDF_LiteralStandardScheme extends org.apache.thrift.scheme.StandardScheme<RDF_Literal> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, RDF_Literal struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // LEX
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.lex = iprot.readI32();
              struct.setLexIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // LANGTAG
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.langtag = iprot.readI32();
              struct.setLangtagIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // DATATYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.datatype = iprot.readI32();
              struct.setDatatypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // DT_PREFIX
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.dtPrefix = new RDF_PrefixName();
              struct.dtPrefix.read(iprot);
              struct.setDtPrefixIsSet(true);
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
      if (!struct.isSetLex()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'lex' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    @Override
    public void write(org.apache.thrift.protocol.TProtocol oprot, RDF_Literal struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(LEX_FIELD_DESC);
      oprot.writeI32(struct.lex);
      oprot.writeFieldEnd();
      if (struct.isSetLangtag()) {
        oprot.writeFieldBegin(LANGTAG_FIELD_DESC);
        oprot.writeI32(struct.langtag);
        oprot.writeFieldEnd();
      }
      if (struct.isSetDatatype()) {
        oprot.writeFieldBegin(DATATYPE_FIELD_DESC);
        oprot.writeI32(struct.datatype);
        oprot.writeFieldEnd();
      }
      if (struct.dtPrefix != null) {
        if (struct.isSetDtPrefix()) {
          oprot.writeFieldBegin(DT_PREFIX_FIELD_DESC);
          struct.dtPrefix.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class RDF_LiteralTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public RDF_LiteralTupleScheme getScheme() {
      return new RDF_LiteralTupleScheme();
    }
  }

  private static class RDF_LiteralTupleScheme extends org.apache.thrift.scheme.TupleScheme<RDF_Literal> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, RDF_Literal struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeI32(struct.lex);
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetLangtag()) {
        optionals.set(0);
      }
      if (struct.isSetDatatype()) {
        optionals.set(1);
      }
      if (struct.isSetDtPrefix()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetLangtag()) {
        oprot.writeI32(struct.langtag);
      }
      if (struct.isSetDatatype()) {
        oprot.writeI32(struct.datatype);
      }
      if (struct.isSetDtPrefix()) {
        struct.dtPrefix.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, RDF_Literal struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.lex = iprot.readI32();
      struct.setLexIsSet(true);
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.langtag = iprot.readI32();
        struct.setLangtagIsSet(true);
      }
      if (incoming.get(1)) {
        struct.datatype = iprot.readI32();
        struct.setDatatypeIsSet(true);
      }
      if (incoming.get(2)) {
        struct.dtPrefix = new RDF_PrefixName();
        struct.dtPrefix.read(iprot);
        struct.setDtPrefixIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}
