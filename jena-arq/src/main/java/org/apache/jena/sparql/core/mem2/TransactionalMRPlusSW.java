package org.apache.jena.sparql.core.mem2;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.Transactional;

public class TransactionalMRPlusSW implements Transactional {

    private final ThreadLocal<Boolean> inTransaction = ThreadLocal.withInitial(() -> Boolean.FALSE);
    private final ThreadLocal<TxnType> txnType = ThreadLocal.withInitial(() -> null);
    private final ThreadLocal<ReadWrite> txnMode = ThreadLocal.withInitial(() -> null);

    @Override
    public void begin(TxnType type) {

    }

    @Override
    public boolean promote(Promote mode) {
        return false;
    }

    @Override
    public void commit() {

    }

    @Override
    public void abort() {

    }

    @Override
    public void end() {

    }

    @Override
    public ReadWrite transactionMode() {
        return null;
    }

    @Override
    public TxnType transactionType() {
        return null;
    }

    @Override
    public boolean isInTransaction() {
        return false;
    }
}
