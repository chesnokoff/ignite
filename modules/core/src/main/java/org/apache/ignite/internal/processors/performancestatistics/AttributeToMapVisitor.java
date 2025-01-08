package org.apache.ignite.internal.processors.performancestatistics;

import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.Nullable;

/** Fullfill {@code data} Map for specific row. */
class AttributeToMapVisitor implements SystemViewRowAttributeWalker.AttributeWithValueVisitor {
    /** Map to store data. */
    private Map<String, Object> data;

    /**
     * Sets map.
     *
     * @param data Map to fill.
     */
    public void data(Map<String, Object> data) {
        this.data = data;
    }

    /** {@inheritDoc} */
    @Override public <T> void accept(int idx, String name, Class<T> clazz, @Nullable T val) {
        if (val == null)
            data.put(name, val);
        else if (clazz.isEnum())
            data.put(name, ((Enum<?>)val).name());
        else if (clazz.isAssignableFrom(Class.class))
            data.put(name, ((Class<?>)val).getName());
        else if (clazz.isAssignableFrom(IgniteUuid.class) || clazz.isAssignableFrom(UUID.class) ||
            clazz.isAssignableFrom(InetSocketAddress.class) || clazz.isAssignableFrom(AbstractMap.class))
            data.put(name, String.valueOf(val));
        else
            data.put(name, val);
    }

    /** {@inheritDoc} */
    @Override public void acceptBoolean(int idx, String name, boolean val) {
        data.put(name, val);
    }

    /** {@inheritDoc} */
    @Override public void acceptChar(int idx, String name, char val) {
        data.put(name, val);
    }

    /** {@inheritDoc} */
    @Override public void acceptByte(int idx, String name, byte val) {
        data.put(name, val);
    }

    /** {@inheritDoc} */
    @Override public void acceptShort(int idx, String name, short val) {
        data.put(name, val);
    }

    /** {@inheritDoc} */
    @Override public void acceptInt(int idx, String name, int val) {
        data.put(name, val);
    }

    /** {@inheritDoc} */
    @Override public void acceptLong(int idx, String name, long val) {
        data.put(name, val);
    }

    /** {@inheritDoc} */
    @Override public void acceptFloat(int idx, String name, float val) {
        data.put(name, val);
    }

    /** {@inheritDoc} */
    @Override public void acceptDouble(int idx, String name, double val) {
        data.put(name, val);
    }
}