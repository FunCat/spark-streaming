package com.epam.bdcc.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.numenta.nupic.network.Network;
import org.numenta.nupic.serialize.HTMObjectInput;
import org.numenta.nupic.serialize.HTMObjectOutput;
import org.numenta.nupic.serialize.SerialConfig;
import org.numenta.nupic.serialize.SerializerCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// SerDe HTM objects using SerializerCore (https://github.com/RuedigerMoeller/fast-serialization)
public class SparkKryoHTMSerializer<T> extends Serializer<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkKryoHTMSerializer.class);
    private final SerializerCore htmSerializer = new SerializerCore(SerialConfig.DEFAULT_REGISTERED_TYPES);

    public SparkKryoHTMSerializer() {
        htmSerializer.registerClass(Network.class);
    }

    @Override
    public T copy(Kryo kryo, T original) {
        //TODO : Add implementation for copy, if needed
        throw new UnsupportedOperationException("Add implementation for copy");
    }

    @Override
    public void write(Kryo kryo, Output kryoOutput, T t) {
        HTMObjectOutput writer = null;
        //TODO : Add implementation for serialization
        throw new UnsupportedOperationException("Add implementation for serialization");
    }

    @Override
    public T read(Kryo kryo, Input kryoInput, Class<T> aClass) {
        HTMObjectInput reader = null;
        //TODO : Add implementation for deserialization
        throw new UnsupportedOperationException("Add implementation for deserialization");
    }

    public static void registerSerializers(Kryo kryo) {
        kryo.register(Network.class, new SparkKryoHTMSerializer<>());
        for (Class c : SerialConfig.DEFAULT_REGISTERED_TYPES)
            kryo.register(c, new SparkKryoHTMSerializer<>());
    }
}
