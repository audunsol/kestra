package io.kestra.core.models;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

import java.io.IOException;

/**
 * Define a plugin properties that will be rendered and converted to a target type at use time.
 *
 * @param <T> the target type of the property
 */
@JsonDeserialize(using = Property.PropertyDeserializer.class)
@JsonSerialize(using = Property.PropertySerializer.class)
public class Property<T> {
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();

    public String expression;
    private T value;

    public Property() {
    }

    // used only by the deserializer
    Property(String expression) {
        this.expression = expression;
    }

    /**
     * Build a new Property object with a value already set.<br>
     *
     * A property build with this method will always return the value passed at build time, no rendering will be done.
     */
    public static <V> Property<V> of(V value) {
        // trick the serializer so the property would not be null at deserialization time
        Property<V> p = new Property<>(MAPPER.convertValue(value, String.class));
        p.value = value;
        return p;
    }

    /**
     * Render a property then convert it to its target type.<br>
     *
     * This method is safe to be used as many times as you want as the rendering and conversion will be cached.
     * Warning, due to the caching mechanism, this method is not thread-safe.
     */
    public T as(RunContext runContext, Class<T> clazz) throws IllegalVariableEvaluationException {
        if (this.value == null) {
            String rendered =  runContext.render(expression);
            this.value = MAPPER.convertValue(rendered, clazz);
        }

        return this.value;
    }

    // used only by the serializer
    String getExpression() {
        return this.expression;
    }

    static class PropertyDeserializer extends StdDeserializer<Property<?>> {

        protected PropertyDeserializer() {
            super(Property.class);
        }

        @Override
        public Property<?> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            return new Property<>(p.getValueAsString());
        }
    }

    @SuppressWarnings("raw")
    static class PropertySerializer extends StdSerializer<Property> {

        protected PropertySerializer() {
            super(Property.class);
        }

        @Override
        public void serialize(Property value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeString(value.getExpression());
        }
    }
}
