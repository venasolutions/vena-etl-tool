package org.vena.etltool.util;

import java.io.IOException;

import org.vena.etltool.entities.Id;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class JsonIdSerializer extends JsonSerializer<Id> {

	@Override
	public void serialize(Id value, JsonGenerator generator,
			SerializerProvider provider) throws IOException,
			JsonProcessingException {
		generator.writeString(value.toString());
	}

}