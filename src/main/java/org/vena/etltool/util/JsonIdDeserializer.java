package org.vena.etltool.util;

import java.io.IOException;

import org.vena.etltool.entities.Id;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class JsonIdDeserializer extends JsonDeserializer<Id> {

	@Override
	public Id deserialize(JsonParser parser, DeserializationContext ctxt)
			throws IOException, JsonProcessingException {
		String value = parser.getText();
		if (value.charAt(0) == '{') {
			// Support JSON produced by old code that did not use the correct JsonIdSerializer
			// e.g. {"idValue":106489494357671936}
			OldId oldId = parser.readValueAs(OldId.class);
			return new Id(oldId.idValue);
		}
		return Id.valueOf(value);
	}

	private static class OldId {
		@JsonProperty("idValue")
		Long idValue;
	}

}
