package be.nabu.eai.server.impl;

import be.nabu.libs.types.api.MarshalRuleProvider;
import be.nabu.utils.mime.api.Part;

public class MarshalRuleProviderImpl implements MarshalRuleProvider {

	@Override
	public MarshalRule getMarshalRule(Class<?> clazz) {
		if (Part.class.isAssignableFrom(clazz)) {
			return MarshalRule.NEVER;
		}
		return null;
	}

}
