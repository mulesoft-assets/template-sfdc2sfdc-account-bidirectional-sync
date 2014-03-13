package org.mule.templates.test.util;

import org.mule.tck.probe.Probe;

public abstract class AssertionProbe implements Probe {

	private String errorMessage;

	@Override
	public boolean isSatisfied() {
		try {
			assertSatisfied();

			return true;

		} catch (final Exception e) {
			errorMessage = e.getMessage();
			return false;
		}
	}

	@Override
	public String describeFailure() {
		return errorMessage;
	}

	public abstract void assertSatisfied() throws Exception;

}
