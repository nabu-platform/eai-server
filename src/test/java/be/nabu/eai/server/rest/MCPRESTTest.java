package be.nabu.eai.server.rest;

import junit.framework.TestCase;

public class MCPRESTTest extends TestCase {
	public void testLineDiffKeepsUnchangedXmlBlocksAsContext() {
		String before = "<sequence>\n"
			+ "\t<map>\n"
			+ "\t\t<link>\n"
			+ "\t\t\t<from>result01711b740f90427b81a8aabe80418630/result</from>\n"
			+ "\t\t\t<to>location</to>\n"
			+ "\t\t</link>\n"
			+ "\t\t<invoke\n"
			+ "\t\t\tresultName=\"result9bfb8ec403fe4313a4b8dbce2ea9e05c\"\n"
			+ "\t\t\tserviceId=\"bebatOne.utils.normalizeBusinessHours\">\n"
			+ "\t\t\t<link>\n"
			+ "\t\t\t\t<from>input/content/collectionHoursSame</from>\n"
			+ "\t\t\t\t<to>collectionHoursSame</to>\n"
			+ "\t\t\t</link>\n"
			+ "\t\t</invoke>\n"
			+ "\t</map>\n"
			+ "\t<map>\n"
			+ "\t\t<invoke\n"
			+ "\t\t\tresultName=\"resulte85da69e37f94fb4884f68bbbca9e7e9\"\n"
			+ "\t\t\tserviceId=\"nabu.services.jdbc.Services.update\">\n"
			+ "\t\t\t<link>\n"
			+ "\t\t\t\t<from>location</from>\n"
			+ "\t\t\t\t<to>instances[0]</to>\n"
			+ "\t\t\t</link>\n"
			+ "\t\t</invoke>\n"
			+ "\t</map>\n"
			+ "</sequence>\n";
		String after = "<sequence>\n"
			+ "\t<map>\n"
			+ "\t\t<link>\n"
			+ "\t\t\t<from>result01711b740f90427b81a8aabe80418630/result</from>\n"
			+ "\t\t\t<to>location</to>\n"
			+ "\t\t</link>\n"
			+ "\t\t<link>\n"
			+ "\t\t\t<from>result01711b740f90427b81a8aabe80418630/result/locationTypeId</from>\n"
			+ "\t\t\t<to>previousLocationTypeId</to>\n"
			+ "\t\t</link>\n"
			+ "\t\t<invoke\n"
			+ "\t\t\tresultName=\"result9bfb8ec403fe4313a4b8dbce2ea9e05c\"\n"
			+ "\t\t\tserviceId=\"bebatOne.utils.normalizeBusinessHours\">\n"
			+ "\t\t\t<link>\n"
			+ "\t\t\t\t<from>input/content/collectionHoursSame</from>\n"
			+ "\t\t\t\t<to>collectionHoursSame</to>\n"
			+ "\t\t\t</link>\n"
			+ "\t\t</invoke>\n"
			+ "\t</map>\n"
			+ "\t<map>\n"
			+ "\t\t<invoke\n"
			+ "\t\t\tresultName=\"resulte85da69e37f94fb4884f68bbbca9e7e9\"\n"
			+ "\t\t\tserviceId=\"nabu.services.jdbc.Services.update\">\n"
			+ "\t\t\t<link>\n"
			+ "\t\t\t\t<from>location</from>\n"
			+ "\t\t\t\t<to>instances[0]</to>\n"
			+ "\t\t\t</link>\n"
			+ "\t\t</invoke>\n"
			+ "\t</map>\n"
			+ "\t<map\n"
			+ "\t\tcomment=\"Recalculate the owning organisation type when the location type changes\"\n"
			+ "\t\tlabel=\"location/parentId != null &amp;&amp; location/locationTypeId != null &amp;&amp; previousLocationTypeId != location/locationTypeId\">\n"
			+ "\t\t<invoke\n"
			+ "\t\t\tresultName=\"resultRecalculateOrganisationType\"\n"
			+ "\t\t\tserviceId=\"bebatOne.services.organisation.recalculateOrganisationType\">\n"
			+ "\t\t\t<link>\n"
			+ "\t\t\t\t<from>location/parentId</from>\n"
			+ "\t\t\t\t<to>organisationId</to>\n"
			+ "\t\t\t</link>\n"
			+ "\t\t</invoke>\n"
			+ "\t</map>\n"
			+ "</sequence>\n";
		String diff = MCPREST.buildLineDiff(before, after);
		assertTrue(diff.contains("+\t\t\t<to>previousLocationTypeId</to>\n"));
		assertTrue(diff.contains("+\t\t\tserviceId=\"bebatOne.services.organisation.recalculateOrganisationType\">\n"));
		assertTrue(diff.contains(" \t\t\tserviceId=\"bebatOne.utils.normalizeBusinessHours\">\n"));
		assertFalse(diff.contains("-\t\t\tserviceId=\"bebatOne.utils.normalizeBusinessHours\">\n"));
		assertFalse(diff.contains("+\t\t\tserviceId=\"bebatOne.utils.normalizeBusinessHours\">\n"));
		assertTrue("Expected compact diff, got " + countLines(diff) + " lines:\n" + diff, countLines(diff) <= 30);
	}

	public void testLineDiffReturnsEmptyWhenUnchanged() {
		assertEquals("", MCPREST.buildLineDiff("one\ntwo\n", "one\ntwo\n"));
	}

	private int countLines(String value) {
		if (value == null || value.length() == 0) {
			return 0;
		}
		return value.split("\\n", -1).length - 1;
	}
}
