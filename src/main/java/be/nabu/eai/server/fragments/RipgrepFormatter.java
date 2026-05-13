package be.nabu.eai.server.fragments;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public final class RipgrepFormatter {

	private RipgrepFormatter() {
	}

	public static List<String> format(String content, Pattern pattern, int before, int after) {
		List<String> matches = new ArrayList<String>();
		if (content == null) {
			return matches;
		}
		String[] lines = content.split("\\r?\\n", -1);
		Set<Integer> included = new LinkedHashSet<Integer>();
		Set<Integer> matched = new LinkedHashSet<Integer>();
		for (int i = 0; i < lines.length; i++) {
			if (pattern.matcher(lines[i]).find()) {
				matched.add(i);
				int start = Math.max(0, i - Math.max(0, before));
				int end = Math.min(lines.length - 1, i + Math.max(0, after));
				for (int j = start; j <= end; j++) {
					included.add(j);
				}
			}
		}
		Integer previous = null;
		for (Integer lineIndex : included) {
			if (previous != null && lineIndex.intValue() != previous.intValue() + 1) {
				matches.add("--");
			}
			String separator = matched.contains(lineIndex) ? ":" : "-";
			matches.add((lineIndex.intValue() + 1) + separator + lines[lineIndex.intValue()]);
			previous = lineIndex;
		}
		return matches;
	}
}
