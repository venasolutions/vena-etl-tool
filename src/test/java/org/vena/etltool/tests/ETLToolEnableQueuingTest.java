package org.vena.etltool.tests;

import static org.junit.Assert.*;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.vena.etltool.ETLClient;
import org.vena.etltool.Main;
import org.vena.etltool.entities.ETLMetadataDTO;

@RunWith(Parameterized.class)
public class ETLToolEnableQueuingTest extends ETLToolTest {
	
	String[] commands;
	boolean queuingEnabled;
	boolean exitExpected;
	
	public ETLToolEnableQueuingTest(String[] commands, boolean queuingEnabled, boolean exitExpected) {
		this.commands = commands;
		this.queuingEnabled = queuingEnabled;
		this.exitExpected = exitExpected;
	}

	@Test
	public void testEnableAndDisableQueuing() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(commands);
		
		try {
			ETLMetadataDTO metadata = Main.parseCmdlineArgs(args, etlClient);
			assertEquals(modelId, metadata.getModelId());
			assertEquals(queuingEnabled, metadata.getQueuingEnabled());
		} catch (ExitException e) {
			assertTrue(exitExpected);
			assertEquals(1, e.status);
			assertEquals("Error: --enableQueuing and --disableQueuing options cannot be combined.\n", err.toString());
		}
	}
	
	@Parameters(name="{0}")
	public static Iterable<Object[]> parameters() {
		Object[][] result = new Object[][] {
			{ new String[] {"--jobName", "File To Cube", "--file", "lidsFile.csv;type=lids;format=CSV;", "--enableQueuing"}, true, false },
			{ new String[] {"--jobName", "File To Cube", "--file", "lidsFile.csv;type=lids;format=CSV;", "--disableQueuing"}, false, false },
			{ new String[] {"--jobName", "Stage To Cube", "--loadFromStaging", "--enableQueuing"}, true, false },
			{ new String[] {"--jobName", "Stage To Cube", "--loadFromStaging", "--disableQueuing"}, false, false },
			{ new String[] {"--delete", "intersections", "--deleteQuery", "dimension('Accounts': 'Expense')", "--enableQueuing"}, true, false },
			{ new String[] {"--delete", "intersections", "--deleteQuery", "dimension('Accounts': 'Expense')", "--disableQueuing"}, false, false },
			{ new String[] {"--jobName", "Cube To Stage", "--export", "intersections", "--exportToTable", "export_table", "--exportQuery", "dimension('Accounts':'Sale')", "--enableQueuing"}, true, false },
			{ new String[] {"--jobName", "Cube To Stage", "--export", "intersections", "--exportToTable", "export_table", "--exportQuery", "dimension('Accounts':'Sale')", "--disableQueuing"}, false, false },
			{ new String[] {"--jobName", "File To Stage To Cube", "--file", "intersectionsFile.csv;type=intersections;format=CSV;table=values_table", "--stageAndTransform", "--enableQueuing"}, true, false },
			{ new String[] {"--jobName", "File To Stage To Cube", "--file", "intersectionsFile.csv;type=intersections;format=CSV;table=values_table", "--stageAndTransform", "--disableQueuing"}, false, false },
			{ new String[] {"--jobName", "Loading job with multiple steps", "--loadSteps", "src" + File.separatorChar + "test" + File.separatorChar + "resources" + File.separatorChar + "steps.txt", "--enableQueuing"}, true, false },
			{ new String[] {"--jobName", "Loading job with multiple steps", "--loadSteps", "src" + File.separatorChar + "test" + File.separatorChar + "resources" + File.separatorChar + "steps.txt", "--enableQueuing"}, true, false },
			{ new String[] {"--jobName", "Stage To Cube", "--loadFromStaging", "--disableQueuing", "--enableQueuing"}, false, true }
		};
		return Arrays.asList(result);
	}
}
