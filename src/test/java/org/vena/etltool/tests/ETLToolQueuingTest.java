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
public class ETLToolQueuingTest extends ETLToolTest {
	
	String[] commands;
	Boolean queuingEnabled;
	boolean exitExpected;
	
	public ETLToolQueuingTest(String[] commands, Boolean queuingEnabled, boolean exitExpected) {
		this.commands = commands;
		this.queuingEnabled = queuingEnabled;
		this.exitExpected = exitExpected;
	}

	@Test
	public void testEnableAndDisableQueuing() throws UnsupportedEncodingException {
		ETLClient etlClient = mockETLClient();
		String[] args = buildCommand(commands);
		
		try {
			ETLMetadataDTO metadata = Main.buildETLMetadata(args, etlClient);
			assertEquals(modelId, metadata.getModelId());
			assertEquals(queuingEnabled, metadata.getQueuingEnabled());
		} catch (ExitException e) {
			assertTrue(exitExpected);
			assertEquals(1, e.status);
			assertEquals("Error: --queue and --noqueue options cannot be combined.", err.toString().trim());
		}
	}
	
	@Parameters(name="{0}")
	public static Iterable<Object[]> parameters() {
		Object[][] result = new Object[][] {
			{ new String[] {"--jobName", "File To Cube", "--file", "lidsFile.csv;type=lids;format=CSV;", "--queue"}, true, false },
			{ new String[] {"--jobName", "File To Cube", "--file", "lidsFile.csv;type=lids;format=CSV;", "--noqueue"}, false, false },
			{ new String[] {"--jobName", "File To Cube", "--file", "lidsFile.csv;type=lids;format=CSV;"}, null, false },
			{ new String[] {"--jobName", "Stage To Cube", "--loadFromStaging", "--queue"}, true, false },
			{ new String[] {"--jobName", "Stage To Cube", "--loadFromStaging", "--noqueue"}, false, false },
			{ new String[] {"--jobName", "Stage To Cube", "--loadFromStaging"}, null, false },
			{ new String[] {"--delete", "intersections", "--deleteQuery", "dimension('Accounts': 'Expense')", "--queue"}, true, false },
			{ new String[] {"--delete", "intersections", "--deleteQuery", "dimension('Accounts': 'Expense')", "--noqueue"}, false, false },
			{ new String[] {"--delete", "intersections", "--deleteQuery", "dimension('Accounts': 'Expense')"}, null, false },
			{ new String[] {"--jobName", "Cube To Stage", "--export", "intersections", "--exportToTable", "export_table", "--exportQuery", "dimension('Accounts':'Sale')", "--queue"}, true, false },
			{ new String[] {"--jobName", "Cube To Stage", "--export", "intersections", "--exportToTable", "export_table", "--exportQuery", "dimension('Accounts':'Sale')", "--noqueue"}, false, false },
			{ new String[] {"--jobName", "Cube To Stage", "--export", "intersections", "--exportToTable", "export_table", "--exportQuery", "dimension('Accounts':'Sale')"}, null, false },
			{ new String[] {"--jobName", "File To Stage To Cube", "--file", "intersectionsFile.csv;type=intersections;format=CSV;table=values_table", "--stageAndTransform", "--queue"}, true, false },
			{ new String[] {"--jobName", "File To Stage To Cube", "--file", "intersectionsFile.csv;type=intersections;format=CSV;table=values_table", "--stageAndTransform", "--noqueue"}, false, false },
			{ new String[] {"--jobName", "File To Stage To Cube", "--file", "intersectionsFile.csv;type=intersections;format=CSV;table=values_table", "--stageAndTransform"}, null, false },
			{ new String[] {"--jobName", "Loading job with multiple steps", "--loadSteps", "src" + File.separatorChar + "test" + File.separatorChar + "resources" + File.separatorChar + "steps.txt", "--queue"}, true, false },
			{ new String[] {"--jobName", "Loading job with multiple steps", "--loadSteps", "src" + File.separatorChar + "test" + File.separatorChar + "resources" + File.separatorChar + "steps.txt", "--noqueue"}, false, false },
			{ new String[] {"--jobName", "Loading job with multiple steps", "--loadSteps", "src" + File.separatorChar + "test" + File.separatorChar + "resources" + File.separatorChar + "steps.txt"}, null, false },
			{ new String[] {"--jobName", "Stage To Cube", "--loadFromStaging", "--noqueue", "--queue"}, false, true }
		};
		return Arrays.asList(result);
	}
}
