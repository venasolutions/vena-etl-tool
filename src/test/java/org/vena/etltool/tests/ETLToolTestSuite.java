package org.vena.etltool.tests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	ETLToolDeleteTest.class,
	ETLToolAuthenticationTest.class,
	ETLToolSSLTest.class,
	ETLToolFileToCubeTest.class,
	ETLToolFileToStageTest.class,
	ETLToolFileToStageToCubeTest.class,
	ETLToolStageToCubeTest.class,
	ETLToolTransformCompleteTest.class,
	ETLToolCancelTest.class,
	ETLToolSetErrorTest.class,
	ETLToolStatusTest.class,
	ETLToolLoadStepsTest.class,
	ETLToolVersionTest.class,
	ETLToolValidateTest.class,
	ETLToolVerboseTest.class,
	ETLToolTemplateIdTest.class
})
public class ETLToolTestSuite {

}
