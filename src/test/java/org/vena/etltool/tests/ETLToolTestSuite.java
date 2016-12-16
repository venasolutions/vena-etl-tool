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
	ETLToolStageToCubeTest.class,
	ETLToolTransformCompleteTest.class,
	ETLToolCancelTest.class,
	ETLToolSetErrorTest.class,
	ETLToolStatusTest.class
})
public class ETLToolTestSuite {

}
