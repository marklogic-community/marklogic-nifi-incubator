In order to run all of the tests in this project, you must first deploy a test application to MarkLogic:

Execute the commands as below :

1. ./gradlew -i mlDeploy
2. cd dhf4 && ./gradlew -i mlDeploy
3. cd ../dhf5 && ./gradlew -i mlDeploy

Step 1 will deploy the test application and It ensures that all of the integration tests - those ending in "IT" - will be able to run. 
Step 2 and 3 will setup two Datahub test project - DHF4 (version '4.3.2') and DHF5 (version '5.0.3'). These projects will be used to execute EvaluateCollector Processor unit test cases which are ending with "DHF4/5".

Tests that do not end in "IT" do not depend on the test application having been deployed. 