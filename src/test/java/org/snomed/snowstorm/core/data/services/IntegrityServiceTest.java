package org.snomed.snowstorm.core.data.services;

import io.kaicode.elasticvc.api.BranchService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.snomed.snowstorm.AbstractTest;
import org.snomed.snowstorm.TestConfig;
import org.snomed.snowstorm.core.data.domain.Concept;
import org.snomed.snowstorm.core.data.domain.Relationship;
import org.snomed.snowstorm.core.data.services.pojo.IntegrityIssueReport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfig.class)
public class IntegrityServiceTest extends AbstractTest {

	@Autowired
	private BranchService branchService;

	@Autowired
	private BranchMergeService branchMergeService;

	@Autowired
	private ConceptService conceptService;

	@Autowired
	private IntegrityService integrityService;

	@Test
	/*
		Test the method that checks all the components visible on the branch.
	 */
	public void testFindAllComponentsWithBadIntegrity() throws ServiceException {
		branchService.create("MAIN");
		branchService.create("MAIN/project");

		branchService.create("MAIN/project/test1");

		conceptService.create(new Concept("1"), "MAIN/project");
		// Valid relationship on MAIN/project
		conceptService.create(new Concept("101").addRelationship(new Relationship("101", "1")), "MAIN/project");
		// Valid relationship on MAIN/project
		conceptService.create(new Concept("2").addRelationship(new Relationship("101", "1")), "MAIN/project");
		// Missing Type on MAIN/project
		conceptService.create(new Concept("3").addRelationship(new Relationship("102", "2")), "MAIN/project");
		// Missing Destination on MAIN/project
		conceptService.create(new Concept("4").addRelationship(new Relationship("101", "1000")), "MAIN/project");
		// Valid relationship on MAIN/project
		conceptService.create(new Concept("5").addRelationship(new Relationship("101", "2")), "MAIN/project");

		branchService.create("MAIN/project/test2");
		// Valid relationship on MAIN/project/test2
		conceptService.create(new Concept("6").addRelationship(new Relationship("101", "5")), "MAIN/project/test2");
		// Missing Destination on MAIN/project/test2
		conceptService.create(new Concept("7").addRelationship(new Relationship("101", "1000")), "MAIN/project/test2");

		// Two bad relationships are on MAIN/project
		IntegrityIssueReport reportProject = integrityService.findAllComponentsWithBadIntegrity(branchService.findLatest("MAIN/project"), true);
		assertNull(reportProject.getRelationshipsWithMissingOrInactiveSource());
		assertEquals(1, reportProject.getRelationshipsWithMissingOrInactiveType().size());
		assertEquals(1, reportProject.getRelationshipsWithMissingOrInactiveDestination().size());

		// MAIN/project/test1 was created before the bad relationships so there should be no issue on that branch
		IntegrityIssueReport reportProjectTest1 = integrityService.findAllComponentsWithBadIntegrity(branchService.findLatest("MAIN/project/test1"), true);
		assertNull(reportProjectTest1.getRelationshipsWithMissingOrInactiveSource());
		assertNull(reportProjectTest1.getRelationshipsWithMissingOrInactiveType());
		assertNull(reportProjectTest1.getRelationshipsWithMissingOrInactiveDestination());

		// MAIN/project/test2 was created after the bad relationships so should be able to see the issues on MAIN/project plus the new bad relationship on MAIN/project/test2
		IntegrityIssueReport reportProjectTest2 = integrityService.findAllComponentsWithBadIntegrity(branchService.findLatest("MAIN/project/test2"), true);
		assertNull(reportProjectTest2.getRelationshipsWithMissingOrInactiveSource());
		assertEquals(1, reportProjectTest2.getRelationshipsWithMissingOrInactiveType().size());
		assertEquals(2, reportProjectTest2.getRelationshipsWithMissingOrInactiveDestination().size());

		// Let's make concept 5 inactive on MAIN/project
		conceptService.update((Concept) new Concept("5").setActive(false), "MAIN/project");

		// MAIN/project should have no new issues. Concept 5's relationship will not have a missing source concept because the relationship will have been deleted automatically

		// Still just two bad relationships are on MAIN/project
		assertEquals("MAIN/project report should be unchanged.", reportProject, integrityService.findAllComponentsWithBadIntegrity(branchService.findLatest("MAIN/project"), true));

		// There is a relationship on MAIN/project/test2 which will break now that concept 5 is inactive,
		// however the report on MAIN/project/test2 should not have changed yet because we have not rebased.
		assertEquals("MAIN/project/test2 report should be unchanged", reportProjectTest2, integrityService.findAllComponentsWithBadIntegrity(branchService.findLatest("MAIN/project/test2"), true));

		// Let's rebase MAIN/project/test2
		branchMergeService.mergeBranchSync("MAIN/project", "MAIN/project/test2", Collections.emptySet());

		// MAIN/project/test2 should now have a new issue because the stated relationship on concept 6 points to the inactive concept 5.
		IntegrityIssueReport reportProjectTest2Run2 = integrityService.findAllComponentsWithBadIntegrity(branchService.findLatest("MAIN/project/test2"), true);
		assertNotEquals("MAIN/project/test2 report should be unchanged", reportProjectTest2, reportProjectTest2Run2);
		assertNull(reportProjectTest2Run2.getRelationshipsWithMissingOrInactiveSource());
		assertEquals(1, reportProjectTest2Run2.getRelationshipsWithMissingOrInactiveType().size());
		assertEquals("There should be an extra rel with missing destination.", 3, reportProjectTest2Run2.getRelationshipsWithMissingOrInactiveDestination().size());
	}

	@Test
	/*
		Test the method that checks only the components changed on the branch.
		The purpose of this method is to check only what's changed for speed but to block promotion until changes are fixed.
	 */
	public void testFindChangedComponentsWithBadIntegrity() throws ServiceException {
		branchService.create("MAIN");
		branchService.create("MAIN/project");

		branchService.create("MAIN/project/test1");

		conceptService.create(new Concept("1"), "MAIN/project");
		// Valid relationship on MAIN/project
		conceptService.create(new Concept("101").addRelationship(new Relationship("101", "1")), "MAIN/project");
		// Valid relationship on MAIN/project
		conceptService.create(new Concept("2").addRelationship(new Relationship("101", "1")), "MAIN/project");
		// Missing Type on MAIN/project
		conceptService.create(new Concept("3").addRelationship(new Relationship("102", "2")), "MAIN/project");
		// Missing Destination on MAIN/project
		conceptService.create(new Concept("4").addRelationship(new Relationship("101", "1000")), "MAIN/project");
		// Valid relationship on MAIN/project
		conceptService.create(new Concept("5").addRelationship(new Relationship("101", "2")), "MAIN/project");

		branchService.create("MAIN/project/test2");
		// Valid relationship on MAIN/project/test2
		conceptService.create(new Concept("6").addRelationship(new Relationship("101", "5")), "MAIN/project/test2");
		// Missing Destination on MAIN/project/test2
		conceptService.create(new Concept("7").addRelationship(new Relationship("101", "1000")), "MAIN/project/test2");

		try {
			integrityService.findChangedComponentsWithBadIntegrity(branchService.findLatest("MAIN"));
			fail("We should never get to this line because we should throw an exception when attempting the incremental integrity check on MAIN - use the full check there!");
		} catch (Exception e) {
			// Pass
		}

		// Two bad relationships are on MAIN/project
		IntegrityIssueReport reportProject = integrityService.findChangedComponentsWithBadIntegrity(branchService.findLatest("MAIN/project"));
		assertNull(reportProject.getRelationshipsWithMissingOrInactiveSource());
		assertEquals(1, reportProject.getRelationshipsWithMissingOrInactiveType().size());
		assertEquals(1, reportProject.getRelationshipsWithMissingOrInactiveDestination().size());

		// MAIN/project/test1 was created before the bad relationships so there should be no issue on that branch
		IntegrityIssueReport reportProjectTest1 = integrityService.findChangedComponentsWithBadIntegrity(branchService.findLatest("MAIN/project/test1"));
		assertNull(reportProjectTest1.getRelationshipsWithMissingOrInactiveSource());
		assertNull(reportProjectTest1.getRelationshipsWithMissingOrInactiveType());
		assertNull(reportProjectTest1.getRelationshipsWithMissingOrInactiveDestination());

		// MAIN/project/test2 was created after the bad relationships so can see the issues on MAIN/project,
		// however this method only reports issues created on that branch so we are only expecting the 1 issue created on MAIN/project/test2 to be reported
		IntegrityIssueReport reportProjectTest2 = integrityService.findChangedComponentsWithBadIntegrity(branchService.findLatest("MAIN/project/test2"));
		assertNull(reportProjectTest2.getRelationshipsWithMissingOrInactiveSource());
		assertNull(reportProjectTest2.getRelationshipsWithMissingOrInactiveType());
		assertEquals(1, reportProjectTest2.getRelationshipsWithMissingOrInactiveDestination().size());

		// Let's make concept 5 inactive on MAIN/project
		conceptService.update((Concept) new Concept("5").setActive(false), "MAIN/project");

		// MAIN/project should have no new issues. Concept 5's relationship will not have a missing source concept because the relationship will have been deleted automatically

		// Still just two bad relationships are on MAIN/project
		assertEquals("MAIN/project report should be unchanged.", reportProject, integrityService.findChangedComponentsWithBadIntegrity(branchService.findLatest("MAIN/project")));

		// There is a relationship on MAIN/project/test2 which will break now that concept 5 is inactive,
		// however the report on MAIN/project/test2 should not have changed yet because we have not rebased.
		assertEquals("MAIN/project/test2 report should be unchanged", reportProjectTest2, integrityService.findChangedComponentsWithBadIntegrity(branchService.findLatest("MAIN/project/test2")));

		// Let's rebase MAIN/project/test2
		branchMergeService.mergeBranchSync("MAIN/project", "MAIN/project/test2", Collections.emptySet());

		// MAIN/project/test2 should now have a new issue because the stated relationship on concept 6 points to the inactive concept 5.
		// Although this method only reports changes on that branch and the concept was not made inactive on that branch because the relationship was created (or modified) on
		// that branch it will still be reported.
		IntegrityIssueReport reportProjectTest2Run2 = integrityService.findChangedComponentsWithBadIntegrity(branchService.findLatest("MAIN/project/test2"));
		assertNotEquals("MAIN/project/test2 report should be unchanged", reportProjectTest2, reportProjectTest2Run2);
		assertNull(reportProjectTest2Run2.getRelationshipsWithMissingOrInactiveSource());
		assertNull(reportProjectTest2Run2.getRelationshipsWithMissingOrInactiveType());
		assertEquals("There should be an extra rel with missing destination.", 2, reportProjectTest2Run2.getRelationshipsWithMissingOrInactiveDestination().size());
	}

}