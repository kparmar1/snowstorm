package org.snomed.snowstorm.validation;

import io.kaicode.elasticvc.api.VersionControlHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.snomed.snowstorm.AbstractTest;
import org.snomed.snowstorm.TestConfig;
import org.snomed.snowstorm.core.data.domain.Concept;
import org.snomed.snowstorm.core.data.domain.Concepts;
import org.snomed.snowstorm.core.data.domain.Relationship;
import org.snomed.snowstorm.core.data.services.ConceptService;
import org.snomed.snowstorm.core.data.services.QueryService;
import org.snomed.snowstorm.core.data.services.ServiceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfig.class)
public class RelationshipDroolsValidationServiceTest extends AbstractTest {

	@Autowired
	private ConceptService conceptService;

	@Autowired
	private VersionControlHelper versionControlHelper;

	@Autowired
	private QueryService queryService;

	private RelationshipDroolsValidationService service;

	@Before
	public void setup() throws ServiceException {
		String branchPath = "MAIN";
		conceptService.create(new Concept("100001").addRelationship(
				new Relationship(Concepts.ISA, Concepts.SNOMEDCT_ROOT)),
				branchPath);

		service = new RelationshipDroolsValidationService(branchPath, versionControlHelper.getBranchCriteria(branchPath), queryService);
	}

	@Test
	public void hasActiveInboundStatedRelationship() throws Exception {
		Assert.assertTrue(service.hasActiveInboundStatedRelationship(Concepts.SNOMEDCT_ROOT, Concepts.ISA));
		Assert.assertFalse(service.hasActiveInboundStatedRelationship(Concepts.SNOMEDCT_ROOT, "10000123"));
		Assert.assertFalse(service.hasActiveInboundStatedRelationship("100002", Concepts.ISA));
	}

}
