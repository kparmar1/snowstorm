package org.snomed.snowstorm.core.data.services;

import com.google.common.collect.Lists;
import io.kaicode.elasticvc.api.BranchCriteria;
import io.kaicode.elasticvc.api.BranchService;
import io.kaicode.elasticvc.api.PathUtil;
import io.kaicode.elasticvc.api.VersionControlHelper;
import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.domain.Commit;
import io.kaicode.elasticvc.domain.DomainEntity;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.commons.lang.NotImplementedException;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snomed.snowstorm.config.SearchLanguagesConfiguration;
import org.snomed.snowstorm.core.data.domain.*;
import org.snomed.snowstorm.core.data.services.identifier.IdentifierService;
import org.snomed.snowstorm.core.util.DescriptionHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.data.util.CloseableIterator;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.kaicode.elasticvc.api.ComponentService.LARGE_PAGE;
import static java.lang.Long.parseLong;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;

@Service
public class AdminOperationsService {

	@Autowired
	private SearchLanguagesConfiguration searchLanguagesConfiguration;

	@Autowired
	private ElasticsearchOperations elasticsearchTemplate;

	@Autowired
	private VersionControlHelper versionControlHelper;

	@Autowired
	private BranchMergeService branchMergeService;

	@Autowired
	private BranchService branchService;

	@Autowired
	private DomainEntityConfiguration domainEntityConfiguration;

	@Autowired
	private ConceptUpdateHelper conceptUpdateHelper;

	private Logger logger = LoggerFactory.getLogger(getClass());

	public void reindexDescriptionsForLanguage(String languageCode) throws IOException {
		Map<String, Set<Character>> charactersNotFoldedSets = searchLanguagesConfiguration.getCharactersNotFoldedSets();
		Set<Character> foldedCharacters = charactersNotFoldedSets.getOrDefault(languageCode, Collections.emptySet());
		logger.info("Reindexing all description documents in version control with language code '{}' using {} folded characters.", languageCode, foldedCharacters.size());
		AtomicLong descriptionCount = new AtomicLong();
		AtomicLong descriptionUpdateCount = new AtomicLong();
		try (CloseableIterator<Description> descriptionsOnAllBranchesStream = elasticsearchTemplate.stream(new NativeSearchQueryBuilder()
						.withQuery(termQuery(Description.Fields.LANGUAGE_CODE, languageCode))
						.withSort(SortBuilders.fieldSort("internalId"))
						.withPageable(LARGE_PAGE)
						.build(),
				Description.class)) {

			List<UpdateQuery> updateQueries = new ArrayList<>();
			AtomicReference<IOException> exceptionThrown = new AtomicReference<>();
			descriptionsOnAllBranchesStream.forEachRemaining(description -> {
				if (exceptionThrown.get() == null) {

					String newFoldedTerm = DescriptionHelper.foldTerm(description.getTerm(), foldedCharacters);
					descriptionCount.incrementAndGet();
					if (!newFoldedTerm.equals(description.getTermFolded())) {
						UpdateRequest updateRequest = new UpdateRequest();
						try {
							updateRequest.doc(jsonBuilder()
									.startObject()
									.field(Description.Fields.TERM_FOLDED, newFoldedTerm)
									.endObject());
						} catch (IOException e) {
							exceptionThrown.set(e);
						}

						updateQueries.add(new UpdateQueryBuilder()
								.withClass(Description.class)
								.withId(description.getInternalId())
								.withUpdateRequest(updateRequest)
								.build());
						descriptionUpdateCount.incrementAndGet();
					}
					if (updateQueries.size() == 10_000) {
						logger.info("Bulk update {}", descriptionUpdateCount.get());
						elasticsearchTemplate.bulkUpdate(updateQueries);
						updateQueries.clear();
					}
				}
			});
			if (exceptionThrown.get() != null) {
				throw exceptionThrown.get();
			}
			if (!updateQueries.isEmpty()) {
				logger.info("Bulk update {}", descriptionUpdateCount.get());
				elasticsearchTemplate.bulkUpdate(updateQueries);
			}
		} finally {
			elasticsearchTemplate.refresh(Description.class);
		}
		logger.info("Completed reindexing of description documents with language code '{}'. Of the {} documents found {} were updated due to a character folding change.",
				languageCode, descriptionCount.get(), descriptionUpdateCount.get());
	}

	public Map<Class, Set<String>> findAndEndDonatedContent(String branch) {
		if (PathUtil.isRoot(branch)) {
			throw new IllegalArgumentException("Donated content should be ended on extension branch, not MAIN.");
		}

		logger.info("Finding and fixing donated content on {}.", branch);

		BranchCriteria branchCriteria = versionControlHelper.getBranchCriteria(branch);

		Map<Class, Set<String>> fixesApplied = new HashMap<>();
		branchMergeService.findAndEndDonatedComponentsOfAllTypes(branch, branchCriteria, fixesApplied);

		logger.info("Completed donated content fixing on {}.", branch);
		return fixesApplied;
	}

	public void rollbackCommit(String branchPath, long timepoint) {
		Branch branchVersion = branchService.findAtTimepointOrThrow(branchPath, new Date(timepoint));
		if (branchVersion.getEnd() != null) {
			throw new IllegalStateException(String.format("Branch %s at timepoint %s is already ended, it's not the latest commit.", branchPath, timepoint));
		}
		branchService.rollbackCompletedCommit(branchVersion, new ArrayList<>(domainEntityConfiguration.getAllDomainEntityTypes()));
	}

	public void restoreGroupNumberOfInactiveRelationships(String branchPath, String currentEffectiveTime, String previousReleaseBranch) {
		logger.info("Restoring group number of inactive relationships on branch {}.", branchPath);

		Map<Long, Relationship> inactiveRelationships = getAllInactiveRelationships(branchPath, currentEffectiveTime);
		logger.info("{} relationships inactive on this branch branch with effectiveTime {}.", inactiveRelationships.size(), currentEffectiveTime);

		BranchCriteria branchCriteria = versionControlHelper.getBranchCriteria(previousReleaseBranch);
		NativeSearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(
				boolQuery()
						.must(branchCriteria.getEntityBranchCriteria(Relationship.class))
						.must(termQuery(Relationship.Fields.ACTIVE, false))
						.must(termQuery(Relationship.Fields.CHARACTERISTIC_TYPE_ID, Concepts.INFERRED_RELATIONSHIP)))
				.withFilter(termsQuery(Relationship.Fields.RELATIONSHIP_ID, inactiveRelationships.keySet()))
				.withPageable(LARGE_PAGE)
				.build();

		List<Relationship> correctedRelationships = new ArrayList<>();
		try (CloseableIterator<Relationship> stream = elasticsearchTemplate.stream(searchQuery, Relationship.class)) {
			stream.forEachRemaining(relationshipInPreviousRelease -> {
				Relationship currentRelationship = inactiveRelationships.get(parseLong(relationshipInPreviousRelease.getRelationshipId()));
				if (currentRelationship.getGroupId() != relationshipInPreviousRelease.getGroupId()) {
					currentRelationship.setGroupId(relationshipInPreviousRelease.getGroupId());
					currentRelationship.copyReleaseDetails(relationshipInPreviousRelease);
					currentRelationship.markChanged();
					correctedRelationships.add(currentRelationship);
				}
			});
		}

		List<String> first10Ids = correctedRelationships.subList(0, correctedRelationships.size() > 10 ? 10 : correctedRelationships.size())
				.stream().map(Relationship::getRelationshipId).collect(Collectors.toList());

		logger.info("{} relationships found on the previous release branch {} with a different role group, examples: {}",
				correctedRelationships.size(), previousReleaseBranch, first10Ids);

		if (!correctedRelationships.isEmpty()) {
			try (Commit commit = branchService.openCommit(branchPath)) {
				for (List<Relationship> batch : Lists.partition(correctedRelationships, 1_000)) {
					logger.info("Correcting batch of {} relationships ...", batch.size());
					conceptUpdateHelper.doSaveBatchRelationships(batch, commit);
				}
				commit.markSuccessful();
			}
			logger.info("All inactive relationship groups restored.");
		}
	}

	private Map<Long, Relationship> getAllInactiveRelationships(String previousReleaseBranch, String effectiveTime) {
		Map<Long, Relationship> relationshipMap = new Long2ObjectOpenHashMap<>();
		try (CloseableIterator<Relationship> stream = elasticsearchTemplate.stream(new NativeSearchQueryBuilder()
				.withQuery(
						boolQuery()
								.must(versionControlHelper.getBranchCriteria(previousReleaseBranch).getEntityBranchCriteria(Relationship.class))
								.must(termQuery(Relationship.Fields.EFFECTIVE_TIME, effectiveTime))
								.must(termQuery(Relationship.Fields.CHARACTERISTIC_TYPE_ID, Concepts.INFERRED_RELATIONSHIP))
								.must(termQuery(Relationship.Fields.ACTIVE, false))
						)
				.withPageable(LARGE_PAGE)
				.build(), Relationship.class)) {
			stream.forEachRemaining(relationship -> {
				relationshipMap.put(parseLong(relationship.getRelationshipId()), relationship);
				if (relationshipMap.size() % 10_000 == 0) {
					System.out.print(".");
				}
			});
			System.out.println();
		}
		return relationshipMap;
	}

	public void hardDeleteBranch(String path) {
		Branch branch = branchService.findBranchOrThrow(path);
		if (PathUtil.isRoot(path)) {
			throw new IllegalArgumentException("The root branch can not be deleted.");
		}
		int childrenCount = branchService.findChildren(path).size();
		if (childrenCount != 0) {
			throw new IllegalStateException(String.format("Branch '%s' can not be deleted because is has children (%s).", path, childrenCount));
		}

		if (branch.isLocked()) {
			branchService.unlock(path);
		}
		branchService.lockBranch(path, "Deleting branch.");

		logger.info("Deleting all documents on branch {}.", path);
		DeleteQuery deleteQuery = new DeleteQuery();
		deleteQuery.setQuery(QueryBuilders.termQuery("path", path));
		for (Class<? extends DomainEntity> domainEntityType : domainEntityConfiguration.getAllDomainEntityTypes()) {
			logger.info("Deleting all {} type documents on branch {}.", domainEntityType.getSimpleName(), path);
			elasticsearchTemplate.delete(deleteQuery, domainEntityType);
			elasticsearchTemplate.refresh(domainEntityType);
		}

		logger.info("Deleting branch documents for path {}.", path);
		elasticsearchTemplate.delete(deleteQuery, Branch.class);
		elasticsearchTemplate.refresh(Branch.class);
	}

	public void fixEffectiveTime(Set<String> componentIds, String branchPath) {
		Set<String> concepts = new HashSet<>();
		Set<String> descriptions = new HashSet<>();
		Set<String> relationships = new HashSet<>();
		Set<String> members = new HashSet<>();
		for (String componentId : componentIds) {
			if (IdentifierService.isConceptId(componentId)) {
				concepts.add(componentId);
			} else if (IdentifierService.isDescriptionId(componentId)) {
				descriptions.add(componentId);
			} else if (IdentifierService.isRelationshipId(componentId)) {
				relationships.add(componentId);
			} else if (componentId.length() == 36 && componentId.contains("-")) {
				members.add(componentId);
			}
		}
		if (!concepts.isEmpty()) {
			throw new NotImplementedException("Fix for concepts is not yet implemented.");
		}
		if (!members.isEmpty()) {
			throw new NotImplementedException("Fix for members is not yet implemented.");
		}
		if (descriptions.isEmpty() && relationships.isEmpty()) {
			throw new IllegalArgumentException("No identifiers given a recognisable format.");
		}

		CodeSystem codeSystem = codeSystemService.findCodeSystemApplicableToBranch(branchPath);
		if (codeSystem == null) {
			throw new IllegalStateException("No code system found for branch.");
		}
		CodeSystemVersion latestVersion = codeSystemService.findLatestImportedVersion(codeSystem.getShortName());
		if (latestVersion == null) {
			throw new IllegalStateException("No code system version found for branch.");
		}
		String releaseBranch = latestVersion.getBranchPath();

		Set<Description> descriptionsToPersist = attemptFix(descriptions, "Description",
				id -> descriptionService.findDescription(branchPath, id), id -> descriptionService.findDescription(releaseBranch, id));
		Set<Relationship> relationshipsToPersist = attemptFix(relationships, "Relationship",
				id -> relationshipService.findRelationship(branchPath, id), id -> relationshipService.findRelationship(releaseBranch, id));

		if (!descriptionsToPersist.isEmpty() || !relationshipsToPersist.isEmpty()) {
			try (Commit commit = branchService.openCommit(branchPath)) {
				if (!descriptionsToPersist.isEmpty()) {
					conceptUpdateHelper.doSaveBatchDescriptions(descriptionsToPersist, commit);
				}
				if (!relationshipsToPersist.isEmpty()) {
					conceptUpdateHelper.doSaveBatchRelationships(relationshipsToPersist, commit);
				}
				commit.markSuccessful();
			}
		}
		if (!descriptions.isEmpty()) {
			logger.info("Effective time fix attempted on {} descriptions, fix applied to {} descriptions on {}.", descriptions.size(), descriptionsToPersist.size(), branchPath);
		}
		if (!relationships.isEmpty()) {
			logger.info("Effective time fix attempted on {} relationships, fix applied to {} relationships on {}.", relationships.size(), relationshipsToPersist.size(), branchPath);
		}
	}

	@SuppressWarnings("unchecked")
	private <T extends SnomedComponent> Set<T> attemptFix(Set<String> componentIds, final String typeName,
			Function<String, T> branchComponentSupplier, Function<String, T> releasedComponentSupplier) {

		Set<T> componentsToPersist = new HashSet<>();
		for (String descriptionId : componentIds) {
			T descriptionToFix = branchComponentSupplier.apply(descriptionId);
			if (descriptionToFix == null) {
				throw new IllegalStateException(String.format(typeName + " %s not found on specified branch.", descriptionId));
			}
			T descriptionReleased = releasedComponentSupplier.apply(descriptionId);
			if (descriptionReleased == null) {
				throw new IllegalStateException(String.format(typeName + " %s not found on release branch.", descriptionId));
			}
			if (descriptionToFix.getEffectiveTimeI() == null && !descriptionToFix.isComponentChanged(descriptionReleased)) {
				// description is the same as the release branch, restore effectiveTime
				descriptionToFix.copyReleaseDetails(descriptionReleased);
				descriptionToFix.updateEffectiveTime();
				if (descriptionToFix.getEffectiveTimeI() != null) {
					descriptionToFix.markChanged();
					componentsToPersist.add(descriptionToFix);
				}
			}
		}
		return componentsToPersist;
	}

	@Autowired
	private CodeSystemService codeSystemService;

	@Autowired
	private DescriptionService descriptionService;

	@Autowired
	private RelationshipService relationshipService;
}
