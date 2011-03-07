package org.phenoscape.obd.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.phenoscape.obd.model.Character;
import org.phenoscape.obd.model.DefaultTerm;
import org.phenoscape.obd.model.GeneAnnotation;
import org.phenoscape.obd.model.GeneTerm;
import org.phenoscape.obd.model.LinkedTerm;
import org.phenoscape.obd.model.Matrix;
import org.phenoscape.obd.model.OTU;
import org.phenoscape.obd.model.PublicationTerm;
import org.phenoscape.obd.model.Relationship;
import org.phenoscape.obd.model.SimpleTerm;
import org.phenoscape.obd.model.Specimen;
import org.phenoscape.obd.model.Synonym;
import org.phenoscape.obd.model.Synonym.SCOPE;
import org.phenoscape.obd.model.TaxonAnnotation;
import org.phenoscape.obd.model.TaxonTerm;
import org.phenoscape.obd.model.Term;
import org.phenoscape.obd.model.Vocab.CDAO;
import org.phenoscape.obd.model.Vocab.OBO;
import org.phenoscape.obd.model.Vocab.PATO;
import org.phenoscape.obd.model.Vocab.PHENOSCAPE;
import org.phenoscape.obd.query.SearchHit.MatchType;

import com.eekboom.utils.Strings;

public class PhenoscapeDataStore {

    private final DataSource dataSource;
    public enum POSTCOMP_OPTION { STRUCTURE, SEMANTIC_LABEL, SIMPLE_LABEL, NONE };
    /**
     * Mapping for how to represent relations used in post-comp differentia when generating a human-readable label.
     * If the relation is not in this map, use "of".
     */
    private static final Map<String, String> POSTCOMP_RELATIONS = new HashMap<String, String>();
    static {
        POSTCOMP_RELATIONS.put("OBO_REL:connected_to", "on");
        POSTCOMP_RELATIONS.put("connected_to", "on");
        POSTCOMP_RELATIONS.put("anterior_to", "anterior to");
        POSTCOMP_RELATIONS.put("BSPO:0000096", "anterior to");
        POSTCOMP_RELATIONS.put("posterior_to", "posterior to");
        POSTCOMP_RELATIONS.put("BSPO:0000099", "posterior to");
        POSTCOMP_RELATIONS.put("adjacent_to", "adjacent to");
        POSTCOMP_RELATIONS.put("OBO_REL:adjacent_to", "adjacent to");
        POSTCOMP_RELATIONS.put(PATO.INCREASED_IN_MAGNITUDE_RELATIVE_TO, "increased in magnitude relative to");
        POSTCOMP_RELATIONS.put(PATO.DECREASED_IN_MAGNITUDE_RELATIVE_TO, "decreased in magnitude relative to");
        POSTCOMP_RELATIONS.put(PATO.SIMILAR_IN_MAGNITUDE_RELATIVE_TO, "similar in magnitude relative to");
        POSTCOMP_RELATIONS.put(PHENOSCAPE.COMPLEMENT_OF, "not");
    }

    public PhenoscapeDataStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Returns the date on which the Knowledgebase data were loaded.
     */
    public Date getRefreshDate() throws SQLException {
        final DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
        //TODO don't put date in "notes" column
        final SimpleQuery query = new SimpleQuery("SELECT notes from obd_schema_metadata");
        final QueryExecutor<Date> executor = new QueryExecutor<Date>(this.dataSource, query) {
            @Override
            public Date processResult(ResultSet result) throws SQLException {
                try {
                    while (result.next()) {
                        final String date = result.getString("notes");
                        if (date != null) {
                            return formatter.parse(result.getString("notes"));    
                        }
                    }
                    // if a date was not found, return dummy date
                    return formatter.parse("1859-11-24_00:00:00");
                } catch (ParseException e) {
                    throw new SQLException(e);
                }
            }
        };
        return executor.executeQuery();
    }

    public Term getTerm(String uid) throws SQLException {
        return this.queryForTerm(uid);
    }

    public LinkedTerm getLinkedTerm(String uid) throws SQLException {
        final DefaultTerm term = this.queryForTerm(uid);
        if (term != null) {
            this.addLinksToTerm(term);    
        }
        return term;
    }

    private DefaultTerm queryForTerm(String uid) throws SQLException {
        final QueryBuilder query = new TermQueryBuilder(uid);
        return (new QueryExecutor<DefaultTerm>(this.dataSource, query) {
            @Override
            public DefaultTerm processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    final DefaultTerm term = createTerm(result);
                    return term;
                }
                return null;
            }}).executeQuery();
    }

    private DefaultTerm createTerm(ResultSet result) throws SQLException {
        final DefaultTerm term = new DefaultTerm(result.getInt("node_id"), result.getInt("source_id"));
        term.setUID(result.getString("uid"));
        term.setLabel(result.getString("label"));
        term.setDefinition(result.getString("definition"));
        term.setComment(result.getString("comment"));
        term.setSource(new SimpleTerm(result.getString("source_uid"), result.getString("source_label")));
        this.addSynonymsToTerm(term);
        this.addXrefsToTerm(term);
        return term;
    }

    private void addLinksToTerm(DefaultTerm term) throws SQLException {
        final QueryBuilder parentsQuery = new TermLinkSubjectQueryBuilder(term);
        final Set<Relationship> parents = (new QueryExecutor<Set<Relationship>>(this.dataSource, parentsQuery) {
            @Override
            public Set<Relationship> processResult(ResultSet result) throws SQLException {
                final Set<Relationship> parentResults = new HashSet<Relationship>();
                while (result.next()) {
                    parentResults.add(createRelationship(result));
                }
                return parentResults;
            }
        }).executeQuery();
        for (Relationship parent : parents) {
            term.addSubjectLink(parent);
        }
        final QueryBuilder childrenQuery = new TermLinkObjectQueryBuilder(term);
        final Set<Relationship> children = (new QueryExecutor<Set<Relationship>>(this.dataSource, childrenQuery) {
            @Override
            public Set<Relationship> processResult(ResultSet result) throws SQLException {
                final Set<Relationship> childrenResults = new HashSet<Relationship>();
                while (result.next()) {
                    childrenResults.add(createRelationship(result));
                }
                return childrenResults;
            }
        }).executeQuery();
        for (Relationship child : children) {
            term.addObjectLink(child);
        }
    }

    private Relationship createRelationship(ResultSet result) throws SQLException {
        final Relationship relationship = new Relationship();
        final DefaultTerm otherTerm = new DefaultTerm(result.getInt("other_node_id"), null);
        otherTerm.setUID(result.getString("other_uid"));
        otherTerm.setLabel(result.getString("other_label"));
        relationship.setOther(otherTerm);
        final DefaultTerm predicate = new DefaultTerm(result.getInt("relation_node_id"), null);
        predicate.setUID(result.getString("relation_uid"));
        predicate.setLabel(result.getString("relation_label"));
        relationship.setPredicate(predicate);
        return relationship;
    }

    /**
     * Return a TaxonTerm object for the given UID. Returns null if no taxon with that UID exists. 
     * The TaxonTerm will include references to its synonyms, parent, and children taxa.
     */
    public TaxonTerm getTaxonTerm(final String uid, final boolean includeChildren, final boolean includeSynonymsAndXrefs) throws SQLException {
        //TODO add order and family to TaxonTerms?
        final QueryBuilder query = new TaxonQueryBuilder(uid);
        final TaxonTerm taxonTerm = (new QueryExecutor<TaxonTerm>(this.dataSource, query) {
            @Override
            public TaxonTerm processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    final TaxonTerm taxon = createTaxonTermWithProperties(result);
                    taxon.setSource(new SimpleTerm(result.getString("source_uid"), result.getString("source_label")));
                    if (result.getString("parent_uid") != null) {
                        final TaxonTerm parent = new TaxonTerm(result.getInt("parent_node_id"), null);
                        parent.setUID(result.getString("parent_uid"));
                        parent.setLabel(result.getString("parent_label"));
                        parent.setExtinct(result.getBoolean("parent_is_extinct"));
                        if (result.getString("parent_rank_uid") != null) {
                            final Term parentRank = new DefaultTerm(result.getInt("parent_rank_node_id"), null);
                            parentRank.setUID(result.getString("parent_rank_uid"));
                            parentRank.setLabel(result.getString("parent_rank_label"));
                            parent.setRank(parentRank);
                        }
                        taxon.setParent(parent);
                    }
                    if (includeChildren) {
                        addChildrenToTaxon(taxon);
                        taxon.setSpeciesCount(getSpeciesCountForTaxon(uid));
                    }
                    if (includeSynonymsAndXrefs) { 
                        addSynonymsToTerm(taxon);
                        addXrefsToTerm(taxon);
                    }
                    return taxon;
                }
                //no taxon with this ID
                return null;
            }}).executeQuery();
        return taxonTerm;
    }

    public int getSpeciesCountForTaxon(String uid) throws SQLException {
        final QueryBuilder query = new SpeciesCountQueryBuilder(uid);
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return result.getInt("species_count");
                }
                return 0;
            }}).executeQuery(); 
    }

    private void addChildrenToTaxon(TaxonTerm taxon) throws SQLException {
        final QueryBuilder childrenQuery = new TaxonChildrenQueryBuilder(taxon);
        final Set<TaxonTerm> children = (new QueryExecutor<Set<TaxonTerm>>(dataSource, childrenQuery) {
            @Override
            public Set<TaxonTerm> processResult(ResultSet result) throws SQLException {
                final Set<TaxonTerm> children = new HashSet<TaxonTerm>();
                while (result.next()) {
                    children.add(createTaxonTermWithProperties(result));
                }
                return children;
            }
        }).executeQuery();
        for (TaxonTerm child : children) {
            taxon.addChild(child);
        }
    }

    /**
     * Creates a new TaxonTerm and extracts its uid, label, isExtinct, and rank from the ResultSet
     */
    private TaxonTerm createTaxonTermWithProperties(ResultSet result) throws SQLException {
        final TaxonTerm taxon = new TaxonTerm(result.getInt("node_id"), null);
        taxon.setUID(result.getString("uid"));
        taxon.setLabel(result.getString("label"));
        taxon.setExtinct(result.getBoolean("is_extinct"));
        if (result.getString("rank_uid") != null) {
            final Term rank = new DefaultTerm(result.getInt("rank_node_id"), null);
            rank.setUID(result.getString("rank_uid"));
            rank.setLabel(result.getString("rank_label"));
            taxon.setRank(rank);
        }
        return taxon;
    }

    private void addSynonymsToTerm(DefaultTerm term) throws SQLException {
        final QueryBuilder query = new SynonymsQueryBuilder(term);
        final Set<Synonym> synonyms = (new QueryExecutor<Set<Synonym>>(this.dataSource, query) {
            @Override
            public Set<Synonym> processResult(ResultSet result) throws SQLException {
                final Set<Synonym> synonyms = new HashSet<Synonym>();
                while (result.next()) {
                    synonyms.add(createSynonym(result));
                }
                return synonyms;
            }
        }).executeQuery();
        for (Synonym synonym : synonyms) {
            term.addSynonym(synonym);
        }
    }

    private Synonym createSynonym(ResultSet result) throws SQLException {
        final Synonym synonym = new Synonym();
        synonym.setLabel(result.getString("label"));
        if (result.getString("scope") != null) {
            final String scope = result.getString("scope");
            if (scope.equals("B")) { synonym.setScope(SCOPE.BROAD); }
            else if (scope.equals("N")) { synonym.setScope(SCOPE.NARROW); }
            else if (scope.equals("E")) { synonym.setScope(SCOPE.EXACT); }
            else if (scope.equals("R")) { synonym.setScope(SCOPE.RELATED); }
        }
        if (result.getString("type_uid") != null) {
            synonym.setType(new SimpleTerm(result.getString("type_uid"), null));
        }
        return synonym;
    }

    private void addXrefsToTerm(DefaultTerm term) throws SQLException {
        final QueryBuilder query = new XrefsQueryBuilder(term);
        final Set<Term> xrefs = (new QueryExecutor<Set<Term>>(this.dataSource, query) {
            @Override
            public Set<Term> processResult(ResultSet result) throws SQLException {
                final Set<Term> xrefs = new HashSet<Term>();
                while (result.next()) {
                    xrefs.add(new SimpleTerm(result.getString("xref_uid"), null));
                }
                return xrefs;
            }
        }).executeQuery();
        for (Term xref : xrefs) {
            term.addXref(xref);
        }
    }

    public List<LinkedTerm> getPathForTerm(String uid) throws SQLException {
        final LinkedTerm term = this.getLinkedTerm(uid);
        if (term != null) {
            for (Relationship relationship : term.getSubjectLinks()) {
                if (relationship.getPredicate().getUID().equals(OBO.IS_A)) {
                    final List<LinkedTerm> terms = this.getPathForTerm(relationship.getOther().getUID());
                    terms.add(term);
                    return terms;
                }
            }
            final List<LinkedTerm> terms = new ArrayList<LinkedTerm>();
            terms.add(term);
            return terms;
        } else {
            return Collections.emptyList();
        }
    }

    public PublicationTerm getPublicationTerm(String uid) throws SQLException {
        final QueryBuilder query = new PublicationTermQueryBuilder(uid);
        return (new QueryExecutor<PublicationTerm>(this.dataSource, query) {
            @Override
            public PublicationTerm processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    final PublicationTerm publication = new PublicationTerm(result.getInt("node_id"), result.getInt("source_id"));
                    publication.setUID(result.getString("uid"));
                    publication.setLabel(result.getString("label"));
                    publication.setSource(new SimpleTerm(result.getString("source_uid"), result.getString("source_label")));
                    publication.setCitation(result.getString("citation_label"));
                    publication.setAbstractText(result.getString("abstract_label"));
                    publication.setDoi(result.getString("doi"));
                    return publication;
                }
                //no publication with this ID
                return null;
            }}).executeQuery();
    }

    public int getCountOfCuratedTaxonomicAnnotations(AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new CuratedTaxonomicAnnotationsQueryBuilder(config, true);
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        }).executeQuery();
    }
    
    public List<String> getDistinctPhenotypes(final AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new PhenotypeQueryBuilder(config, false);
        return (new QueryExecutor<List<String>>(this.dataSource, query) {
            @Override
            public List<String> processResult(ResultSet result) throws SQLException {
                final List<String> phenotypeIDs = new ArrayList<String>();
                while (result.next()) {
                    phenotypeIDs.add(result.getString("uid"));
                }
                return phenotypeIDs;
            }
        }).executeQuery();
    }

    public List<TaxonAnnotation> getDistinctTaxonAnnotations(final AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new DistinctTaxonomicAnnotationsQueryBuilder(config, false);
        return (new QueryExecutor<List<TaxonAnnotation>>(this.dataSource, query) {
            @Override
            public List<TaxonAnnotation> processResult(ResultSet result) throws SQLException {
                final List<TaxonAnnotation> annotations = new ArrayList<TaxonAnnotation>();
                while (result.next()) {
                    annotations.add(createTaxonAnnotation(result, config.getPostcompositionOption()));
                }
                return annotations;
            }
        }).executeQuery();
    }

    private TaxonAnnotation createTaxonAnnotation(ResultSet result, POSTCOMP_OPTION option) throws SQLException {
        final TaxonAnnotation annotation = new TaxonAnnotation();
        final TaxonTerm taxon = new TaxonTerm(result.getInt("taxon_node_id"), null);
        taxon.setUID(result.getString("taxon_uid"));
        taxon.setLabel(result.getString("taxon_label"));
        final Term rank = new SimpleTerm(result.getString("taxon_rank_uid"), result.getString("taxon_rank_label"));
        if (rank.getUID() != null) {
            taxon.setRank(rank);
        }
        taxon.setExtinct(result.getBoolean("taxon_is_extinct"));
        annotation.setTaxon(taxon);
        annotation.setEntity(this.createBasicTerm(result.getString("entity_uid"), result.getString("entity_label"), option));
        annotation.setQuality(this.createBasicTerm(result.getString("quality_uid"), result.getString("quality_label"), option));
        final String relatedEntityUID = result.getString("related_entity_uid");
        if (relatedEntityUID != null) {
            annotation.setRelatedEntity(this.createBasicTerm(relatedEntityUID, result.getString("related_entity_label"), option));
        }
        return annotation;
    }

    private TaxonAnnotation createSupportingTaxonAnnotation(ResultSet result, POSTCOMP_OPTION option) throws SQLException {
        final TaxonAnnotation annotation = this.createTaxonAnnotation(result, option);
        annotation.setPublication(new SimpleTerm(result.getString("publication_uid"), result.getString("publication_label")));
        annotation.setOtu(new SimpleTerm(result.getString("otu_uid"), result.getString("otu_label")));
        final Character character = new Character(null, result.getString("character_label"), result.getString("character_number"));
        annotation.setCharacter(character);
        annotation.setState(new SimpleTerm(null, result.getString("state_label")));
        return annotation;
    }

    public int getCountOfDistinctTaxonomicAnnotations(AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new DistinctTaxonomicAnnotationsQueryBuilder(config, true);
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        }).executeQuery();
    }

    public List<TaxonAnnotation> getSupportingTaxonomicAnnotations(final AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new SupportingTaxonomicAnnotationsQueryBuilder(config);
        return (new QueryExecutor<List<TaxonAnnotation>>(this.dataSource, query) {
            @Override
            public List<TaxonAnnotation> processResult(ResultSet result) throws SQLException {
                final List<TaxonAnnotation> annotations = new ArrayList<TaxonAnnotation>();
                while (result.next()) {
                    annotations.add(createSupportingTaxonAnnotation(result, config.getPostcompositionOption()));
                }
                return annotations;
            }
        }).executeQuery();
    }

    public List<TaxonTerm> getAnnotatedTaxa(AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new AnnotatedTaxaQueryBuilder(config, false);
        return (new QueryExecutor<List<TaxonTerm>>(this.dataSource, query) {
            @Override
            public List<TaxonTerm> processResult(ResultSet result) throws SQLException {
                final List<TaxonTerm> taxa = new ArrayList<TaxonTerm>();
                while (result.next()) {
                    final TaxonTerm taxon = new TaxonTerm(result.getInt("node_id"), null);
                    taxon.setUID(result.getString("uid"));
                    taxon.setLabel(result.getString("label"));
                    taxon.setExtinct(result.getBoolean("is_extinct"));
                    if (result.getString("rank_uid") != null) {
                        final Term rank = new DefaultTerm(result.getInt("rank_node_id"), null);
                        rank.setUID(result.getString("rank_uid"));
                        rank.setLabel(result.getString("rank_label"));
                        taxon.setRank(rank);
                    }
                    if (result.getString("family_uid") != null) {
                        final TaxonTerm family = new TaxonTerm(result.getInt("family_node_id"), null);
                        family.setUID(result.getString("family_uid"));
                        family.setLabel(result.getString("family_label"));
                        family.setExtinct(result.getBoolean("family_is_extinct"));
                        taxon.setTaxonomicFamily(family);
                    }
                    if (result.getString("order_uid") != null) {
                        final TaxonTerm order = new TaxonTerm(result.getInt("order_node_id"), null);
                        order.setUID(result.getString("order_uid"));
                        order.setLabel(result.getString("order_label"));
                        order.setExtinct(result.getBoolean("order_is_extinct"));
                        taxon.setTaxonomicOrder(order);
                    }
                    taxa.add(taxon);
                }
                return taxa;
            }
        }).executeQuery();
    }

    public int getCountOfAnnotatedTaxa(AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new AnnotatedTaxaQueryBuilder(config, true);
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        }).executeQuery();
    }

    public List<GeneAnnotation> getSupportingGenotypeAnnotations(final AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new SupportingGenotypeAnnotationsQueryBuilder(config);
        return (new QueryExecutor<List<GeneAnnotation>>(this.dataSource, query) {
            @Override
            public List<GeneAnnotation> processResult(ResultSet result) throws SQLException {
                final List<GeneAnnotation> annotations = new ArrayList<GeneAnnotation>();
                while (result.next()) {
                    annotations.add(createSupportingGenotypeAnnotation(result, config.getPostcompositionOption()));
                }
                return annotations;
            }
        }).executeQuery();
    }

    public GeneAnnotation createSupportingGenotypeAnnotation(ResultSet result, POSTCOMP_OPTION option) throws SQLException {
        final GeneAnnotation annotation = this.createGeneAnnotation(result, option);
        annotation.setGenotype(new SimpleTerm(result.getString("genotype_uid"), result.getString("genotype_label")));
        annotation.setGenotypeClass(new SimpleTerm(result.getString("type_uid"), result.getString("type_label")));
        annotation.setPublication(new SimpleTerm(result.getString("publication_uid"), result.getString("publication_label")));
        return annotation;
    }

    public List<GeneAnnotation> getGeneAnnotations(final AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new GeneAnnotationsQueryBuilder(config, false);
        return (new QueryExecutor<List<GeneAnnotation>>(this.dataSource, query) {
            @Override
            public List<GeneAnnotation> processResult(ResultSet result) throws SQLException {
                final List<GeneAnnotation> annotations = new ArrayList<GeneAnnotation>();
                while (result.next()) {
                    annotations.add(createGeneAnnotation(result, config.getPostcompositionOption()));
                }
                return annotations;
            }
        }).executeQuery();
    }

    private GeneAnnotation createGeneAnnotation(ResultSet result, POSTCOMP_OPTION option) throws SQLException {
        final GeneAnnotation annotation = new GeneAnnotation();
        final GeneTerm gene = new GeneTerm(result.getInt("gene_node_id"), null);
        gene.setUID(result.getString("gene_uid"));
        gene.setLabel(result.getString("gene_label"));
        annotation.setGene(gene);
        annotation.setEntity(this.createBasicTerm(result.getString("entity_uid"), result.getString("entity_label"), option));
        annotation.setQuality(this.createBasicTerm(result.getString("quality_uid"), result.getString("quality_label"), option));
        final String relatedEntityUID = result.getString("related_entity_uid");
        if (relatedEntityUID != null) {
            annotation.setRelatedEntity(this.createBasicTerm(result.getString("related_entity_uid"), result.getString("related_entity_label"), option));    
        }
        return annotation;
    }

    public int getCountOfGeneAnnotations(AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new GeneAnnotationsQueryBuilder(config, true);
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        }).executeQuery();
    }

    public int getCountOfGenotypeAnnotations(AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new GenotypeAnnotationsQueryBuilder(config, true);
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        }).executeQuery();
    }

    public List<GeneTerm> getAnnotatedGenes(AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new AnnotatedGenesQueryBuilder(config, false);
        return (new QueryExecutor<List<GeneTerm>>(this.dataSource, query) {
            @Override
            public List<GeneTerm> processResult(ResultSet result) throws SQLException {
                final List<GeneTerm> genes = new ArrayList<GeneTerm>();
                while (result.next()) {
                    final GeneTerm gene = new GeneTerm(result.getInt("gene_node_id"), null);
                    gene.setUID(result.getString("gene_uid"));
                    gene.setLabel(result.getString("gene_label"));
                    gene.setFullName(result.getString("gene_full_name"));
                    genes.add(gene);
                }
                return genes;
            }
        }).executeQuery();
    }

    public int getCountOfAnnotatedGenes(AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new AnnotatedGenesQueryBuilder(config, true);
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        }).executeQuery();
    }

    public int getCountOfDistinctPhenotypesAnnotatedToGenes(AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new DistinctGenePhenotypesQueryBuilder(config, true);
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        }).executeQuery();
    }

    public int getCountOfDistinctPhenotypesAnnotatedToTaxa(AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new DistinctTaxonPhenotypesQueryBuilder(config, true);
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        }).executeQuery();
    }

    public List<Term> getAnnotatedPublications(AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new PublicationsQueryBuilder(config, false);
        return (new QueryExecutor<List<Term>>(this.dataSource, query) {
            @Override
            public List<Term> processResult(ResultSet result) throws SQLException {
                final List<Term> annotations = new ArrayList<Term>();
                while (result.next()) {
                    annotations.add(createPublicationTerm(result));
                }
                return annotations;
            }
        }).executeQuery();
    }

    public int getCountOfAnnotatedPublications(AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new PublicationsQueryBuilder(config, true);
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        }).executeQuery();
    }

    public List<Term> getAnnotatedCharacters(AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new CharactersQueryBuilder(config, false);
        return (new QueryExecutor<List<Term>>(this.dataSource, query) {
            @Override
            public List<Term> processResult(ResultSet result) throws SQLException {
                final List<Term> annotations = new ArrayList<Term>();
                while (result.next()) {
                    annotations.add(createCharacterTerm(result));
                }
                return annotations;
            }
        }).executeQuery();
    }

    public int getCountOfAnnotatedCharacters(AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new CharactersQueryBuilder(config, true);
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        }).executeQuery();
    }

    private Term createCharacterTerm(ResultSet result) throws SQLException {
        //TODO should create a Character object with character number, etc.
        final DefaultTerm term = new DefaultTerm(result.getInt("character_node_id"), null);
        term.setLabel(result.getString("character_label"));
        return term;
    }

    public int getCountOfAllCharacters() throws SQLException {
        final String instanceOf = String.format(QueryBuilder.NODE_S, OBO.INSTANCE_OF);
        final String characterType = String.format(QueryBuilder.NODE_S, CDAO.CHARACTER);
        final QueryBuilder query = new SimpleQuery(String.format("SELECT count(DISTINCT uid) FROM node JOIN link ON (link.node_id = node.node_id AND link.predicate_id = %s AND link.object_id = %s)", instanceOf, characterType));
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        }).executeQuery();
    }

    public int getCountOfAnnotatedCharacterStates(AnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new CharacterStatesQueryBuilder(config, true);
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        }).executeQuery();
    }

    public int getCountOfAllCharacterStates() throws SQLException {
        final String instanceOf = String.format(QueryBuilder.NODE_S, OBO.INSTANCE_OF);
        final String characterStateType = String.format(QueryBuilder.NODE_S, CDAO.CHARACTER_STATE);
        final QueryBuilder query = new SimpleQuery(String.format("SELECT count(DISTINCT uid) FROM node JOIN link ON (link.node_id = node.node_id AND link.predicate_id = %s AND link.object_id = %s)", instanceOf, characterStateType));
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        }).executeQuery();
    }

    public int getCountOfAllOTUs() throws SQLException {
        final String instanceOf = String.format(QueryBuilder.NODE_S, OBO.INSTANCE_OF);
        final String characterType = String.format(QueryBuilder.NODE_S, CDAO.OTU);
        final QueryBuilder query = new SimpleQuery(String.format("SELECT count(DISTINCT uid) FROM node JOIN link ON (link.node_id = node.node_id AND link.predicate_id = %s AND link.object_id = %s)", instanceOf, characterType));
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        }).executeQuery();
    }

    public int getCountOfAllCuratedPhenotypes() throws SQLException {
        final String hasPhenotype = String.format(QueryBuilder.NODE_S, CDAO.HAS_PHENOTYPE);
        final QueryBuilder query = new SimpleQuery(String.format("SELECT count(*) FROM link WHERE link.predicate_id = %s AND link.is_inferred = false", hasPhenotype));
        return (new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        }).executeQuery();
    }

    private Term createPublicationTerm(ResultSet result) throws SQLException {
        final DefaultTerm term = new DefaultTerm(result.getInt("node_id"), null);
        term.setUID(result.getString("uid"));
        term.setLabel(result.getString("label"));
        return term;
    }

    public AutocompleteResult getAutocompleteMatches(final SearchConfig config) throws SQLException {
        final AutocompleteResult matches = new AutocompleteResult(config);
        if (config.getNamespaces().isEmpty()) {
            log().warn("No namespaces provided for autocomplete search. Returning empty result.");
            return matches;
        }
        final List<SearchHit> sortedHits = new ArrayList<SearchHit>();
        if (config.searchNames()) {
            final Collection<SearchHit> nameMatches = this.queryNameMatches(config, false);
            sortedHits.addAll(nameMatches);    
        }
        if (config.searchSynonyms()) {
            final Collection<SearchHit> synonymMatches = this.querySynonymMatches(config, false);
            sortedHits.addAll(synonymMatches);
        }
        Collections.sort(sortedHits, new Comparator<SearchHit>() {
            @Override
            public int compare(SearchHit a, SearchHit b) {
                final boolean aStartsWithMatch = a.getMatchText().toLowerCase().startsWith(config.getSearchText().toLowerCase());
                final boolean bStartsWithMatch = b.getMatchText().toLowerCase().startsWith(config.getSearchText().toLowerCase());
                if (aStartsWithMatch && bStartsWithMatch) {
                    return Strings.compareNatural(a.getMatchText(), b.getMatchText());
                } else if (aStartsWithMatch) {
                    return -1;
                } else if (bStartsWithMatch) {
                    return 1;
                } else {
                    return Strings.compareNatural(a.getMatchText(), b.getMatchText());
                }
            }
        });
        if ((config.getLimit() > 0) && (sortedHits.size() > config.getLimit())) {
            matches.addAllSearchHits(sortedHits.subList(0, config.getLimit()));
        } else {
            matches.addAllSearchHits(sortedHits);    
        }
        return matches;
    }

    private List<SearchHit> queryNameMatches(SearchConfig config, boolean startsWith) throws SQLException {
        final AutocompleteNameQueryBuilder queryBuilder = new AutocompleteNameQueryBuilder(config, startsWith);
        final QueryExecutor<List<SearchHit>> queryExecutor = new QueryExecutor<List<SearchHit>>(this.dataSource, queryBuilder) {
            @Override
            public List<SearchHit> processResult(ResultSet result) throws SQLException {
                final List<SearchHit> hits = new ArrayList<SearchHit>();
                while (result.next()) {
                    final SearchHit hit = createSearchHit(result, MatchType.NAME);
                    hits.add(hit);
                }
                return hits;
            }
        };
        return queryExecutor.executeQuery();
    }

    private List<SearchHit> querySynonymMatches(SearchConfig config, boolean startsWith) throws SQLException {
        final AutocompleteSynonymQueryBuilder queryBuilder = new AutocompleteSynonymQueryBuilder(config, startsWith);
        final QueryExecutor<List<SearchHit>> queryExecutor = new QueryExecutor<List<SearchHit>>(this.dataSource, queryBuilder) {
            @Override
            public List<SearchHit> processResult(ResultSet result) throws SQLException {
                final List<SearchHit> hits = new ArrayList<SearchHit>();
                while (result.next()) {
                    final SearchHit hit = createSearchHit(result, MatchType.SYNONYM);
                    hits.add(hit);
                }
                return hits;
            }
        };
        return queryExecutor.executeQuery();
    }

    private SearchHit createSearchHit(ResultSet nodeResult, MatchType type) throws SQLException {
        final DefaultTerm term = new DefaultTerm(nodeResult.getInt("node_id"), nodeResult.getInt("source_id"));
        term.setUID(nodeResult.getString("uid"));
        final String label = nodeResult.getString("label");
        term.setLabel(label);
        term.setSource(new SimpleTerm(nodeResult.getString("source_uid"), nodeResult.getString("source_label")));
        final String matchText;
        if (type == MatchType.SYNONYM) {
            matchText = nodeResult.getString("synonym_label");
        } else {
            matchText = label;
        }
        final SearchHit hit = new SearchHit(term, matchText, type);
        return hit;
    }

    public List<Term> getNamesForIDs(final Collection<String> ids, final POSTCOMP_OPTION option) throws SQLException {
        final List<Term> terms = new ArrayList<Term>();
        for (final String id : ids) {
            final QueryBuilder query = new BulkTermNameQueryBuilder(id);
            terms.add((new QueryExecutor<Term>(this.dataSource, query) {
                @Override
                public Term processResult(ResultSet result) throws SQLException {
                    while (result.next()) {
                        final String taxonUID = result.getString("taxon_uid");
                        if (taxonUID != null) {
                            final TaxonTerm taxon = new TaxonTerm(result.getInt("node_id"), null);
                            taxon.setUID(result.getString("uid"));
                            taxon.setLabel(result.getString("label"));
                            taxon.setExtinct(result.getBoolean("is_extinct"));
                            if (result.getString("rank_uid") != null) {
                                taxon.setRank(new SimpleTerm(result.getString("rank_uid"), result.getString("rank_label")));
                            }
                            return taxon;
                        } else {
                            return createBasicTerm(result.getString("uid"), result.getString("label"), option);
                        }
                    }
                    return null;
                }
            }).executeQuery());
        }
        return terms;
    }

    public LinkedTerm renderPostcomposition(final String uid) throws SQLException {
        final QueryBuilder query = new IntersectionLinksQueryBuilder(uid);
        return (new QueryExecutor<LinkedTerm>(this.dataSource, query) {
            @Override
            public LinkedTerm processResult(ResultSet result) throws SQLException {
                final LinkedTerm term = new DefaultTerm(-1, null);
                term.setUID(uid);
                while (result.next()) {
                    final Relationship rel = createRelationship(result);
                    if (rel.getOther().getLabel() == null) {
                        rel.setOther(renderPostcomposition(rel.getOther().getUID()));
                    }
                    term.addSubjectLink(rel);
                }
                return term;
            }
        }).executeQuery();
    }

    public String semanticLabel(final String uid) throws SQLException {
        final QueryBuilder query = new QueryBuilder() {
            @Override
            protected String getQuery() {
                return "SELECT semantic_label FROM smart_node_label WHERE uid=?";
            }
            @Override
            protected void fillStatement(PreparedStatement statement) throws SQLException {
                statement.setString(1, uid);
            }            
        };
        return (new QueryExecutor<String>(this.dataSource, query) {
            @Override
            public String processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return result.getString("semantic_label");
                }
                return uid;
            }
        }).executeQuery();
    }
    
    public String simpleLabel(final String uid) throws SQLException {
        final QueryBuilder query = new QueryBuilder() {
            @Override
            protected String getQuery() {
                return "SELECT simple_label FROM smart_node_label WHERE uid=?";
            }
            @Override
            protected void fillStatement(PreparedStatement statement) throws SQLException {
                statement.setString(1, uid);
            }            
        };
        return (new QueryExecutor<String>(this.dataSource, query) {
            @Override
            public String processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return result.getString("simple_label");
                }
                return uid;
            }
        }).executeQuery();
    }

    private Term createBasicTerm(String uid, String label, POSTCOMP_OPTION option) throws SQLException {
        if ((label == null) && (!option.equals(POSTCOMP_OPTION.NONE))) {
            if (option.equals(POSTCOMP_OPTION.SEMANTIC_LABEL)) {
                return new SimpleTerm(uid, this.semanticLabel(uid));
            } else if (option.equals(POSTCOMP_OPTION.SIMPLE_LABEL)) {
                return new SimpleTerm(uid, this.simpleLabel(uid));
            } else {
                return this.renderPostcomposition(uid);
            }
        } else {
            return new SimpleTerm(uid, label);
        }
    }

    public Matrix getMatrixForPublication(String pubID) throws SQLException {
        final QueryBuilder query = new MatrixDataQueryBuilder(pubID);
        return (new QueryExecutor<Matrix>(this.dataSource, query) {
            @Override
            public Matrix processResult(ResultSet result) throws SQLException {
                final Matrix matrix = new Matrix();
                while (result.next()) {
                    final OTU otu = new OTU(result.getInt("otu_node_id"));
                    otu.setUID(result.getString("otu_uid"));
                    otu.setLabel(result.getString("otu_label"));
                    otu.setComment(result.getString("otu_comment"));
                    final TaxonTerm taxon = new TaxonTerm(0, null);
                    taxon.setUID(result.getString("taxon_uid"));
                    taxon.setLabel(result.getString("taxon_label"));
                    if (result.getString("rank_uid") != null) {
                        taxon.setRank(new SimpleTerm(result.getString("rank_uid"), result.getString("rank_label")));
                    }
                    taxon.setExtinct(result.getBoolean("is_extinct"));
                    otu.setTaxon(taxon);
                    final Character character = new Character(result.getString("character_uid"), result.getString("character_label"), result.getString("character_number"));
                    final Term state = new SimpleTerm(result.getString("state_uid"), result.getString("state_label"));
                    matrix.setState(otu, character, state);
                }
                return matrix;
            }
        }).executeQuery();
    }

    public List<OTU> getOTUsForPublication(String pubID) throws SQLException {
        final QueryBuilder query = new PublicationOTUsQueryBuilder(pubID);
        return (new QueryExecutor<List<OTU>>(this.dataSource, query) {
            @Override
            public List<OTU> processResult(ResultSet result) throws SQLException {
                final List<OTU> otus = new ArrayList<OTU>();
                while (result.next()) {
                    final OTU otu = new OTU(result.getInt("node_id"));
                    otu.setUID(result.getString("uid"));
                    otu.setLabel(result.getString("label"));
                    otu.setComment(result.getString("comment"));
                    final TaxonTerm taxon = new TaxonTerm(result.getInt("taxon_node_id"), null);
                    taxon.setUID(result.getString("taxon_uid"));
                    taxon.setLabel(result.getString("taxon_label"));
                    if (result.getString("rank_uid") != null) {
                        taxon.setRank(new SimpleTerm(result.getString("rank_uid"), result.getString("rank_label")));
                    }
                    taxon.setExtinct(result.getBoolean("is_extinct"));
                    otu.setTaxon(taxon);
                    otu.addAllSpecimens(getSpecimensForOTU(otu.getUID()));
                    otus.add(otu);
                }
                return otus;
            }
        }).executeQuery();
    }

    public List<Specimen> getSpecimensForOTU(String otuID) throws SQLException {
        final QueryBuilder query = new OTUSpecimensQueryBuilder(otuID);
        return (new QueryExecutor<List<Specimen>>(this.dataSource, query) {
            @Override
            public List<Specimen> processResult(ResultSet result) throws SQLException {
                final List<Specimen> specimens = new ArrayList<Specimen>();
                while (result.next()) {
                    final Term collection = new SimpleTerm(result.getString("collection_uid"), result.getString("collection_label"));
                    final Specimen specimen = new Specimen(collection, result.getString("catalog_id"));
                    specimens.add(specimen);
                }
                return specimens;
            }
        }).executeQuery();
    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
