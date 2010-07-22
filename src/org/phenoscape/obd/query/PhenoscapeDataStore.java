package org.phenoscape.obd.query;

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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.phenoscape.obd.model.DefaultTerm;
import org.phenoscape.obd.model.GeneAnnotation;
import org.phenoscape.obd.model.GeneTerm;
import org.phenoscape.obd.model.LinkedTerm;
import org.phenoscape.obd.model.Relationship;
import org.phenoscape.obd.model.SimpleTerm;
import org.phenoscape.obd.model.Synonym;
import org.phenoscape.obd.model.TaxonTerm;
import org.phenoscape.obd.model.Term;
import org.phenoscape.obd.model.Vocab.CDAO;
import org.phenoscape.obd.model.Vocab.OBO;
import org.phenoscape.obd.query.SearchHit.MatchType;

import com.eekboom.utils.Strings;

public class PhenoscapeDataStore {

    private final DataSource dataSource;

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
        this.addSynonymsToTerm(term);
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
    public TaxonTerm getTaxonTerm(String uid) throws SQLException {
        final QueryBuilder query = new TaxonQueryBuilder(uid);
        final TaxonTerm taxonTerm = (new QueryExecutor<TaxonTerm>(this.dataSource, query) {
            @Override
            public TaxonTerm processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    final TaxonTerm taxon = createTaxonTermWithProperties(result);
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
                    addChildrenToTaxon(taxon);
                    addSynonymsToTerm(taxon);
                    return taxon;
                }
                //no taxon with this ID
                return null;
            }}).executeQuery();
        return taxonTerm;
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
            final Term rank = new DefaultTerm(result.getInt("node_id"), null);
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
        return synonym;
    }
    

    public int getCountOfTaxonomicAnnotations(boolean includeInferredAnnotations) throws SQLException {
        //TODO revise
        final QueryBuilder query = new SimpleQuery("SELECT count(*) FROM taxon_annotation");
        final QueryExecutor<Integer> queryExecutor = new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        };
        return queryExecutor.executeQuery();
    }
    public List<TaxonTerm> getAnnotatedTaxa(TaxonAnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new AnnotatedTaxaQueryBuilder(config, true);
        return (new QueryExecutor<List<TaxonTerm>>(this.dataSource, query) {
            @Override
            public List<TaxonTerm> processResult(ResultSet result) throws SQLException {
                final List<TaxonTerm> taxa = new ArrayList<TaxonTerm>();
                while (result.next()) {
                    taxa.add(createTaxonTermWithProperties(result));
                }
                return taxa;
            }
        }).executeQuery();
    }

    public int getCountOfAnnotatedTaxa(TaxonAnnotationsQueryConfig config) throws SQLException {
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

    public List<GeneAnnotation> getGeneAnnotations(GeneAnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new GeneAnnotationsQueryBuilder(config, false);
        return (new QueryExecutor<List<GeneAnnotation>>(this.dataSource, query) {
            @Override
            public List<GeneAnnotation> processResult(ResultSet result) throws SQLException {
                final List<GeneAnnotation> annotations = new ArrayList<GeneAnnotation>();
                while (result.next()) {
                    annotations.add(createGeneAnnotation(result));
                }
                return annotations;
            }
        }).executeQuery();
    }
    
    private GeneAnnotation createGeneAnnotation(ResultSet result) throws SQLException {
        //TODO this needs to check for empty strings before creating terms
        final GeneAnnotation annotation = new GeneAnnotation();
        final GeneTerm gene = new GeneTerm(result.getInt("gene_node_id"), null);
        gene.setUID(result.getString("gene_uid"));
        gene.setLabel(result.getString("gene_label"));
        annotation.setGene(gene);
        annotation.setEntity(new SimpleTerm(result.getString("entity_uid"), result.getString("entity_label")));
        annotation.setQuality(new SimpleTerm(result.getString("quality_uid"), result.getString("quality_label")));
        annotation.setRelatedEntity(new SimpleTerm(result.getString("related_entity_uid"), result.getString("related_entity_label")));
        return annotation;
    }

    public int getCountOfGeneAnnotations(GeneAnnotationsQueryConfig config) throws SQLException {
        final QueryBuilder query = new GeneAnnotationsQueryBuilder(config, true);
        final QueryExecutor<Integer> queryExecutor = new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        };
        return queryExecutor.executeQuery();
    }

    public int getCountOfAnnotatedGenes() throws SQLException {
        final QueryBuilder query = new SimpleQuery("SELECT count(DISTINCT gene_node_id) FROM gene_annotation");
        final QueryExecutor<Integer> queryExecutor = new QueryExecutor<Integer>(this.dataSource, query) {
            @Override
            public Integer processResult(ResultSet result) throws SQLException {
                while (result.next()) {
                    return Integer.valueOf(result.getInt(1));
                }
                return Integer.valueOf(0);
            }
        };
        return queryExecutor.executeQuery();
    }
    
    public List<Term> getAnnotatedPublications(TaxonAnnotationsQueryConfig config) throws SQLException {
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
    
    public int getCountOfAnnotatedPublications(TaxonAnnotationsQueryConfig config) throws SQLException {
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
    
    public List<Term> getAnnotatedCharacters(TaxonAnnotationsQueryConfig config) throws SQLException {
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
    
    public int getCountOfAnnotatedCharacters(TaxonAnnotationsQueryConfig config) throws SQLException {
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
    
    public int getCountOfAnnotatedCharacterStates(TaxonAnnotationsQueryConfig config) throws SQLException {
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
        final DefaultTerm term = new DefaultTerm(result.getInt("publication_node_id"), null);
        term.setLabel(result.getString("publication_label"));
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
        final String matchText;
        if (type == MatchType.SYNONYM) {
            matchText = nodeResult.getString("synonym_label");
        } else {
            matchText = label;
        }
        final SearchHit hit = new SearchHit(term, matchText, type);
        return hit;
    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
