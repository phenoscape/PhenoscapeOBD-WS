package org.phenoscape.ws.resource.statistics;

import java.util.HashMap;
import java.util.Map;

import org.obd.model.Node;

public abstract class AbstractStatisticsGenerator extends AbstractOBDResource {

    protected static final String SILURIFORMES_ID = "TTO:1380";
    protected static final String CYPRINIFORMES_ID = "TTO:1360";
    protected static final String CHARACIFORMES_ID = "TTO:1370";
    protected static final String GYMNOTIFORMES_ID = "TTO:1390";
    protected static final String GONORYNCHIFORMES_ID = "TTO:1350";
    protected static final String CLUPEIFORMES_ID = "TTO:1340";
    protected static final String EUTELEOSTEI_ID = "TTO:254";

    protected static final String[] ORDERS = 
    { SILURIFORMES_ID, CYPRINIFORMES_ID, CHARACIFORMES_ID, GYMNOTIFORMES_ID, 
        GONORYNCHIFORMES_ID, CLUPEIFORMES_ID, EUTELEOSTEI_ID };

    protected static final String CARDIOVASCULAR_SYSTEM_ID = "TAO:0000010";
    protected static final String DIGESTIVE_SYSTEM_ID = "TAO:0000339";
    protected static final String ENDOCRINE_SYSTEM_ID = "TAO:0001158";
    protected static final String HEMATOPOIETIC_SYSTEM_ID = "TAO:0005023";
    protected static final String IMMUNE_SYSTEM_ID = "TAO:0001159";
    protected static final String LIVER_AND_BILIARY_SYSTEM_ID = "TAO:0000036";
    protected static final String MUSCULATURE_SYSTEM_ID = "TAO:0000548";
    protected static final String NERVOUS_SYSTEM_ID = "TAO:0000396";
    protected static final String RENAL_SYSTEM_ID = "TAO:0000163";
    protected static final String REPRODUCTIVE_SYSTEM_ID = "TAO:0000632";
    protected static final String RESPIRATORY_SYSTEM_ID = "TAO:0000272";
    protected static final String SENSORY_SYSTEM_ID = "TAO:0000282";
    protected static final String SKELETAL_SYSTEM_ID = "TAO:0000434";
    protected static final String[] SYSTEMS = 
    {CARDIOVASCULAR_SYSTEM_ID, DIGESTIVE_SYSTEM_ID, ENDOCRINE_SYSTEM_ID, HEMATOPOIETIC_SYSTEM_ID, IMMUNE_SYSTEM_ID, 
        LIVER_AND_BILIARY_SYSTEM_ID, MUSCULATURE_SYSTEM_ID, NERVOUS_SYSTEM_ID, RENAL_SYSTEM_ID, REPRODUCTIVE_SYSTEM_ID, 
        RESPIRATORY_SYSTEM_ID, SENSORY_SYSTEM_ID, SKELETAL_SYSTEM_ID};

    protected static final String SHAPE_ID = "PATO:0000052";
    protected static final String SIZE_ID = "PATO:0000117";
    protected static final String TEXTURE_ID = "PATO:0000150";
    protected static final String POSITION_ID = "PATO:0000140";
    protected static final String RELATIONAL_SPATIAL_QUALITY_ID = "PATO:0001631";
    protected static final String RELATIONAL_STRUCTURAL_QUALITY_ID = "PATO:0001452";
    protected static final String RELATIONAL_SHAPE_QUALITY_ID = "PATO:0001647";
    protected static final String COLOR_ID = "PATO:0000014";
    protected static final String COUNT_ID = "PATO:0000070";
    protected static final String COMPOSITION_ID = "PATO:0000025";
    protected static final String STRUCTURE_ID = "PATO:0000141";
    protected static final String[] ATTRIBUTES = 
    {SHAPE_ID, SIZE_ID, TEXTURE_ID, POSITION_ID, RELATIONAL_SPATIAL_QUALITY_ID, RELATIONAL_STRUCTURAL_QUALITY_ID,
        RELATIONAL_SHAPE_QUALITY_ID, COLOR_ID, COUNT_ID, COMPOSITION_ID, STRUCTURE_ID};

    protected static Map<String, String> uidToCladeNameMap;

    static {
        uidToCladeNameMap = new HashMap<String, String>();
        uidToCladeNameMap.put(CLUPEIFORMES_ID, "Clupeiformes");
        uidToCladeNameMap.put(GONORYNCHIFORMES_ID, "Gonorhynchiformes");
        uidToCladeNameMap.put(CYPRINIFORMES_ID, "Cypriniformes");
        uidToCladeNameMap.put(CHARACIFORMES_ID, "Characiformes");
        uidToCladeNameMap.put(SILURIFORMES_ID, "Siluriformes");
        uidToCladeNameMap.put(GYMNOTIFORMES_ID, "Gymnotiformes");
        uidToCladeNameMap.put(EUTELEOSTEI_ID, "Euteleostei");
    }

    protected String label(String id) {
        final Node node = this.getShard().getNode(id);
        return node.getLabel();
    }
    
}
