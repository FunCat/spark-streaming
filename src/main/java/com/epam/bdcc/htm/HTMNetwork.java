package com.epam.bdcc.htm;

import org.numenta.nupic.Parameters;
import org.numenta.nupic.algorithms.*;
import org.numenta.nupic.encoders.MultiEncoder;
import org.numenta.nupic.encoders.MultiEncoderAssembler;
import org.numenta.nupic.network.Inference;
import org.numenta.nupic.network.Network;
import org.numenta.nupic.util.Tuple;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class HTMNetwork implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String STR_DT = "DT";
    private static final String STR_MEASUREMENT = "Measurement";
    private static final String STR_DT_FORMAT = "YY-MM-dd HH:mm";

    private String id = null;
    private Network network = null;
    private ResultState resultState = new ResultState();

    public HTMNetwork() {
    }

    public HTMNetwork(String id) {
        this.id = id;

        final Parameters parameters = getNetworkParams();
        final MultiEncoder encoder = MultiEncoder.builder().name("MultiEncoder").build();
        MultiEncoderAssembler.assemble(encoder, getFieldEncodingMap());

        this.network = Network.create(id, parameters)
                .add(Network.createRegion("Region 1")
                        .add(Network.createLayer("Layer 2/3", parameters)
                                .alterParameter(Parameters.KEY.AUTO_CLASSIFY, Boolean.TRUE)
                                .add(Anomaly.create())
                                .add(new TemporalMemory())
                                .add(new SpatialPooler())
                                // .add(Sensor.create(FileSensor::create, SensorParams.create(SensorParams.Keys::path, "", "input2.csv")))
                                .add(encoder)
                        ));
    }

    private Map<String, Map<String, Object>> setupMap(Map<String, Map<String, Object>> map, Integer n, Integer w, Double min, Double max, Double radius, Double resolution, Boolean periodic, Boolean clip, Boolean forced, String fieldName, String fieldType, String encoderType) {
        if (map == null) {
            map = new HashMap<>();
        }
        Map<String, Object> inner;
        if ((inner = map.get(fieldName)) == null) {
            map.put(fieldName, inner = new HashMap<>());
        }

        inner.put("n", n);
        inner.put("w", w);
        inner.put("minVal", min);
        inner.put("maxVal", max);
        inner.put("radius", radius);
        inner.put("resolution", resolution);

        if (periodic != null) inner.put("periodic", periodic);
        if (clip != null) inner.put("clipInput", clip);
        if (forced != null) inner.put("forced", forced);
        if (fieldName != null) inner.put("fieldName", fieldName);
        if (fieldType != null) inner.put("fieldType", fieldType);
        if (encoderType != null) inner.put("encoderType", encoderType);

        return map;
    }

    private Map<String, Map<String, Object>> getFieldEncodingMap() {
        Map<String, Map<String, Object>> fieldEncodings = setupMap(null, 0, 0, 0.0d, 0.0d, 0.0d, 0.0d, null, null, null, STR_DT, "datetime", "DateEncoder");

        // remove the time of day
        // fieldEncodings.get("DT").put(Parameters.KEY.DATEFIELD_TOFD.getFieldName(), new Tuple(21, 9.5)); // Time of day

        // check if we can safely remove the seazonal factor or play with it
        fieldEncodings.get(STR_DT).put(Parameters.KEY.DATEFIELD_SEASON.getFieldName(), new Tuple(31, 91.5)); // season
        fieldEncodings.get(STR_DT).put(Parameters.KEY.DATEFIELD_PATTERN.getFieldName(), STR_DT_FORMAT);

        // !!! check these parameters
        fieldEncodings = setupMap(fieldEncodings, 0, 51, 0.0d, 0.1d, 0.0d, 0.001d, null, Boolean.TRUE, null, STR_MEASUREMENT, "float", "ScalarEncoder");

        return fieldEncodings;
    }

    private Map<String, Class<? extends Classifier>> getInferredFieldsMap(String field, Class<? extends Classifier> classifier) {
        Map<String, Class<? extends Classifier>> inferredFieldsMap = new HashMap<>();
        inferredFieldsMap.put(field, classifier);
        return inferredFieldsMap;
    }

    private Parameters getNetworkParams() {
        Parameters p = Parameters.getAllDefaultParameters();

        // universal parameters
        p.set(Parameters.KEY.SEED, 42);

        // ENCODER PARAMETERS
        p.set(Parameters.KEY.FIELD_ENCODING_MAP, getFieldEncodingMap());  /// !!!!! check after this if the N, W etc. is set properly
        p.set(Parameters.KEY.INFERRED_FIELDS, getInferredFieldsMap(STR_MEASUREMENT, SDRClassifier.class));

        // SPATIAL POOLER
        p.set(Parameters.KEY.INPUT_DIMENSIONS, new int[]{500}); // it will be fixed automatically
        p.set(Parameters.KEY.GLOBAL_INHIBITION, true);
        p.set(Parameters.KEY.NUM_ACTIVE_COLUMNS_PER_INH_AREA, 40.0);
        p.set(Parameters.KEY.POTENTIAL_PCT, 0.8);
        p.set(Parameters.KEY.SYN_PERM_CONNECTED, 0.1);
        p.set(Parameters.KEY.SYN_PERM_ACTIVE_INC, 0.0001);
        p.set(Parameters.KEY.SYN_PERM_INACTIVE_DEC, 0.0005);
        p.set(Parameters.KEY.MAX_BOOST, 1.0);

        // parameters set by default
        /* !!! check if this should be set at all
        p.set(Parameters.KEY.POTENTIAL_RADIUS, 12);
        p.set(Parameters.KEY.LOCAL_AREA_DENSITY, -1.0);
        p.set(Parameters.KEY.STIMULUS_THRESHOLD, 1.0);
        p.set(Parameters.KEY.SYN_PERM_TRIM_THRESHOLD, 0.05);
        p.set(Parameters.KEY.MIN_PCT_OVERLAP_DUTY_CYCLES, 0.1);
        p.set(Parameters.KEY.MIN_PCT_ACTIVE_DUTY_CYCLES, 0.1);
        p.set(Parameters.KEY.DUTY_CYCLE_PERIOD, 10);
        */

        // TEMPORAL MEMORY
        p.set(Parameters.KEY.COLUMN_DIMENSIONS, new int[]{2048});
        p.set(Parameters.KEY.CELLS_PER_COLUMN, 4);

        p.set(Parameters.KEY.MAX_NEW_SYNAPSE_COUNT, 20);
        p.set(Parameters.KEY.INITIAL_PERMANENCE, 0.21);
        p.set(Parameters.KEY.PERMANENCE_INCREMENT, 0.1);
        p.set(Parameters.KEY.PERMANENCE_DECREMENT, 0.1);
        p.set(Parameters.KEY.MIN_THRESHOLD, 9);
        p.set(Parameters.KEY.ACTIVATION_THRESHOLD, 12);

        // parameters set by default
        /* !!! check if they should be set
        parameters.set(Parameters.KEY.CONNECTED_PERMANENCE, 0.8);
        */
        return p;
    }

    public ResultState compute(Map<String, Object> inputMap) {
        final Inference inference = network.computeImmediate(inputMap);
        int recordNumber = inference.getRecordNum();

        // we are in "compute" method; if we called it, it means that we're moving to the new data point;
        // in the new data point we take the prediction which was BEFORE THAT
        double prediction = resultState.getPredictionNext();
        double predictionNext = (double) inference.getClassification(STR_MEASUREMENT).getMostProbableValue(1);
        double actual = (double) inference.getClassifierInput().get(STR_MEASUREMENT).get("inputValue");
        double error = 0.0d, anomaly = 0.0d;
        if (resultState.getRecordNumber() > 0) {
            error = Math.abs(resultState.getPrediction() - resultState.getActual());
            anomaly = inference.getAnomalyScore();
        }
        resultState.setState(recordNumber, inputMap, actual, prediction, error, anomaly, predictionNext);
        return resultState;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Network getNetwork() {
        return network;
    }

    public void setNetwork(Network network) {
        this.network = network;
    }

    public ResultState getResultState() {
        return resultState;
    }

    public void setResultState(ResultState resultState) {
        this.resultState = resultState;
    }
}
